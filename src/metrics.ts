import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeServiceIdentity,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export const ENRICHMENT_STAGE_LATENCY_BUCKETS_SECONDS = [
  0.01,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  30,
  60,
  120,
  300
] as const;

export type EnrichmentStageOutcome = "success" | "duplicate" | "invalid" | "retry" | "dlq" | "failure";
export type EnrichmentHealthProbe = "liveness" | "startup" | "readiness";
export type EnrichmentHealthOutcome = "ok" | "degraded" | "unhealthy";

export interface EnrichmentMetricIdentity extends RuntimeServiceIdentity {
  readonly revision?: string;
  readonly deployment?: "local" | "test" | "shadow" | "production" | "unknown";
  readonly adapter?: "in_memory" | "mixed" | "production" | "unknown";
}

export interface EnrichmentPrometheusTelemetrySinkOptions extends Omit<PrometheusRuntimeTelemetrySinkOptions, "identity"> {
  readonly identity: EnrichmentMetricIdentity;
}

export interface EnrichmentMetricsSink extends RuntimeTelemetrySink {
  readonly allowedLabels: PrometheusRuntimeTelemetrySink["allowedLabels"];
  collect(): string;
  setInFlight(queue: string, value: number): void;
  setShutdownDraining(draining: boolean): void;
}

export interface EnrichmentPrometheusTelemetrySink extends EnrichmentMetricsSink {
  setConsumerActive(activeConsumers: number): void;
  setHealthProbe(probe: EnrichmentHealthProbe, outcome: EnrichmentHealthOutcome): void;
}

const ENRICHMENT_STAGE_SERVICE = "enrichment";
const ENRICHMENT_MAIN_QUEUE = "nutsnews.worker.enrichment.v1";
const ENRICHMENT_STAGE_OUTCOMES = [
  "success",
  "duplicate",
  "invalid",
  "retry",
  "dlq",
  "failure"
] as const satisfies readonly EnrichmentStageOutcome[];
const HEALTH_PROBES = [
  "liveness",
  "startup",
  "readiness"
] as const satisfies readonly EnrichmentHealthProbe[];
const HEALTH_OUTCOMES = [
  "ok",
  "degraded",
  "unhealthy"
] as const satisfies readonly EnrichmentHealthOutcome[];
const MAX_LABEL_LENGTH = 96;

export function createEnrichmentPrometheusTelemetrySink(
  options: EnrichmentPrometheusTelemetrySinkOptions
): EnrichmentPrometheusTelemetrySink {
  const runtime = createPrometheusRuntimeTelemetrySink(options);
  const environment = metricLabelValue(options.identity.environment);
  const counters = new Map<EnrichmentStageOutcome, number>(
    ENRICHMENT_STAGE_OUTCOMES.map((outcome) => [
      outcome,
      0
    ])
  );
  const bucketCounts = ENRICHMENT_STAGE_LATENCY_BUCKETS_SECONDS.map(() => 0);
  const health = new Map<EnrichmentHealthProbe, EnrichmentHealthOutcome>([
    [
      "liveness",
      "ok"
    ],
    [
      "startup",
      "unhealthy"
    ],
    [
      "readiness",
      "unhealthy"
    ]
  ]);
  let consumerActive = 0;
  let latencyCount = 0;
  let latencySum = 0;

  return {
    allowedLabels: runtime.allowedLabels,
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      recordHealthEvent(health, event);
      consumerActive = consumerActiveFromEvent(event) ?? consumerActive;
      const outcome = enrichmentStageOutcome(event);

      if (outcome !== undefined) {
        counters.set(outcome, (counters.get(outcome) ?? 0) + 1);
        const durationSeconds = durationSecondsFrom(event.durationMs);

        if (durationSeconds !== undefined) {
          latencyCount += 1;
          latencySum += durationSeconds;

          for (const [index, boundary] of ENRICHMENT_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
            if (durationSeconds <= boundary) {
              bucketCounts[index] = (bucketCounts[index] ?? 0) + 1;
            }
          }
        }
      }

      if (shouldForwardToRuntime(event)) {
        try {
          await runtime.emit(event);
        } catch {
          // The compatibility sink is best effort and cannot alter worker semantics.
        }
      }
    },
    collect(): string {
      const runtimeOutput = collectRuntimeMetrics(runtime);
      const outputs = [
        runtimeOutput,
        collectCompatibilityIdentityMetrics(options, runtimeOutput),
        collectExpectedActiveMetric(environment),
        collectConsumerActiveMetric(environment, consumerActive),
        collectHealthProbeMetrics(environment, health),
        collectEnrichmentStageMetrics(environment, counters, bucketCounts, latencyCount, latencySum)
      ].filter((output) => output.length > 0);

      return `${outputs.join("\n")}\n`;
    },
    setInFlight(queue, value): void {
      runBestEffort(() => runtime.setInFlight(queue, value));
    },
    setShutdownDraining(draining): void {
      runBestEffort(() => runtime.setShutdownDraining(draining));
    },
    setConsumerActive(activeConsumers): void {
      consumerActive = Math.max(0, Math.floor(activeConsumers));
    },
    setHealthProbe(probe, outcome): void {
      health.set(probe, outcome);
    }
  };
}

function collectCompatibilityIdentityMetrics(
  options: EnrichmentPrometheusTelemetrySinkOptions,
  runtimeOutput: string
): string {
  const identity = options.identity;
  const environment = metricLabelValue(identity.environment);
  const service = metricLabelValue(identity.service);
  const lines: string[] = [];

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_build_info")) {
    lines.push(
      "# HELP nutsnews_worker_build_info Immutable worker build identity.",
      "# TYPE nutsnews_worker_build_info gauge",
      `nutsnews_worker_build_info${labels({
        environment,
        service,
        version: metricLabelValue(identity.version),
        revision: metricLabelValue(identity.revision ?? "unknown")
      })} 1`
    );
  }

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_deployment_info")) {
    lines.push(
      "# HELP nutsnews_worker_deployment_info Worker deployment ownership and dependency adapter identity.",
      "# TYPE nutsnews_worker_deployment_info gauge",
      `nutsnews_worker_deployment_info${labels({
        environment,
        service,
        deployment: metricLabelValue(identity.deployment ?? "unknown"),
        adapter: metricLabelValue(identity.adapter ?? "unknown")
      })} 1`
    );
  }

  return lines.join("\n");
}

function hasMetricFamily(output: string, metric: string): boolean {
  return output.split("\n").some((line) => line.startsWith(`# HELP ${metric} `)
    || line.startsWith(`${metric}{`)
    || line.startsWith(`${metric} `));
}

function collectRuntimeMetrics(runtime: PrometheusRuntimeTelemetrySink): string {
  try {
    return runtime.collect().trimEnd();
  } catch {
    return "";
  }
}

function runBestEffort(operation: () => unknown): void {
  try {
    const result = operation();

    if (isPromiseLike(result)) {
      void result.then(undefined, () => undefined);
    }
  } catch {
    // The compatibility sink is best effort and cannot alter worker semantics.
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return typeof value === "object" && value !== null && "then" in value && typeof value.then === "function";
}

function shouldForwardToRuntime(event: RuntimeTelemetryEvent): boolean {
  if (event.name === "runtime.health.evaluated") {
    return false;
  }

  if (event.name !== "runtime.dependency.observed") {
    return true;
  }

  const attributeDuration = event.attributes?.durationMs;

  return (event.durationMs !== undefined && Number.isFinite(event.durationMs))
    || (typeof attributeDuration === "number" && Number.isFinite(attributeDuration));
}

function recordHealthEvent(
  health: Map<EnrichmentHealthProbe, EnrichmentHealthOutcome>,
  event: RuntimeTelemetryEvent
): void {
  if (event.name !== "runtime.health.evaluated") {
    return;
  }

  const probe = event.attributes?.probe;
  const outcome = event.outcome;

  if (isHealthProbe(probe) && isHealthOutcome(outcome)) {
    health.set(probe, outcome);
  }
}

function collectExpectedActiveMetric(environment: string): string {
  return [
    "# HELP nutsnews_worker_expected_active Whether this worker deployment is expected to own active production work.",
    "# TYPE nutsnews_worker_expected_active gauge",
    `nutsnews_worker_expected_active${labels({
      environment,
      service: ENRICHMENT_STAGE_SERVICE
    })} 0`
  ].join("\n");
}

function collectConsumerActiveMetric(environment: string, activeConsumers: number): string {
  return [
    "# HELP nutsnews_worker_consumer_active Active enrichment main-queue consumers reported by the service.",
    "# TYPE nutsnews_worker_consumer_active gauge",
    `nutsnews_worker_consumer_active${labels({
      environment,
      service: ENRICHMENT_STAGE_SERVICE,
      queue: ENRICHMENT_MAIN_QUEUE
    })} ${formatMetricNumber(activeConsumers)}`
  ].join("\n");
}

function consumerActiveFromEvent(event: RuntimeTelemetryEvent): number | undefined {
  if (event.name !== "runtime.broker.consumer_state_changed" || event.stage !== "enrichment" || event.queue !== ENRICHMENT_MAIN_QUEUE) {
    return undefined;
  }

  const activeConsumers = event.attributes?.activeConsumers;

  if (typeof activeConsumers === "number" && Number.isFinite(activeConsumers)) {
    return Math.max(0, Math.floor(activeConsumers));
  }

  return event.outcome === "active" ? 1 : 0;
}

function collectHealthProbeMetrics(
  environment: string,
  health: ReadonlyMap<EnrichmentHealthProbe, EnrichmentHealthOutcome>
): string {
  if (health.size === 0) {
    return "";
  }

  const lines = [
    "# HELP nutsnews_worker_health_probe Worker health status by distinct bounded probe and outcome.",
    "# TYPE nutsnews_worker_health_probe gauge"
  ];

  for (const probe of HEALTH_PROBES) {
    const current = health.get(probe);

    if (current === undefined) {
      continue;
    }

    for (const outcome of HEALTH_OUTCOMES) {
      lines.push(`nutsnews_worker_health_probe${labels({
        environment,
        service: ENRICHMENT_STAGE_SERVICE,
        probe,
        outcome
      })} ${outcome === current ? "1" : "0"}`);
    }
  }

  return lines.join("\n");
}

function enrichmentStageOutcome(event: RuntimeTelemetryEvent): EnrichmentStageOutcome | undefined {
  if (event.stage !== "enrichment" || event.queue !== ENRICHMENT_MAIN_QUEUE) {
    return undefined;
  }

  switch (event.name) {
    case "runtime.message.accepted":
      return "success";
    case "runtime.message.duplicate":
      return "duplicate";
    case "runtime.message.invalid":
      return "invalid";
    case "runtime.message.retry":
      return "retry";
    case "runtime.message.dlq":
      return "dlq";
    case "runtime.message.started":
    case "runtime.broker.state_changed":
    case "runtime.broker.topology_asserted":
    case "runtime.broker.consumer_state_changed":
    case "runtime.dependency.observed":
    case "runtime.health.evaluated":
    case "runtime.shutdown.started":
    case "runtime.shutdown.completed":
    case "runtime.shutdown.failed":
      return undefined;
  }
}

function collectEnrichmentStageMetrics(
  environment: string,
  counters: ReadonlyMap<EnrichmentStageOutcome, number>,
  bucketCounts: readonly number[],
  latencyCount: number,
  latencySum: number
): string {
  const eventLabels = (outcome: EnrichmentStageOutcome): string => labels({
    environment,
    service: ENRICHMENT_STAGE_SERVICE,
    outcome
  });
  const histogramLabels = labels({
    environment,
    service: ENRICHMENT_STAGE_SERVICE
  });
  const lines = [
    "# HELP nutsnews_worker_uplift_stage_events_total Completed worker-uplift stage deliveries by bounded service and outcome.",
    "# TYPE nutsnews_worker_uplift_stage_events_total counter"
  ];

  for (const outcome of ENRICHMENT_STAGE_OUTCOMES) {
    lines.push(`nutsnews_worker_uplift_stage_events_total${eventLabels(outcome)} ${formatMetricNumber(counters.get(outcome) ?? 0)}`);
  }

  lines.push(
    "# HELP nutsnews_worker_uplift_stage_latency_seconds Worker-uplift stage completion latency in seconds.",
    "# TYPE nutsnews_worker_uplift_stage_latency_seconds histogram"
  );

  for (const [index, boundary] of ENRICHMENT_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
    lines.push(`nutsnews_worker_uplift_stage_latency_seconds_bucket${labels({
      environment,
      service: ENRICHMENT_STAGE_SERVICE,
      le: String(boundary)
    })} ${formatMetricNumber(bucketCounts[index] ?? 0)}`);
  }

  lines.push(
    `nutsnews_worker_uplift_stage_latency_seconds_bucket${labels({
      environment,
      service: ENRICHMENT_STAGE_SERVICE,
      le: "+Inf"
    })} ${formatMetricNumber(latencyCount)}`,
    `nutsnews_worker_uplift_stage_latency_seconds_sum${histogramLabels} ${formatMetricNumber(latencySum)}`,
    `nutsnews_worker_uplift_stage_latency_seconds_count${histogramLabels} ${formatMetricNumber(latencyCount)}`
  );

  return lines.join("\n");
}

function labels(values: Readonly<Record<string, string>>): string {
  return `{${Object.entries(values).map(([name, value]) => `${name}="${escapeLabelValue(value)}"`).join(",")}}`;
}

function metricLabelValue(value: string): string {
  const cleaned = value
    .trim()
    .replace(/[^A-Za-z0-9_.:-]+/gu, "_")
    .slice(0, MAX_LABEL_LENGTH);

  return cleaned.length > 0 ? cleaned : "unknown";
}

function escapeLabelValue(value: string): string {
  return value
    .replace(/\\/gu, "\\\\")
    .replace(/"/gu, "\\\"");
}

function formatMetricNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(6)));
}

function durationSecondsFrom(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) ? Math.max(0, value) / 1_000 : undefined;
}

function isHealthProbe(value: unknown): value is EnrichmentHealthProbe {
  return typeof value === "string" && HEALTH_PROBES.some((probe) => probe === value);
}

function isHealthOutcome(value: unknown): value is EnrichmentHealthOutcome {
  return typeof value === "string" && HEALTH_OUTCOMES.some((outcome) => outcome === value);
}
