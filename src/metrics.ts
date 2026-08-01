import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeServiceIdentity,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export const ENRICHMENT_STAGE_LATENCY_BUCKETS_SECONDS = [
  0.005,
  0.01,
  0.025,
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
const ENRICHMENT_RUNTIME_DEPENDENCIES = [
  "article-enrichment",
  "article-enrichment-work-handler",
  "local-enrichment-work-handler",
  "enrichment-shell"
] as const;
const ENRICHMENT_RUNTIME_HEALTH_CHECKS = [
  "process",
  "service-started",
  "configuration-mode",
  "broker-lifecycle",
  "rabbitmq-consumer",
  "enrichment-state",
  "database-transactions",
  "broker-outbox",
  "http-client",
  "dns-policy",
  "html-parser",
  "production-adapters",
  "shadow-mode"
] as const;
const MAX_LABEL_LENGTH = 96;

export function createEnrichmentPrometheusTelemetrySink(
  options: EnrichmentPrometheusTelemetrySinkOptions
): EnrichmentPrometheusTelemetrySink {
  const runtime = createPrometheusRuntimeTelemetrySink({
    ...options,
    defaultQueue: options.defaultQueue ?? ENRICHMENT_MAIN_QUEUE,
    cardinality: {
      ...options.cardinality,
      dependencies: options.cardinality?.dependencies ?? ENRICHMENT_RUNTIME_DEPENDENCIES,
      healthChecks: options.cardinality?.healthChecks ?? ENRICHMENT_RUNTIME_HEALTH_CHECKS
    },
    expectedActive: options.expectedActive ?? false
  });
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
  let lastSuccessTimestampSeconds = -1;
  let latencyCount = 0;
  let latencySum = 0;

  emitRuntimeConsumerState(runtime, consumerActive);

  return {
    allowedLabels: runtime.allowedLabels,
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      recordHealthEvent(health, event);
      const observedConsumerActive = consumerActiveFromEvent(event);

      if (observedConsumerActive !== undefined) {
        consumerActive = observedConsumerActive;

        if (consumerActive === 0) {
          health.set("readiness", "unhealthy");
        }
      }
      lastSuccessTimestampSeconds = updateRuntimeLastSuccess(
        runtime,
        event,
        lastSuccessTimestampSeconds
      );
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
      const runtimeOutput = cohereRuntimeConsumerHealthCheck(
        withoutRuntimeHealthProbeFamily(collectRuntimeMetrics(runtime)),
        consumerActive
      );
      const outputs = [
        runtimeOutput,
        collectCompatibilityIdentityMetrics(options, runtimeOutput),
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
      const normalized = Number.isFinite(activeConsumers)
        ? Math.max(0, Math.floor(activeConsumers))
        : 0;

      if (normalized === 0) {
        health.set("readiness", "unhealthy");
      }

      if (normalized === consumerActive) {
        return;
      }

      consumerActive = normalized;
      emitRuntimeConsumerState(runtime, consumerActive);
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

function withoutRuntimeHealthProbeFamily(output: string): string {
  return output
    .split("\n")
    .filter((line) => !line.startsWith("# HELP nutsnews_worker_health_probe ")
      && line !== "# TYPE nutsnews_worker_health_probe gauge"
      && !line.startsWith("nutsnews_worker_health_probe{"))
    .join("\n")
    .trimEnd();
}

function cohereRuntimeConsumerHealthCheck(output: string, activeConsumers: number): string {
  const expectedOutcome: EnrichmentHealthOutcome = activeConsumers > 0 ? "ok" : "unhealthy";
  const lines = output.split("\n");
  const observedOutcomes = new Set(lines
    .filter(isRuntimeConsumerHealthCheckLine)
    .flatMap((line) => HEALTH_OUTCOMES.filter((candidate) => line.includes(`outcome="${candidate}"`))));

  if (!HEALTH_OUTCOMES.every((outcome) => observedOutcomes.has(outcome))) {
    return output.trimEnd();
  }

  return lines
    .map((line) => {
      if (!isRuntimeConsumerHealthCheckLine(line)) {
        return line;
      }

      const outcome = HEALTH_OUTCOMES.find((candidate) => line.includes(`outcome="${candidate}"`));
      const sampleSeparator = line.lastIndexOf(" ");

      if (outcome === undefined || sampleSeparator < 0) {
        return line;
      }

      return `${line.slice(0, sampleSeparator)} ${outcome === expectedOutcome ? "1" : "0"}`;
    })
    .join("\n")
    .trimEnd();
}

function isRuntimeConsumerHealthCheckLine(line: string): boolean {
  return line.startsWith("nutsnews_worker_health_check{")
    && line.includes('probe="readiness"')
    && line.includes('check="rabbitmq-consumer"');
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
  if (event.name !== "runtime.dependency.observed") {
    return true;
  }

  const attributeDuration = event.attributes?.durationMs;

  return (event.durationMs !== undefined && Number.isFinite(event.durationMs))
    || (typeof attributeDuration === "number" && Number.isFinite(attributeDuration));
}

function updateRuntimeLastSuccess(
  runtime: PrometheusRuntimeTelemetrySink,
  event: RuntimeTelemetryEvent,
  currentTimestampSeconds: number
): number {
  if ((event.name !== "runtime.message.accepted" && event.name !== "runtime.message.duplicate")
    || event.stage !== "enrichment"
    || (event.queue !== undefined && event.queue !== ENRICHMENT_MAIN_QUEUE)) {
    return currentTimestampSeconds;
  }

  const timestampSeconds = Math.floor(Date.parse(event.at) / 1_000);

  if (!Number.isFinite(timestampSeconds) || timestampSeconds < 0 || timestampSeconds <= currentTimestampSeconds) {
    return currentTimestampSeconds;
  }

  try {
    runtime.setLastSuccessTimestamp(timestampSeconds);
    return timestampSeconds;
  } catch {
    return currentTimestampSeconds;
  }
}

function emitRuntimeConsumerState(
  runtime: PrometheusRuntimeTelemetrySink,
  activeConsumers: number
): void {
  runBestEffort(() => runtime.emit({
    name: "runtime.broker.consumer_state_changed",
    level: activeConsumers > 0 ? "info" : "warn",
    at: new Date().toISOString(),
    stage: "enrichment",
    queue: ENRICHMENT_MAIN_QUEUE,
    outcome: activeConsumers > 0 ? "active" : "inactive",
    attributes: {
      activeConsumers
    }
  }));
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
