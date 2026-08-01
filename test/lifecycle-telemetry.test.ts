import {
  WORKER_DELIVERY_BEHAVIOR,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  RUNTIME_ALLOWED_METRIC_LABELS,
  RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS,
  createBufferedRuntimeTelemetrySink,
  type RuntimeMessageDelivery,
  type RuntimeTelemetryEvent
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadEnrichmentConfig } from "../src/config.js";
import {
  ENRICHMENT_STAGE_LATENCY_BUCKETS_SECONDS,
  createEnrichmentPrometheusTelemetrySink,
  type EnrichmentPrometheusTelemetrySink
} from "../src/metrics.js";
import { createEnrichmentService } from "../src/service.js";
import {
  InMemoryEnrichmentStateStore,
  LocalBrokerTransport,
  LocalEnrichmentWorkHandler,
  ManualEnrichmentClock,
  createLocalEnrichmentDependencies,
  createMinimalEnrichmentDelivery,
  createMinimalEnrichmentEnvelope,
  createMinimalEnrichmentPayload
} from "../src/test-doubles.js";

const COMPLETING_MESSAGE_EVENTS = new Set([
  "runtime.message.accepted",
  "runtime.message.duplicate",
  "runtime.message.invalid",
  "runtime.message.retry",
  "runtime.message.dlq"
]);

describe("enrichment lifecycle telemetry", () => {
  it("emits exactly one completing event for accepted, duplicate, invalid, retry, retry-exhausted, and terminal deliveries", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    context.telemetry.clear();
    await exerciseLifecycleOutcomes(context);

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate",
      "runtime.message.started",
      "runtime.message.invalid",
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);

    const started = messageEvents.filter((event) => event.name === "runtime.message.started");
    const completed = messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name));
    expect(started).toHaveLength(6);
    expect(completed).toHaveLength(started.length);
    expect(context.workHandler.handled).toHaveLength(4);
    expect(completed[0]).toMatchObject({
      name: "runtime.message.accepted",
      outcome: "success",
      messageId: messageId(1),
      idempotencyKey: idempotencyKey(1)
    });
    expect(completed[1]).toMatchObject({
      name: "runtime.message.duplicate",
      outcome: "duplicate",
      messageId: messageId(1),
      idempotencyKey: idempotencyKey(1)
    });
    expect(completed[2]).toMatchObject({
      name: "runtime.message.invalid",
      outcome: "invalid",
      attributes: {
        issuePath: "$.schemaVersion"
      }
    });
    expect(completed[3]).toMatchObject({
      name: "runtime.message.retry",
      outcome: "retry",
      attributes: {
        reason: "transient-enrichment-error",
        destination: "nutsnews.worker.enrichment.v1.retry-30s"
      }
    });
    expect(completed[4]).toMatchObject({
      name: "runtime.message.dlq",
      outcome: "dlq",
      attributes: {
        reason: "retry-exhausted",
        destination: "nutsnews.worker.enrichment.v1.dlq"
      }
    });
    expect(completed[5]).toMatchObject({
      name: "runtime.message.dlq",
      outcome: "dlq",
      attributes: {
        reason: "terminal-enrichment-error",
        destination: "nutsnews.worker.enrichment.v1.dlq"
      }
    });

    await context.service.stop();
  });

  it("exports bounded canonical stage, fixed-bucket latency, ownership, probe, and consumer metrics", async () => {
    const context = createTelemetryContext();

    const initialOutput = context.metrics.collect();
    const initialCanonicalSeries = canonicalStageSeriesKeys(initialOutput);
    expect(initialCanonicalSeries).toHaveLength(22);

    for (const outcome of [
      "success",
      "duplicate",
      "invalid",
      "retry",
      "dlq",
      "failure"
    ]) {
      expect(metricValue(initialOutput, "nutsnews_worker_uplift_stage_events_total", outcome)).toBe(0);
    }

    for (const boundary of ENRICHMENT_STAGE_LATENCY_BUCKETS_SECONDS) {
      expect(sampleValue(initialOutput, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
        le: String(boundary)
      })).toBe(0);
    }
    expect(sampleValue(initialOutput, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "+Inf"
    })).toBe(0);
    expect(sampleValue(initialOutput, "nutsnews_worker_uplift_stage_latency_seconds_sum")).toBe(0);
    expect(sampleValue(initialOutput, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(0);
    expectHealthOneHot(initialOutput, "liveness", "ok");
    expectHealthOneHot(initialOutput, "startup", "unhealthy");
    expectHealthOneHot(initialOutput, "readiness", "unhealthy");
    expect(sampleValue(initialOutput, "nutsnews_worker_consumer_active", {
      queue: "nutsnews.worker.enrichment.v1"
    })).toBe(0);

    await context.service.start();
    const startedOutput = context.metrics.collect();
    expectHealthOneHot(startedOutput, "liveness", "ok");
    expectHealthOneHot(startedOutput, "startup", "ok");
    expectHealthOneHot(startedOutput, "readiness", "unhealthy");
    expect(startedOutput).not.toContain("nutsnews_worker_dependency_duration_ms");

    await exerciseLifecycleOutcomes(context);
    await context.service.health.liveness();
    await context.service.health.startup();
    await context.service.health.readiness();

    const output = context.metrics.collect();
    expect(canonicalStageSeriesKeys(output)).toEqual(initialCanonicalSeries);
    expect(ENRICHMENT_STAGE_LATENCY_BUCKETS_SECONDS).toEqual([
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
    ]);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "success")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "duplicate")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "invalid")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "retry")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "dlq")).toBe(2);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "failure")).toBe(0);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "30"
    })).toBe(6);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "+Inf"
    })).toBe(6);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_sum")).toBe(1);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(6);
    expect(output).toContain('nutsnews_worker_expected_active{environment="test",service="enrichment"} 0');
    expect(output).toContain('nutsnews_worker_health_probe{environment="test",service="enrichment",probe="liveness",outcome="ok"} 1');
    expect(output).toContain('nutsnews_worker_health_probe{environment="test",service="enrichment",probe="startup",outcome="ok"} 1');
    expect(output).toContain('nutsnews_worker_health_probe{environment="test",service="enrichment",probe="readiness",outcome="ok"} 1');
    expect(sampleValue(output, "nutsnews_worker_consumer_active", {
      queue: "nutsnews.worker.enrichment.v1"
    })).toBe(1);
    expect(output).not.toContain("nutsnews_worker_dependency_duration_ms");
    expect(context.metrics.allowedLabels).toEqual(RUNTIME_ALLOWED_METRIC_LABELS);

    for (const line of metricSampleLines(output)) {
      expect(metricLabelNames(line)).toEqual(expectedMetricLabelNames(line));
    }

    for (const forbidden of RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS) {
      expect(output).not.toContain(`${forbidden}=`);
    }

    for (const identifier of [
      messageId(1),
      idempotencyKey(1),
      "article-001",
      "candidate-world-001",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b4601",
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    ]) {
      expect(output).not.toContain(identifier);
    }

    await context.service.consumer?.cancel();
    const readiness = await context.service.health.readiness();
    expect(readiness.status).toBe("unhealthy");
    const unhealthyOutput = context.metrics.collect();
    expect(unhealthyOutput).toContain('nutsnews_worker_health_probe{environment="test",service="enrichment",probe="readiness",outcome="unhealthy"} 1');
    expect(sampleValue(unhealthyOutput, "nutsnews_worker_consumer_active", {
      queue: "nutsnews.worker.enrichment.v1"
    })).toBe(0);

    await context.service.stop();
    const stoppedOutput = context.metrics.collect();
    expectHealthOneHot(stoppedOutput, "liveness", "ok");
    expectHealthOneHot(stoppedOutput, "startup", "unhealthy");
    expectHealthOneHot(stoppedOutput, "readiness", "unhealthy");
  });

  it("keeps telemetry and metric sink rejection best-effort without changing exact-one delivery semantics", async () => {
    const config = loadEnrichmentConfig({
      HOSTNAME: "enrichment-rejecting-telemetry-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const clock = new ManualEnrichmentClock();
    const dependencies = createLocalEnrichmentDependencies({
      clock
    });
    const events: RuntimeTelemetryEvent[] = [];
    const rejectingSink: EnrichmentPrometheusTelemetrySink = {
      allowedLabels: RUNTIME_ALLOWED_METRIC_LABELS,
      emit: (event) => {
        events.push(event);
        return Promise.reject(new Error("telemetry unavailable"));
      },
      collect: () => {
        throw new Error("metrics unavailable");
      },
      setInFlight: () => {
        throw new Error("metrics unavailable");
      },
      setShutdownDraining: () => {
        throw new Error("metrics unavailable");
      },
      setConsumerActive: () => {
        throw new Error("metrics unavailable");
      },
      setHealthProbe: () => {
        throw new Error("metrics unavailable");
      }
    };
    const service = createEnrichmentService({
      config,
      dependencies,
      telemetry: rejectingSink,
      metrics: rejectingSink
    });
    const workHandler = dependencies.workHandler as LocalEnrichmentWorkHandler;
    workHandler.onHandleStart = () => {
      clock.advance(250);
    };
    const context = {
      broker: dependencies.brokerTransport as LocalBrokerTransport,
      service,
      workHandler
    };

    await expect(service.start()).resolves.toBeUndefined();
    events.length = 0;
    await exerciseLifecycleOutcomes(context);

    const messageEvents = events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate",
      "runtime.message.started",
      "runtime.message.invalid",
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(6);
    expect(workHandler.handled).toHaveLength(4);
    await expect(service.stop()).resolves.toBeUndefined();
  });

  it("disposes idempotency-store exceptions as exactly one retry or DLQ outcome", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    context.telemetry.clear();
    vi.spyOn(context.stateStore, "claim")
      .mockRejectedValueOnce(new Error("state unavailable"))
      .mockRejectedValueOnce(new Error("state unavailable"));

    await expect(context.broker.deliverEnrichment(enrichmentDelivery(7))).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-claim-error"
    });
    await expect(context.broker.deliverEnrichment(enrichmentDelivery(8, {
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T00:00:00.000Z",
        lastAttemptAt: "2026-07-23T00:05:00.000Z"
      }
    }))).resolves.toMatchObject({
      action: "dlq",
      reason: "idempotency-claim-error"
    });

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(2);
    expect(context.workHandler.handled).toHaveLength(0);
    expect(metricValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_events_total", "retry")).toBe(1);
    expect(metricValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_events_total", "dlq")).toBe(1);
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(2);

    await context.service.stop();
  });

  it("disposes a completion-store exception as one explicitly classified retry", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    context.telemetry.clear();
    vi.spyOn(context.stateStore, "markCompleted").mockRejectedValueOnce(new Error("state unavailable"));

    await expect(context.broker.deliverEnrichment(enrichmentDelivery(9))).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-completion-error"
    });

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.retry"
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(1);
    expect(messageEvents[1]).toMatchObject({
      attributes: {
        reason: "idempotency-completion-error"
      }
    });
    expect(context.workHandler.handled).toHaveLength(1);
    expect(metricValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_events_total", "retry")).toBe(1);
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(1);

    await context.service.stop();
  });

  it("disposes an exhausted failure-record exception as one explicitly classified DLQ outcome", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    context.telemetry.clear();
    context.workHandler.result = {
      status: "retry",
      reason: "transient-enrichment-error"
    };
    const markFailed = vi.spyOn(context.stateStore, "markFailed").mockRejectedValue(new Error("state unavailable"));

    await expect(context.broker.deliverEnrichment(enrichmentDelivery(10, {
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T00:00:00.000Z",
        lastAttemptAt: "2026-07-23T00:05:00.000Z"
      }
    }))).resolves.toMatchObject({
      action: "dlq",
      reason: "idempotency-failure-record-error"
    });

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.dlq"
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(1);
    expect(messageEvents[1]).toMatchObject({
      attributes: {
        reason: "idempotency-failure-record-error"
      }
    });
    expect(markFailed).toHaveBeenCalledTimes(1);
    expect(context.workHandler.handled).toHaveLength(1);
    expect(metricValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_events_total", "dlq")).toBe(1);
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(1);

    await context.service.stop();
  });
});

function createTelemetryContext() {
  const config = loadEnrichmentConfig({
    HOSTNAME: "enrichment-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
    NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
  });
  const clock = new ManualEnrichmentClock();
  const dependencies = createLocalEnrichmentDependencies({
    clock
  });
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createEnrichmentPrometheusTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host,
        revision: config.buildRevision,
        deployment: "test",
        adapter: "in_memory"
      }
  });
  const service = createEnrichmentService({
    config,
    dependencies,
    telemetry: {
      emit: async (event) => {
        await telemetry.emit(event);
        await metrics.emit(event);
      }
    },
    metrics
  });
  const workHandler = dependencies.workHandler as LocalEnrichmentWorkHandler;
  workHandler.onHandleStart = () => {
    clock.advance(250);
  };

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    metrics,
    service,
    stateStore: dependencies.stateStore as InMemoryEnrichmentStateStore,
    telemetry,
    workHandler
  };
}

interface LifecycleContext {
  readonly broker: LocalBrokerTransport;
  readonly service: ReturnType<typeof createEnrichmentService>;
  readonly workHandler: LocalEnrichmentWorkHandler;
}

async function exerciseLifecycleOutcomes(context: LifecycleContext): Promise<void> {
  const accepted = enrichmentDelivery(1);
  await expect(context.broker.deliverEnrichment(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "handled"
  });
  await expect(context.broker.deliverEnrichment(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "duplicate"
  });

  await expect(context.broker.deliverEnrichment(enrichmentDelivery(2, {}, {
    schemaVersion: 99
  }))).resolves.toMatchObject({
    action: "dlq",
    reason: "invalid-payload"
  });

  context.workHandler.result = {
    status: "retry",
    reason: "transient-enrichment-error",
    retryAfterMs: 2_000
  };
  await expect(context.broker.deliverEnrichment(enrichmentDelivery(3))).resolves.toMatchObject({
    action: "retry",
    reason: "transient-enrichment-error"
  });

  context.workHandler.result = {
    status: "retry",
    reason: "retry-exhausted"
  };
  await expect(context.broker.deliverEnrichment(enrichmentDelivery(4, {
    attempt: {
      count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T00:00:00.000Z",
      lastAttemptAt: "2026-07-23T00:05:00.000Z"
    }
  }))).resolves.toMatchObject({
    action: "dlq",
    reason: "retry-exhausted"
  });

  context.workHandler.result = {
    status: "terminal-failure",
    reason: "terminal-enrichment-error"
  };
  await expect(context.broker.deliverEnrichment(enrichmentDelivery(5))).resolves.toMatchObject({
    action: "dlq",
    reason: "terminal-enrichment-error"
  });
}

function expectHealthOneHot(
  output: string,
  probe: "liveness" | "startup" | "readiness",
  expected: "ok" | "degraded" | "unhealthy"
): void {
  const outcomes = [
    "ok",
    "degraded",
    "unhealthy"
  ] as const;
  const values = outcomes.map((outcome) => sampleValue(output, "nutsnews_worker_health_probe", {
    probe,
    outcome
  }));

  expect(values.reduce((sum, value) => sum + value, 0)).toBe(1);
  expect(values[outcomes.indexOf(expected)]).toBe(1);
}

function enrichmentDelivery(
  sequence: number,
  envelopeOverrides: Partial<WorkerMessageEnvelope> = {},
  payloadOverrides: Readonly<Record<string, unknown>> = {}
): RuntimeMessageDelivery {
  return {
    ...createMinimalEnrichmentDelivery(),
    envelope: createMinimalEnrichmentEnvelope({
      messageId: messageId(sequence),
      idempotencyKey: idempotencyKey(sequence),
      ...envelopeOverrides
    }),
    payload: createMinimalEnrichmentPayload({
      idempotencyKey: idempotencyKey(sequence),
      ...payloadOverrides
    })
  };
}

function messageId(sequence: number): string {
  return `018f1598-2dd5-7c4f-9f92-8f7a7f8b48${String(sequence).padStart(2, "0")}`;
}

function idempotencyKey(sequence: number): string {
  return `canonicalizer:enrichment:telemetry-${String(sequence)}:fingerprint001`;
}

function metricValue(output: string, metric: string, outcome: string): number {
  return sampleValue(output, metric, {
    outcome
  });
}

function sampleValue(
  output: string,
  metric: string,
  requiredLabels: Readonly<Record<string, string>> = {}
): number {
  const matches = output
    .split("\n")
    .filter((line) => line.startsWith(`${metric}{`) && Object.entries(requiredLabels).every(([name, value]) => line.includes(`${name}="${value}"`)));

  expect(matches).toHaveLength(1);
  const value = matches[0]?.split(" ").at(-1);

  return Number(value);
}

function metricSampleLines(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_") && line.includes("{"));
}

function canonicalStageSeriesKeys(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_events_total{")
      || line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_"))
    .map((line) => line.slice(0, line.lastIndexOf(" ")))
    .sort();
}

function metricLabelNames(line: string): readonly string[] {
  const start = line.indexOf("{");
  const end = line.indexOf("}", start);

  return line
    .slice(start + 1, end)
    .split(",")
    .map((label) => label.slice(0, label.indexOf("=")));
}

function expectedMetricLabelNames(line: string): readonly string[] {
  if (line.startsWith("nutsnews_worker_build_info{")) {
    return [
      "environment",
      "service",
      "version",
      "revision"
    ];
  }

  if (line.startsWith("nutsnews_worker_deployment_info{")) {
    return [
      "environment",
      "service",
      "deployment",
      "adapter"
    ];
  }

  if (line.startsWith("nutsnews_worker_expected_active{")) {
    return [
      "environment",
      "service"
    ];
  }

  if (line.startsWith("nutsnews_worker_health_probe{")) {
    return [
      "environment",
      "service",
      "probe",
      "outcome"
    ];
  }

  if (line.startsWith("nutsnews_worker_consumer_active{")) {
    return [
      "environment",
      "service",
      "queue"
    ];
  }

  if (line.startsWith("nutsnews_worker_uplift_stage_events_total{")) {
    return [
      "environment",
      "service",
      "outcome"
    ];
  }

  if (line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_bucket{")) {
    return [
      "environment",
      "service",
      "le"
    ];
  }

  if (line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_")) {
    return [
      "environment",
      "service"
    ];
  }

  const actual = new Set(metricLabelNames(line));
  const boundedRuntimeLabels = RUNTIME_ALLOWED_METRIC_LABELS.filter((label) => actual.has(label));

  return line.includes("_bucket{")
    ? [
        ...boundedRuntimeLabels,
        "le"
      ]
    : boundedRuntimeLabels;
}
