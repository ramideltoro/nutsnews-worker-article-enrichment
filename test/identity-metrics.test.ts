import {
  describe,
  expect,
  it
} from "vitest";

import { ENRICHMENT_CONFIG_SCHEMA } from "../src/config.js";
import { createEnrichmentPrometheusTelemetrySink } from "../src/metrics.js";

const BUILD_REVISION = "0123456789abcdef0123456789abcdef01234567";

describe("enrichment immutable telemetry identity", () => {
  it("exports exactly one Runtime-owned expected-active, build, and deployment series with truthful shadow identity", () => {
    const output = createEnrichmentPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-enrichment",
        version: "0.1.0",
        environment: "production",
        host: "backend-vps",
        revision: BUILD_REVISION,
        deployment: "shadow",
        adapter: "mixed"
      }
    }).collect();
    const identitySamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_build_info{")
      || line.startsWith("nutsnews_worker_deployment_info{"));
    const expectedActiveSamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_expected_active{"));

    expect(identitySamples).toHaveLength(2);
    expect(expectedActiveSamples).toEqual([
      'nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-enrichment"} 0'
    ]);
    expect(identitySamples.join("\n")).toContain(`revision="${BUILD_REVISION}"`);
    expect(identitySamples.join("\n")).toContain('deployment="shadow"');
    expect(identitySamples.join("\n")).toContain('adapter="mixed"');
    expect(identitySamples.join("\n")).not.toContain("unknown");
  });

  it("honors the expected-active option without adding a compatibility duplicate", () => {
    const output = createEnrichmentPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-enrichment",
        version: "0.1.0",
        environment: "production",
        host: "backend-vps"
      },
      expectedActive: true
    }).collect();
    const expectedActiveSamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_expected_active{"));

    expect(expectedActiveSamples).toEqual([
      'nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-enrichment"} 1'
    ]);
  });

  it("advances Runtime last-success on accepted and duplicate events without moving backward", async () => {
    const metrics = createEnrichmentPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-enrichment",
        version: "0.1.0",
        environment: "test",
        host: "enrichment-test"
      }
    });

    await metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-08-01T00:05:00.000Z",
      stage: "enrichment",
      queue: "nutsnews.worker.enrichment.v1",
      outcome: "success"
    });
    await metrics.emit({
      name: "runtime.message.duplicate",
      level: "info",
      at: "2026-08-01T00:04:00.000Z",
      stage: "enrichment",
      queue: "nutsnews.worker.enrichment.v1",
      outcome: "duplicate"
    });
    await metrics.emit({
      name: "runtime.message.duplicate",
      level: "info",
      at: "2026-08-01T00:06:00.000Z",
      stage: "enrichment",
      queue: "nutsnews.worker.enrichment.v1",
      outcome: "duplicate"
    });
    await metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-08-01T00:07:00.000Z",
      stage: "approval",
      queue: "nutsnews.worker.approval.v1",
      outcome: "success"
    });

    expect(metrics.collect()).toContain(
      `nutsnews_worker_last_success_timestamp_seconds{environment="test",service="nutsnews-worker-article-enrichment"} ${String(Date.parse("2026-08-01T00:06:00.000Z") / 1_000)}`
    );
  });

  it("retains allowlisted Runtime dependency names as bounded useful labels", async () => {
    const metrics = createEnrichmentPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-enrichment",
        version: "0.1.0",
        environment: "test",
        host: "enrichment-test"
      }
    });

    await metrics.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-08-01T00:00:00.000Z",
      stage: "enrichment",
      queue: "nutsnews.worker.enrichment.v1",
      durationMs: 5,
      outcome: "success",
      attributes: {
        dependency: "article-enrichment"
      }
    });

    const output = metrics.collect();

    expect(output).toContain('nutsnews_worker_dependency_duration_seconds_bucket{');
    expect(output).toContain('dependency="article-enrichment"');
    expect(output).not.toContain('dependency="other"');
  });

  it("does not double Runtime consumer lifecycle counters after broker-owned transitions", async () => {
    const metrics = createEnrichmentPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-enrichment",
        version: "0.1.0",
        environment: "test",
        host: "enrichment-test"
      }
    });

    await metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "info",
      at: "2026-08-01T00:00:00.000Z",
      stage: "enrichment",
      queue: "nutsnews.worker.enrichment.v1",
      outcome: "active",
      attributes: {
        activeConsumers: 1
      }
    });
    metrics.setConsumerActive(1);

    let output = metrics.collect();

    expect(metricSample(output, "nutsnews_worker_consumer_events_total", {
      outcome: "active"
    })).toBe(1);
    expect(metricSample(output, "nutsnews_worker_consumers", {
      outcome: "active"
    })).toBe(1);

    await metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "info",
      at: "2026-08-01T00:01:00.000Z",
      stage: "enrichment",
      queue: "nutsnews.worker.enrichment.v1",
      outcome: "closed",
      attributes: {
        activeConsumers: 0
      }
    });
    metrics.setConsumerActive(0);
    output = metrics.collect();

    expect(metricSample(output, "nutsnews_worker_consumer_events_total", {
      outcome: "closed"
    })).toBe(1);
    expect(metricSample(output, "nutsnews_worker_consumers", {
      outcome: "active"
    })).toBe(0);
  });

  it("coheres consumer-loss health without fabricating another check duration", async () => {
    const metrics = createEnrichmentPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-enrichment",
        version: "0.1.0",
        environment: "test",
        host: "enrichment-test"
      }
    });

    await metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "info",
      at: "2026-08-01T00:00:00.000Z",
      stage: "enrichment",
      queue: "nutsnews.worker.enrichment.v1",
      outcome: "active",
      attributes: {
        activeConsumers: 1
      }
    });
    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-08-01T00:00:01.000Z",
      outcome: "ok",
      attributes: {
        probe: "readiness",
        status: "ok",
        checks: [
          {
            name: "rabbitmq-consumer",
            status: "ok",
            critical: true,
            durationMs: 7
          }
        ]
      }
    });
    await metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "warn",
      at: "2026-08-01T00:00:02.000Z",
      stage: "enrichment",
      queue: "nutsnews.worker.enrichment.v1",
      outcome: "channel-dropped",
      attributes: {
        activeConsumers: 0
      }
    });

    const output = metrics.collect();

    expect(metricSample(output, "nutsnews_worker_health_probe", {
      probe: "readiness",
      outcome: "unhealthy"
    })).toBe(1);
    expect(metricSample(output, "nutsnews_worker_health_check", {
      probe: "readiness",
      check: "rabbitmq-consumer",
      outcome: "ok"
    })).toBe(0);
    expect(metricSample(output, "nutsnews_worker_health_check", {
      probe: "readiness",
      check: "rabbitmq-consumer",
      outcome: "unhealthy"
    })).toBe(1);
    expect(metricSample(output, "nutsnews_worker_health_check_duration_seconds_count", {
      probe: "readiness",
      check: "rabbitmq-consumer"
    })).toBe(1);
  });

  it("declares the immutable revision as required and non-sensitive in production", () => {
    expect(ENRICHMENT_CONFIG_SCHEMA.find((variable) => variable.name === "NUTSNEWS_ENRICHMENT_BUILD_REVISION")).toMatchObject({
      requiredInProduction: true,
      sensitive: false
    });
  });
});

function metricSample(
  output: string,
  metric: string,
  requiredLabels: Readonly<Record<string, string>>
): number {
  const matches = output.split("\n").filter((line) => line.startsWith(`${metric}{`)
    && Object.entries(requiredLabels).every(([name, value]) => line.includes(`${name}="${value}"`)));

  expect(matches).toHaveLength(1);
  return Number(matches[0]?.split(" ").at(-1));
}
