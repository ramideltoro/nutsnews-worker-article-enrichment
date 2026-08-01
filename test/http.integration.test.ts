import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import { loadEnrichmentConfig } from "../src/config.js";
import {
  createEnrichmentHttpServer,
  type EnrichmentHttpServer
} from "../src/http.js";
import { createEnrichmentPrometheusTelemetrySink } from "../src/metrics.js";
import {
  createEnrichmentFailClosedReconciler
} from "../src/reconciliation.js";
import { createEnrichmentService } from "../src/service.js";
import {
  ManualEnrichmentClock,
  createLocalEnrichmentDependencies
} from "../src/test-doubles.js";

let activeServer: EnrichmentHttpServer | undefined;

afterEach(async () => {
  if (activeServer !== undefined) {
    await activeServer.close();
    activeServer = undefined;
  }
});

describe("enrichment HTTP endpoints", () => {
  it("serves liveness, readiness, startup, metrics, and value-free config schema", async () => {
    const config = loadEnrichmentConfig({
      NUTSNEWS_ENRICHMENT_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const metrics = createEnrichmentPrometheusTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    const service = createEnrichmentService({
      config,
      dependencies: createLocalEnrichmentDependencies(),
      telemetry: metrics,
      metrics
    });
    activeServer = createEnrichmentHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await activeServer.listen();

    await expectJsonStatus(activeServer.url("/live"), 200, "ok");
    await expectJsonStatus(activeServer.url("/startup"), 200, "ok");
    await expectJsonStatus(activeServer.url("/ready"), 200, "ok");

    const metricsResponse = await fetch(activeServer.url("/metrics"));
    expect(metricsResponse.status).toBe(200);
    const metricsBody = await metricsResponse.text();
    expect(metricsBody).not.toContain("nutsnews_worker_dependency_duration_ms");
    expect(metricsBody).toContain('nutsnews_worker_expected_active{environment="local",service="nutsnews-worker-article-enrichment"} 0');
    expect(metricsBody).toContain('queue="nutsnews.worker.enrichment.v1",outcome="active"} 1');
    expect(metricsBody).toContain('nutsnews_worker_health_probe{environment="local",service="enrichment",probe="liveness",outcome="ok"} 1');
    expect(metricsBody).toContain('nutsnews_worker_health_probe{environment="local",service="enrichment",probe="startup",outcome="ok"} 1');
    expect(metricsBody).toContain('nutsnews_worker_health_probe{environment="local",service="enrichment",probe="readiness",outcome="ok"} 1');

    const schemaResponse = await fetch(activeServer.url("/config-schema"));
    expect(schemaResponse.status).toBe(200);
    const schema = await schemaResponse.json() as { readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[] };

    expect(schema.variables.some((variable) => variable.name === "NUTSNEWS_ENRICHMENT_RABBITMQ_URL" && variable.sensitive)).toBe(true);
    expect(JSON.stringify(schema)).not.toContain("amqp://");
    expect(JSON.stringify(schema)).not.toContain("postgres://");

    await service.stop();
  });

  it("protects the reconciliation endpoint with bearer auth", async () => {
    const config = loadEnrichmentConfig({
      NUTSNEWS_ENRICHMENT_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const service = createEnrichmentService({
      config,
      dependencies: createLocalEnrichmentDependencies()
    });
    activeServer = createEnrichmentHttpServer({
      config,
      service,
      reconciler: createEnrichmentFailClosedReconciler(new ManualEnrichmentClock()),
      reconciliationToken: "test-token"
    });

    await service.start();
    await activeServer.listen();

    const unauthorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(unauthorized.status).toBe(401);

    const authorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      headers: {
        authorization: "Bearer test-token"
      },
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(authorized.status).toBe(200);
    await expect(authorized.json()).resolves.toMatchObject({
      status: "dry_run",
      writesPerformed: false,
      productionVisibilityEnabled: false
    });

    await service.stop();
  });
});

async function expectJsonStatus(url: string, statusCode: number, status: string): Promise<void> {
  const response = await fetch(url);
  const body = await response.json() as { readonly status: string };

  expect(response.status).toBe(statusCode);
  expect(body.status).toBe(status);
}
