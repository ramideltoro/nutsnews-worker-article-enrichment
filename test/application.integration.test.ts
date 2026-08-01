import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadEnrichmentConfig } from "../src/config.js";
import {
  EnrichmentStartupTimeoutError,
  createEnrichmentApplication
} from "../src/index.js";
import {
  LocalBrokerTransport,
  createLocalEnrichmentDependencies
} from "../src/test-doubles.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("enrichment application startup", () => {
  it("serves fail-closed diagnostics before broker startup completes", async () => {
    const config = loadEnrichmentConfig({
      HOSTNAME: "enrichment-startup-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_ENRICHMENT_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_STARTUP_TIMEOUT_MS: "5000",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const broker = new GatedLocalBrokerTransport();
    const application = createEnrichmentApplication(config, {
      dependencies: createLocalEnrichmentDependencies({
        brokerTransport: broker
      })
    });
    const starting = application.start();

    await broker.connectStarted.promise;

    await expectHealth(application.diagnosticsUrl("/live"), 200, "ok");
    await expectHealth(application.diagnosticsUrl("/startup"), 503, "unhealthy");
    await expectHealth(application.diagnosticsUrl("/ready"), 503, "unhealthy");
    const metricsBeforeStartup = await fetch(application.diagnosticsUrl("/metrics"));

    expect(metricsBeforeStartup.status).toBe(200);
    expect(await metricsBeforeStartup.text()).toContain('queue="nutsnews.worker.enrichment.v1",outcome="active"} 0');

    broker.releaseConnect();
    await expect(starting).resolves.toBeUndefined();
    await expectHealth(application.diagnosticsUrl("/startup"), 200, "ok");
    await expectHealth(application.diagnosticsUrl("/ready"), 200, "ok");

    await application.stop();
  });

  it("bounds a stalled broker startup and rejects with a named timeout", async () => {
    const config = loadEnrichmentConfig({
      HOSTNAME: "enrichment-startup-timeout-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_ENRICHMENT_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_STARTUP_TIMEOUT_MS: "100",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const broker = new GatedLocalBrokerTransport();
    const application = createEnrichmentApplication(config, {
      dependencies: createLocalEnrichmentDependencies({
        brokerTransport: broker
      })
    });
    const sigintListenersBefore = process.listenerCount("SIGINT");
    const sigtermListenersBefore = process.listenerCount("SIGTERM");
    const starting = application.start();

    await broker.connectStarted.promise;
    await expectHealth(application.diagnosticsUrl("/startup"), 503, "unhealthy");
    await expect(starting).rejects.toMatchObject({
      name: "EnrichmentStartupTimeoutError",
      timeoutMs: 100
    });
    await expect(starting).rejects.toBeInstanceOf(EnrichmentStartupTimeoutError);
    expect(process.listenerCount("SIGINT")).toBe(sigintListenersBefore);
    expect(process.listenerCount("SIGTERM")).toBe(sigtermListenersBefore);
    expect(() => application.diagnosticsUrl("/live")).toThrow(
      "Enrichment HTTP server is not listening on a TCP address."
    );

    expect(broker.closeCalls).toBe(1);
    broker.releaseConnect();
    await broker.secondClose.promise;
    expect(broker.closeCalls).toBe(2);
    await expect(broker.deliverEnrichment()).rejects.toThrow("No local consumer is registered for enrichment.");
  });

  it("closes diagnostics and returns the startup timeout even when transport cleanup never settles", async () => {
    const config = loadEnrichmentConfig({
      HOSTNAME: "enrichment-startup-cleanup-timeout-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_ENRICHMENT_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_STARTUP_TIMEOUT_MS: "100",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const broker = new NeverClosingGatedBrokerTransport();
    const application = createEnrichmentApplication(config, {
      dependencies: createLocalEnrichmentDependencies({
        brokerTransport: broker
      })
    });
    const startedAt = Date.now();
    const starting = application.start();

    await broker.connectStarted.promise;
    await broker.closeStarted.promise;
    await expect(starting).rejects.toMatchObject({
      name: "EnrichmentStartupTimeoutError",
      timeoutMs: 100
    });
    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(() => application.diagnosticsUrl("/live")).toThrow(
      "Enrichment HTTP server is not listening on a TCP address."
    );

    broker.releaseConnect();
  });

  it("keeps the default production application diagnostic-only when durable adapters are unreachable", async () => {
    vi.stubEnv("NUTSNEWS_ENRICHMENT_RABBITMQ_URL", "amqp://broker-secret@example.invalid");
    vi.stubEnv("NUTSNEWS_ENRICHMENT_DATABASE_URL", "postgres://database-secret@example.invalid/enrichment");
    const config = loadEnrichmentConfig({
      HOSTNAME: "enrichment-production-application-test",
      NUTSNEWS_ENVIRONMENT: "production",
      NUTSNEWS_ENRICHMENT_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_ENRICHMENT_DATABASE_URL: "postgres://database-secret@example.invalid/enrichment",
      NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE: "production",
      NUTSNEWS_ENRICHMENT_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_RABBITMQ_URL: "amqp://broker-secret@example.invalid",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const application = createEnrichmentApplication(config);

    await application.start();

    await expectHealth(application.diagnosticsUrl("/live"), 200, "ok");
    await expectHealth(application.diagnosticsUrl("/startup"), 200, "ok");
    const readinessResponse = await fetch(application.diagnosticsUrl("/ready"));
    const readiness = await readinessResponse.json() as {
      readonly status: string;
      readonly checks: readonly {
        readonly name: string;
        readonly status: string;
        readonly details?: Readonly<Record<string, unknown>>;
      }[];
    };

    expect(readinessResponse.status).toBe(503);
    expect(readiness.status).toBe("unhealthy");
    expect(readiness.checks.find((check) => check.name === "production-adapters")).toMatchObject({
      status: "ok",
      details: {
        adapterMode: "production",
        stateStoreAdapter: "production",
        transactionRunnerAdapter: "production",
        brokerOutboxAdapter: "production"
      }
    });
    expect(readiness.checks.find((check) => check.name === "enrichment-state")).toMatchObject({
      status: "unhealthy"
    });
    expect(JSON.stringify(readiness)).not.toContain("database-secret");
    expect(JSON.stringify(readiness)).not.toContain("broker-secret");

    const metricsResponse = await fetch(application.diagnosticsUrl("/metrics"));
    const metrics = await metricsResponse.text();

    expect(metrics).toContain('queue="nutsnews.worker.enrichment.v1",outcome="active"} 0');
    expect(metrics).toContain('nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-enrichment"} 0');

    await application.stop();
  });
});

class GatedLocalBrokerTransport extends LocalBrokerTransport {
  readonly connectStarted = deferred<undefined>();
  readonly secondClose = deferred<undefined>();
  closeCalls = 0;
  private readonly connectGate = deferred<undefined>();

  override async connect(): Promise<void> {
    this.connectStarted.resolve(undefined);
    await this.connectGate.promise;
    await super.connect();
  }

  releaseConnect(): void {
    this.connectGate.resolve(undefined);
  }

  override async close(): Promise<void> {
    this.closeCalls += 1;
    await super.close();

    if (this.closeCalls === 2) {
      this.secondClose.resolve(undefined);
    }
  }
}

class NeverClosingGatedBrokerTransport extends GatedLocalBrokerTransport {
  readonly closeStarted = deferred<undefined>();

  override close(): Promise<void> {
    this.closeStarted.resolve(undefined);
    return new Promise<void>(() => undefined);
  }
}

async function expectHealth(url: string, statusCode: number, status: string): Promise<void> {
  const response = await fetch(url);
  const body = await response.json() as { readonly status: string };

  expect(response.status).toBe(statusCode);
  expect(body.status).toBe(status);
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject
  };
}
