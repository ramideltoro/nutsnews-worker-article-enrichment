import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadEnrichmentConfig } from "../src/config.js";
import { createEnrichmentPrometheusTelemetrySink } from "../src/metrics.js";
import { createEnrichmentService } from "../src/service.js";
import {
  InMemoryEnrichmentStateStore,
  LocalBrokerTransport,
  LocalEnrichmentBrokerOutbox,
  LocalEnrichmentDnsPolicy,
  LocalEnrichmentHtmlParser,
  LocalEnrichmentHttpClient,
  LocalEnrichmentTransactionRunner,
  LocalEnrichmentWorkHandler,
  createLocalEnrichmentDependencies,
  createMinimalEnrichmentDelivery
} from "../src/test-doubles.js";

describe("createEnrichmentService", () => {
  it("starts, becomes ready, registers enrichment and approval routes, and drains cleanly", async () => {
    const context = createServiceContext();

    await context.service.start();

    expect(context.service.isStarted).toBe(true);
    expect(context.service.consumer?.stage).toBe("enrichment");
    expect(context.broker.assertedRoutes.map((route) => route.stage)).toEqual([
      "enrichment",
      "approval"
    ]);
    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.startup()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("ok");
    expect(context.metrics.collect()).toContain("nutsnews_worker_inflight");
    expect(context.metrics.collect()).not.toContain("nutsnews_worker_dependency_duration_ms");

    await context.service.stop();

    expect(context.service.isStarted).toBe(false);
    expect(context.service.broker.state).toBe("closed");
    expect(context.telemetry.events.some((event) => event.name === "runtime.broker.state_changed")).toBe(true);
  });

  it("delegates a valid enrichment delivery and acks duplicate replays without business logic", async () => {
    const context = createServiceContext();
    const delivery = createMinimalEnrichmentDelivery();

    await context.service.start();

    await expect(context.broker.deliverEnrichment(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverEnrichment(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.workHandler.handled[0]?.payload).toMatchObject({
      requestId: "enrichment-req-001",
      canonicalArticleId: "article-001",
      candidateId: "candidate-world-001",
      reason: "new"
    });

    await context.service.stop();
  });

  it("waits for an in-flight delivery during shutdown without wall-clock sleeps", async () => {
    const context = createServiceContext();
    const gate = deferred<undefined>();
    const started = deferred<undefined>();

    context.workHandler.handleGate = gate.promise;
    context.workHandler.onHandleStart = () => {
      started.resolve(undefined);
    };

    await context.service.start();
    const delivery = context.broker.deliverEnrichment();
    await started.promise;
    const stop = context.service.stop();

    expect(context.service.isDraining).toBe(true);
    expect(context.workHandler.handled).toHaveLength(0);

    gate.resolve(undefined);
    await expect(delivery).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await stop;

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.service.isStarted).toBe(false);
  });

  it("reports readiness unhealthy when network-bound dependencies are unhealthy", async () => {
    const context = createServiceContext();

    context.httpClient.status = "unhealthy";
    await context.service.start();

    expect((await context.service.health.readiness()).status).toBe("unhealthy");

    await context.service.stop();
  });
  it("reports readiness unhealthy when the main queue consumer is cancelled", async () => {
    const context = createServiceContext();

    await context.service.start();
    await context.service.consumer?.cancel();

    const readiness = await context.service.health.readiness();
    expect(readiness.status).toBe("unhealthy");
    const consumerCheck = readiness.checks.find((check) => check.name === "rabbitmq-consumer");
    expect(consumerCheck?.status).toBe("unhealthy");
    expect(consumerCheck?.details).toMatchObject({
      queue: "nutsnews.worker.enrichment.v1",
      activeConsumers: 0
    });

    await context.service.stop();
  });

  it("does not connect or consume in production when durable adapters are local", async () => {
    const config = loadEnrichmentConfig({
      HOSTNAME: "enrichment-production-test",
      NUTSNEWS_ENVIRONMENT: "production",
      NUTSNEWS_ENRICHMENT_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_ENRICHMENT_DATABASE_URL: "postgres://secret@example.invalid/enrichment",
      NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE: "production",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_RABBITMQ_URL: "amqp://secret@example.invalid",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalEnrichmentDependencies();
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
      dependencies,
      metrics
    });

    await service.start();

    expect(service.isStarted).toBe(true);
    expect(service.consumer).toBeUndefined();
    expect(service.broker.state).toBe("idle");
    expect((dependencies.brokerTransport as LocalBrokerTransport).assertedRoutes).toHaveLength(0);
    await expect((dependencies.brokerTransport as LocalBrokerTransport).deliverEnrichment()).rejects.toThrow(
      "No local consumer is registered for enrichment."
    );
    expect((await service.health.liveness()).status).toBe("ok");
    expect((await service.health.startup()).status).toBe("ok");

    const readiness = await service.health.readiness();

    expect(readiness.status).toBe("unhealthy");
    expect(readiness.checks.find((check) => check.name === "production-adapters")).toMatchObject({
      status: "unhealthy",
      details: {
        mode: "production",
        reason: "production-durable-adapters-unavailable",
        adapterMode: "local",
        stateStoreAdapter: "local",
        transactionRunnerAdapter: "local",
        brokerOutboxAdapter: "local"
      }
    });
    expect(JSON.stringify(readiness)).not.toContain("secret");
    expect(metrics.collect()).toContain('nutsnews_worker_consumer_active{environment="production",service="enrichment",queue="nutsnews.worker.enrichment.v1"} 0');
    expect(metrics.collect()).toContain('nutsnews_worker_expected_active{environment="production",service="enrichment"} 0');

    await service.stop();
  });

  it("requires healthy production durable probes before connecting the broker", async () => {
    const config = loadEnrichmentConfig({
      NUTSNEWS_ENRICHMENT_DATABASE_URL: "postgres://example.invalid/enrichment",
      NUTSNEWS_ENRICHMENT_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE: "production",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalEnrichmentDependencies();

    setProductionAdapterMode(dependencies.stateStore);
    setProductionAdapterMode(dependencies.transactionRunner);
    setProductionAdapterMode(dependencies.brokerOutbox);
    (dependencies.stateStore as InMemoryEnrichmentStateStore).status = "unhealthy";

    const service = createEnrichmentService({
      config,
      dependencies
    });

    await service.start();

    expect(service.consumer).toBeUndefined();
    expect(service.broker.state).toBe("idle");
    expect((dependencies.brokerTransport as LocalBrokerTransport).assertedRoutes).toHaveLength(0);
    expect((await service.health.readiness()).status).toBe("unhealthy");

    await service.stop();
  });

  it("cancels an active production consumer and disposes the current delivery when durability degrades", async () => {
    const config = loadEnrichmentConfig({
      HOSTNAME: "enrichment-production-degradation-test",
      NUTSNEWS_ENVIRONMENT: "production",
      NUTSNEWS_ENRICHMENT_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_ENRICHMENT_DATABASE_URL: "postgres://example.invalid/enrichment",
      NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE: "production",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_ENRICHMENT_STARTUP_TIMEOUT_MS: "100",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalEnrichmentDependencies();
    const telemetry = createBufferedRuntimeTelemetrySink();

    setProductionAdapterMode(dependencies.stateStore);
    setProductionAdapterMode(dependencies.transactionRunner);
    setProductionAdapterMode(dependencies.brokerOutbox);

    const service = createEnrichmentService({
      config,
      dependencies,
      telemetry
    });
    const broker = dependencies.brokerTransport as LocalBrokerTransport;
    const stateStore = dependencies.stateStore as InMemoryEnrichmentStateStore;
    const workHandler = dependencies.workHandler as LocalEnrichmentWorkHandler;

    await service.start();
    telemetry.clear();
    stateStore.status = "degraded";

    await expect(broker.deliverEnrichment()).resolves.toMatchObject({
      action: "retry",
      reason: "production-durable-adapters-unhealthy"
    });
    expect(workHandler.handled).toHaveLength(0);
    expect(service.consumer).toBeUndefined();
    expect(service.broker.consumerStatus("enrichment").activeConsumers).toBe(0);
    expect(telemetry.events.filter((event) => event.name.startsWith("runtime.message.")).map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.retry"
    ]);

    stateStore.status = "ok";
    await expect(broker.deliverEnrichment()).rejects.toThrow("No local consumer is registered for enrichment.");
    expect((await service.health.readiness()).status).toBe("unhealthy");

    await service.stop();
  });

  it("bounds readiness dependency probes that never settle", async () => {
    const context = createServiceContext({
      NUTSNEWS_ENRICHMENT_STARTUP_TIMEOUT_MS: "100"
    });

    await context.service.start();
    Object.defineProperty(context.httpClient, "probe", {
      configurable: true,
      value: () => new Promise<never>(() => undefined)
    });
    const startedAt = Date.now();
    const readiness = await context.service.health.readiness();

    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(readiness.status).toBe("unhealthy");
    expect(readiness.checks.find((check) => check.name === "http-client")).toMatchObject({
      status: "unhealthy",
      details: {
        summary: "dependency probe timed out"
      }
    });

    await context.service.stop();
  });
});

function setProductionAdapterMode(dependency: { readonly adapterMode: string }): void {
  Object.defineProperty(dependency, "adapterMode", {
    configurable: true,
    value: "production"
  });
}

function createServiceContext(env: NodeJS.ProcessEnv = {}) {
  const config = loadEnrichmentConfig({
    NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
    NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent",
    ...env
  });
  const dependencies = createLocalEnrichmentDependencies();
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createPrometheusRuntimeTelemetrySink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    }
  });
  const service = createEnrichmentService({
    config,
    dependencies,
    telemetry,
    metrics
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    dnsPolicy: dependencies.dnsPolicy as LocalEnrichmentDnsPolicy,
    htmlParser: dependencies.htmlParser as LocalEnrichmentHtmlParser,
    httpClient: dependencies.httpClient as LocalEnrichmentHttpClient,
    metrics,
    outbox: dependencies.brokerOutbox as LocalEnrichmentBrokerOutbox,
    service,
    stateStore: dependencies.stateStore as InMemoryEnrichmentStateStore,
    telemetry,
    transactionRunner: dependencies.transactionRunner as LocalEnrichmentTransactionRunner,
    workHandler: dependencies.workHandler as LocalEnrichmentWorkHandler
  };
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
