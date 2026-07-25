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
    expect(context.metrics.collect()).toContain("nutsnews_worker_dependency_duration_ms");

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
      candidateId: "candidate-world-001",
      imageStatus: "no_thumbnail"
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
});

function createServiceContext() {
  const config = loadEnrichmentConfig({
    NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
    NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
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
