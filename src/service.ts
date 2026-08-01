import {
  getRetryDestination,
  getWorkerRoute,
  validateWorkerEnvelope,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createBrokerConsumerReadinessCheck,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  createRuntimeMessageProcessor,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type RuntimeHealthCheck,
  type RuntimeHealthReport,
  type RuntimeHealthProbeSet,
  type RuntimeIdempotencyStore,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { EnrichmentConfig } from "./config.js";
import type {
  EnrichmentMetricsSink,
  EnrichmentPrometheusTelemetrySink
} from "./metrics.js";
import type {
  EnrichmentDependencies,
  EnrichmentDependencyAdapterModes,
  EnrichmentDependencyProbe
} from "./dependencies.js";

export interface EnrichmentServiceOptions {
  readonly config: EnrichmentConfig;
  readonly dependencies: EnrichmentDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: EnrichmentMetricsSink;
}

export interface EnrichmentService {
  readonly broker: BrokerLifecycle;
  readonly health: RuntimeHealthProbeSet;
  readonly isStarted: boolean;
  readonly isDraining: boolean;
  readonly consumer: BrokerConsumerHandle | undefined;
  start(): Promise<void>;
  stop(): Promise<void>;
  processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult>;
}

export function createEnrichmentService(options: EnrichmentServiceOptions): EnrichmentService {
  const enrichmentRoute = getWorkerRoute("enrichment");
  const approvalRoute = getWorkerRoute("approval");
  const telemetry = bestEffortTelemetrySink(options.telemetry);
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      enrichmentRoute,
      approvalRoute
    ],
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  let productionDurabilityFailureHandler = (): Promise<void> => Promise.resolve();
  const sharedProcessor = createRuntimeMessageProcessor({
    stage: "enrichment",
    clock: options.dependencies.clock,
    idempotencyStore: classifyStateStoreFailures(
      options.dependencies.stateStore,
      () => productionDurabilityFailureHandler()
    ),
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    handler: async (context) => {
      try {
        return await drain.track(async () => {
          setInFlight(options.metrics, enrichmentRoute.mainQueue.name, drain.inFlight);
          const result = await options.dependencies.workHandler.handle(context, {
            publish: (command) => broker.publish(command),
            recordOutbox: (command, receipt) => runDurableDependencyOperation(
              () => options.dependencies.brokerOutbox.record(command, receipt),
              () => productionDurabilityFailureHandler()
            ),
            withTransaction: (operation) => runDurableDependencyOperation(
              () => options.dependencies.transactionRunner.withTransaction(operation),
              () => productionDurabilityFailureHandler()
            )
          });

          await emitRuntimeTelemetry(telemetry, {
            name: "runtime.dependency.observed",
            level: result.status === "ok" ? "info" : "warn",
            at: runtimeNow(options.dependencies.clock),
            stage: "enrichment",
            queue: enrichmentRoute.mainQueue.name,
            outcome: result.status === "ok" ? "success" : result.status === "retry" ? "retry" : "failure",
            attributes: {
              event: "enrichment.message.delegated",
              dependency: options.dependencies.workHandler.name,
              shadowMode: options.config.shadowMode
            }
          });

          return result;
        });
      } finally {
        setInFlight(options.metrics, enrichmentRoute.mainQueue.name, drain.inFlight);
      }
    }
  });
  let started = false;
  let consumer: BrokerConsumerHandle | undefined;
  let lifecycleGeneration = 0;
  let durabilityFailureDetected = false;
  const reportConsumerActive = (activeConsumers: number): void => {
    setConsumerActive(options.metrics, activeConsumers);
  };

  const cancelConsumerForDurability = async (): Promise<void> => {
    if (options.config.dependencyMode !== "production" && !isProductionEnvironment(options.config.environment)) {
      return;
    }

    durabilityFailureDetected = true;
    const activeConsumer = consumer;

    consumer = undefined;
    setHealthProbe(options.metrics, "readiness", "unhealthy");

    if (activeConsumer !== undefined) {
      await settleWithinBound(() => activeConsumer.cancel(), options.config.startupTimeoutMs);
    }

    reportConsumerActive(0);
  };
  productionDurabilityFailureHandler = cancelConsumerForDurability;
  const ensureProductionDurability = async (): Promise<boolean> => {
    const ready = await productionAdaptersReady(options.config, options.dependencies);

    if (!ready) {
      await cancelConsumerForDurability();
    }

    return ready;
  };
  const processor = async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const startedAtMs = options.dependencies.clock.now().getTime();

    try {
      if ((options.config.dependencyMode === "production" || isProductionEnvironment(options.config.environment))
        && !(await ensureProductionDurability())) {
        await emitRuntimeTelemetry(telemetry, {
          name: "runtime.message.started",
          level: "info",
          at: runtimeNow(options.dependencies.clock),
          stage: "enrichment",
          queue: enrichmentRoute.mainQueue.name,
          outcome: "started"
        });

        return await completeProcessorFailure(
          delivery,
          new EnrichmentProcessorDispositionError("production-durable-adapters-unhealthy"),
          telemetry,
          options.dependencies.clock,
          startedAtMs
        );
      }

      return await sharedProcessor(delivery);
    } catch (error: unknown) {
      return completeProcessorFailure(
        delivery,
        error,
        telemetry,
        options.dependencies.clock,
        startedAtMs
      );
    }
  };

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      const probes = createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started),
          configurationModeCheck(options.config)
        ],
        readinessChecks: [
          configurationModeCheck(options.config),
          brokerReadinessCheck(broker),
          createBrokerConsumerReadinessCheck(broker, "enrichment"),
          dependencyReadinessCheck(
            "enrichment-state",
            options.dependencies.stateStore,
            options.config.startupTimeoutMs,
            options.config.dependencyMode === "production" ? cancelConsumerForDurability : undefined
          ),
          dependencyReadinessCheck(
            "database-transactions",
            options.dependencies.transactionRunner,
            options.config.startupTimeoutMs,
            options.config.dependencyMode === "production" ? cancelConsumerForDurability : undefined
          ),
          dependencyReadinessCheck(
            "broker-outbox",
            options.dependencies.brokerOutbox,
            options.config.startupTimeoutMs,
            options.config.dependencyMode === "production" ? cancelConsumerForDurability : undefined
          ),
          dependencyReadinessCheck("http-client", options.dependencies.httpClient, options.config.startupTimeoutMs),
          dependencyReadinessCheck("dns-policy", options.dependencies.dnsPolicy, options.config.startupTimeoutMs),
          dependencyReadinessCheck("html-parser", options.dependencies.htmlParser, options.config.startupTimeoutMs),
          productionAdapterReadinessCheck(
            options.config,
            options.dependencies,
            cancelConsumerForDurability
          ),
          shadowModeCheck(options.config)
        ],
        clock: options.dependencies.clock,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      });

      return observeHealthProbes(probes, options.metrics);
    },
    get isStarted(): boolean {
      return started;
    },
    get isDraining(): boolean {
      return drain.isDraining;
    },
    get consumer(): BrokerConsumerHandle | undefined {
      return consumer;
    },
    async start(): Promise<void> {
      if (started) {
        return;
      }

      const startGeneration = lifecycleGeneration + 1;

      lifecycleGeneration = startGeneration;
      const durableAdaptersReady = await ensureProductionDurability();

      assertCurrentStart(startGeneration, lifecycleGeneration);

      if (durableAdaptersReady) {
        await broker.start();

        if (startGeneration !== lifecycleGeneration) {
          await broker.stop("startup-cancelled").catch(() => undefined);
          throw enrichmentStartCancelledError();
        }

        const brokerConsumer = await broker.consume("enrichment", processor);

        if (startGeneration !== lifecycleGeneration || durabilityFailureDetected) {
          await brokerConsumer.cancel().catch(() => undefined);

          if (durabilityFailureDetected && startGeneration === lifecycleGeneration) {
            consumer = undefined;
            setHealthProbe(options.metrics, "readiness", "unhealthy");
          }
        }

        if (startGeneration !== lifecycleGeneration) {
          await broker.stop("startup-cancelled").catch(() => undefined);
          throw enrichmentStartCancelledError();
        }

        if (!durabilityFailureDetected) {
          const wrappedConsumer: BrokerConsumerHandle = {
            stage: brokerConsumer.stage,
            cancel: async () => {
              await brokerConsumer.cancel();

              if (consumer === wrappedConsumer) {
                consumer = undefined;
              }
              reportConsumerActive(0);
              setHealthProbe(options.metrics, "readiness", "unhealthy");
            }
          };

          consumer = wrappedConsumer;
        }
      } else {
        consumer = undefined;
        setHealthProbe(options.metrics, "readiness", "unhealthy");
      }
      started = true;
      reportConsumerActive(consumer === undefined ? 0 : 1);
      setHealthProbe(
        options.metrics,
        "startup",
        configurationModeValid(options.config) ? "ok" : "unhealthy"
      );
      setInFlight(options.metrics, enrichmentRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(telemetry, {
        name: "runtime.dependency.observed",
        level: consumer === undefined ? "warn" : "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "enrichment",
        queue: enrichmentRoute.mainQueue.name,
        outcome: consumer === undefined ? "failure" : "success",
        attributes: {
          dependency: "enrichment-shell",
          mode: options.config.dependencyMode,
          adapterMode: aggregateAdapterMode(options.dependencies),
          durableAdaptersReady,
          prefetch: options.config.prefetch,
          concurrency: options.config.concurrency,
          maxResponseBytes: options.config.fetch.maxResponseBytes,
          shadowMode: options.config.shadowMode,
          consumerStarted: consumer !== undefined
        }
      });
    },
    async stop(): Promise<void> {
      lifecycleGeneration += 1;

      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      setShutdownDraining(options.metrics, true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      setShutdownDraining(options.metrics, false);
      setInFlight(options.metrics, enrichmentRoute.mainQueue.name, drain.inFlight);
      reportConsumerActive(0);
      setHealthProbe(options.metrics, "startup", "unhealthy");
      setHealthProbe(options.metrics, "readiness", "unhealthy");
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies EnrichmentService;

  return service;
}

function assertCurrentStart(startGeneration: number, lifecycleGeneration: number): void {
  if (startGeneration !== lifecycleGeneration) {
    throw enrichmentStartCancelledError();
  }
}

function enrichmentStartCancelledError(): Error {
  const error = new Error("Enrichment startup was cancelled by a newer lifecycle transition.");

  error.name = "EnrichmentStartCancelledError";
  return error;
}

function productionAdaptersAvailable(
  config: EnrichmentConfig,
  dependencies: EnrichmentDependencies
): boolean {
  if (isProductionEnvironment(config.environment) && config.dependencyMode !== "production") {
    return false;
  }

  if (config.dependencyMode !== "production") {
    return true;
  }

  const modes = dependencyAdapterModes(dependencies);

  return modes.stateStore === "production"
    && modes.transactionRunner === "production"
    && modes.brokerOutbox === "production";
}

async function productionAdaptersReady(
  config: EnrichmentConfig,
  dependencies: EnrichmentDependencies
): Promise<boolean> {
  if (isProductionEnvironment(config.environment) && config.dependencyMode !== "production") {
    return false;
  }

  if (config.dependencyMode !== "production") {
    return true;
  }

  if (!productionAdaptersAvailable(config, dependencies)) {
    return false;
  }

  const probes = await Promise.all([
    probeWithinStartupBound(dependencies.stateStore, config.startupTimeoutMs),
    probeWithinStartupBound(dependencies.transactionRunner, config.startupTimeoutMs),
    probeWithinStartupBound(dependencies.brokerOutbox, config.startupTimeoutMs)
  ]);

  return probes.every((probe) => probe.status === "ok");
}

async function probeWithinStartupBound(
  dependency: {
    probe(): EnrichmentDependencyProbe | Promise<EnrichmentDependencyProbe>;
  },
  timeoutMs: number
): Promise<EnrichmentDependencyProbe> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  try {
    return await Promise.race([
      Promise.resolve().then(() => dependency.probe()),
      new Promise<EnrichmentDependencyProbe>((resolve) => {
        timeout = setTimeout(() => {
          resolve({
            status: "unhealthy",
            summary: "dependency probe timed out"
          });
        }, timeoutMs);
      })
    ]);
  } catch (error: unknown) {
    return {
      status: "unhealthy",
      summary: error instanceof Error && error.name.length > 0
        ? error.name
        : "dependency probe failed"
    };
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

async function settleWithinBound(operation: () => Promise<void>, timeoutMs: number): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  try {
    await Promise.race([
      Promise.resolve().then(operation).catch(() => undefined),
      new Promise<void>((resolve) => {
        timeout = setTimeout(resolve, timeoutMs);
      })
    ]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

function aggregateAdapterMode(
  dependencies: EnrichmentDependencies
): "local" | "production" | "unavailable" | "mixed" {
  const modes = dependencyAdapterModes(dependencies);

  return modes.stateStore === modes.transactionRunner && modes.stateStore === modes.brokerOutbox
    ? modes.stateStore
    : "mixed";
}

function dependencyAdapterModes(
  dependencies: EnrichmentDependencies
): EnrichmentDependencyAdapterModes {
  return {
    stateStore: dependencies.stateStore.adapterMode,
    transactionRunner: dependencies.transactionRunner.adapterMode,
    brokerOutbox: dependencies.brokerOutbox.adapterMode
  };
}

class EnrichmentProcessorDispositionError extends Error {
  readonly telemetryReason: string;

  constructor(telemetryReason: string) {
    super(telemetryReason);
    this.name = "EnrichmentProcessorDispositionError";
    this.telemetryReason = telemetryReason;
  }
}

function classifyStateStoreFailures(
  store: RuntimeIdempotencyStore,
  onFailure: () => Promise<void>
): RuntimeIdempotencyStore {
  return {
    claim: async (idempotencyKey, context) => stateStoreOperation(
      "idempotency-claim-error",
      () => store.claim(idempotencyKey, context),
      onFailure
    ),
    markCompleted: async (idempotencyKey, completion) => stateStoreOperation(
      "idempotency-completion-error",
      () => store.markCompleted(idempotencyKey, completion),
      onFailure
    ),
    markFailed: async (idempotencyKey, failure) => stateStoreOperation(
      "idempotency-failure-record-error",
      () => store.markFailed(idempotencyKey, failure),
      onFailure
    ),
    releaseClaim: async (idempotencyKey, failure) => stateStoreOperation(
      "idempotency-completion-error",
      () => store.releaseClaim(idempotencyKey, failure),
      onFailure
    )
  };
}

async function stateStoreOperation<T>(
  reason: string,
  operation: () => Promise<T>,
  onFailure: () => Promise<void>
): Promise<T> {
  try {
    return await operation();
  } catch {
    await onFailure();
    throw new EnrichmentProcessorDispositionError(reason);
  }
}

async function runDurableDependencyOperation<T>(
  operation: () => Promise<T>,
  onFailure: () => Promise<void>
): Promise<T> {
  try {
    return await operation();
  } catch (error: unknown) {
    await onFailure();
    throw error;
  }
}

async function completeProcessorFailure(
  delivery: RuntimeMessageDelivery,
  error: unknown,
  telemetry: RuntimeTelemetrySink | undefined,
  clock: EnrichmentDependencies["clock"],
  startedAtMs: number
): Promise<RuntimeMessageProcessingResult> {
  const queue = getWorkerRoute("enrichment").mainQueue.name;
  const durationMs = Math.max(0, clock.now().getTime() - startedAtMs);
  const envelopeResult = validateWorkerEnvelope(delivery.envelope);

  if (!envelopeResult.ok) {
    const issues = envelopeResult.issues.map((issue) => ({
      path: issue.path,
      code: issue.code,
      message: issue.message
    }));
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.invalid",
      level: "warn",
      at: runtimeNow(clock),
      stage: "enrichment",
      queue,
      durationMs,
      outcome: "invalid",
      attributes: {
        issueCode: issues[0]?.code ?? "invalid-envelope",
        issuePath: issues[0]?.path ?? "$"
      }
    });

    return {
      action: "dlq",
      reason: "invalid-envelope",
      issues
    };
  }

  const envelope = envelopeResult.value;

  if (envelope.route !== "enrichment") {
    const issues = [
      {
        path: "$.route",
        code: "stage-mismatch",
        message: `Envelope route ${envelope.route} does not match processor stage enrichment.`
      }
    ];
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.invalid",
      level: "warn",
      at: runtimeNow(clock),
      stage: "enrichment",
      ...envelopeTelemetryFields(envelope, queue, durationMs),
      outcome: "invalid",
      attributes: {
        issueCode: "stage-mismatch",
        issuePath: "$.route"
      }
    });

    return terminalFailureResult(envelope, "stage-mismatch", issues);
  }

  const reason = error instanceof EnrichmentProcessorDispositionError
    ? error.telemetryReason
    : "processor-error";
  const result = retryOrDlqResult(envelope, reason);
  const destination = result.destination.name;
  const event: RuntimeTelemetryEvent = result.action === "retry"
    ? {
        name: "runtime.message.retry",
        level: "warn",
        at: runtimeNow(clock),
        stage: "enrichment",
        ...envelopeTelemetryFields(envelope, queue, durationMs),
        outcome: "retry",
        attributes: {
          reason,
          destination
        }
      }
    : {
        name: "runtime.message.dlq",
        level: "error",
        at: runtimeNow(clock),
        stage: "enrichment",
        ...envelopeTelemetryFields(envelope, queue, durationMs),
        outcome: "dlq",
        attributes: {
          reason,
          destination
        }
      };
  await emitRuntimeTelemetry(telemetry, event);

  return result;
}

function retryOrDlqResult(envelope: WorkerMessageEnvelope, reason: string) {
  const destination = getRetryDestination(envelope.route, envelope.attempt.count);

  return "ttlMs" in destination
    ? {
        action: "retry",
        reason,
        envelope,
        destination
      } as const
    : {
        action: "dlq",
        reason,
        envelope,
        destination
      } as const;
}

function terminalFailureResult(
  envelope: WorkerMessageEnvelope,
  reason: string,
  issues: readonly { readonly path: string; readonly code: string; readonly message: string }[]
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.max);

  return "routingKey" in destination && !("ttlMs" in destination)
    ? {
        action: "dlq",
        reason,
        envelope,
        destination,
        issues
      }
    : {
        action: "dlq",
        reason,
        envelope,
        issues
      };
}

function envelopeTelemetryFields(
  envelope: WorkerMessageEnvelope,
  queue: string,
  durationMs: number
): Readonly<Record<string, string | number>> {
  const base = {
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    traceparent: envelope.traceparent,
    idempotencyKey: envelope.idempotencyKey,
    queue,
    attempt: envelope.attempt.count,
    durationMs
  } as const;

  return envelope.tracestate === undefined
    ? base
    : {
        ...base,
        tracestate: envelope.tracestate
      };
}

function setConsumerActive(
  metrics: EnrichmentMetricsSink | undefined,
  activeConsumers: number
): void {
  runBestEffort(() => {
    if (isEnrichmentMetrics(metrics)) {
      metrics.setConsumerActive(activeConsumers);
    }
  });
}

function setHealthProbe(
  metrics: EnrichmentMetricsSink | undefined,
  probe: "liveness" | "startup" | "readiness",
  outcome: "ok" | "degraded" | "unhealthy"
): void {
  runBestEffort(() => {
    if (isEnrichmentMetrics(metrics)) {
      metrics.setHealthProbe(probe, outcome);
    }
  });
}

function setInFlight(
  metrics: EnrichmentMetricsSink | undefined,
  queue: string,
  value: number
): void {
  runBestEffort(() => metrics?.setInFlight(queue, value));
}

function setShutdownDraining(
  metrics: EnrichmentMetricsSink | undefined,
  draining: boolean
): void {
  runBestEffort(() => metrics?.setShutdownDraining(draining));
}

function runBestEffort(operation: () => unknown): void {
  try {
    const result = operation();

    if (isPromiseLike(result)) {
      void result.then(undefined, () => undefined);
    }
  } catch {
    // Telemetry is deliberately non-semantic and must never alter message handling.
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return typeof value === "object" && value !== null && "then" in value && typeof value.then === "function";
}

function bestEffortTelemetrySink(sink: RuntimeTelemetrySink | undefined): RuntimeTelemetrySink | undefined {
  if (sink === undefined) {
    return undefined;
  }

  return {
    emit: async (event) => {
      try {
        await sink.emit(event);
      } catch {
        // Telemetry is deliberately non-semantic and must never alter message handling.
      }
    }
  };
}

function observeHealthProbes(
  probes: RuntimeHealthProbeSet,
  metrics: EnrichmentMetricsSink | undefined
): RuntimeHealthProbeSet {
  const observe = async <T extends RuntimeHealthReport>(
    probe: "liveness" | "startup" | "readiness",
    operation: () => Promise<T>
  ): Promise<T> => {
    const report = await operation();
    setHealthProbe(metrics, probe, report.status);

    return report;
  };

  return {
    liveness: () => observe("liveness", () => probes.liveness()),
    startup: () => observe("startup", () => probes.startup()),
    readiness: () => observe("readiness", () => probes.readiness())
  };
}

function isEnrichmentMetrics(
  metrics: EnrichmentMetricsSink | undefined
): metrics is EnrichmentPrometheusTelemetrySink {
  return metrics !== undefined
    && "setConsumerActive" in metrics
    && typeof metrics.setConsumerActive === "function"
    && "setHealthProbe" in metrics
    && typeof metrics.setHealthProbe === "function";
}

function livenessCheck(): RuntimeHealthCheck {
  return {
    name: "process",
    critical: true,
    check: () => "ok"
  };
}

function startupCheck(isStarted: () => boolean): RuntimeHealthCheck {
  return {
    name: "service-started",
    critical: true,
    check: () => isStarted() ? "ok" : "unhealthy"
  };
}

function configurationModeCheck(config: EnrichmentConfig): RuntimeHealthCheck {
  return {
    name: "configuration-mode",
    critical: true,
    check: () => configurationModeValid(config)
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: "production-environment-requires-production-dependency-mode",
            dependencyMode: config.dependencyMode
          }
        }
  };
}

function configurationModeValid(config: EnrichmentConfig): boolean {
  return !isProductionEnvironment(config.environment) || config.dependencyMode === "production";
}

function isProductionEnvironment(environment: string): boolean {
  return environment.trim().toLowerCase() === "production";
}

function brokerReadinessCheck(broker: BrokerLifecycle): RuntimeHealthCheck {
  return {
    name: "broker-lifecycle",
    critical: true,
    check: () => broker.state === "ready"
      ? {
          status: "ok",
          details: {
            state: broker.state
          }
        }
      : {
          status: "unhealthy",
          details: {
            state: broker.state
          }
        }
  };
}

function dependencyReadinessCheck(
  name: string,
  dependency: {
    readonly name: string;
    probe(): EnrichmentDependencyProbe | Promise<EnrichmentDependencyProbe>;
  },
  timeoutMs: number,
  onUnavailable?: () => Promise<void>
): RuntimeHealthCheck {
  return {
    name,
    critical: true,
    check: async () => {
      const probe = await probeWithinStartupBound(dependency, timeoutMs);

      if (probe.status !== "ok") {
        await onUnavailable?.();
      }

      return {
        status: onUnavailable !== undefined && probe.status !== "ok" ? "unhealthy" : probe.status,
        details: {
          dependency: dependency.name,
          summary: probe.summary
        }
      };
    }
  };
}

function productionAdapterReadinessCheck(
  config: EnrichmentConfig,
  dependencies: EnrichmentDependencies,
  onUnavailable: () => Promise<void>
): RuntimeHealthCheck {
  return {
    name: "production-adapters",
    critical: true,
    check: async () => {
      const adapterMode = aggregateAdapterMode(dependencies);
      const adapterModes = dependencyAdapterModes(dependencies);

      if (config.dependencyMode !== "production") {
        return {
          status: "ok",
          details: {
            mode: "test",
            adapterMode,
            stateStoreAdapter: adapterModes.stateStore,
            transactionRunnerAdapter: adapterModes.transactionRunner,
            brokerOutboxAdapter: adapterModes.brokerOutbox
          }
        };
      }

      if (productionAdaptersAvailable(config, dependencies)) {
        return {
          status: "ok",
          details: {
            mode: "production",
            adapterMode,
            stateStoreAdapter: adapterModes.stateStore,
            transactionRunnerAdapter: adapterModes.transactionRunner,
            brokerOutboxAdapter: adapterModes.brokerOutbox
          }
        };
      }

      await onUnavailable();

      return {
        status: "unhealthy",
        details: {
          mode: "production",
          reason: "production-durable-adapters-unavailable",
          adapterMode,
          stateStoreAdapter: adapterModes.stateStore,
          transactionRunnerAdapter: adapterModes.transactionRunner,
          brokerOutboxAdapter: adapterModes.brokerOutbox
        }
      };
    }
  };
}

function shadowModeCheck(config: EnrichmentConfig): RuntimeHealthCheck {
  return {
    name: "shadow-mode",
    critical: true,
    check: () => config.shadowMode
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: "shadow-mode-disabled"
          }
        }
  };
}
