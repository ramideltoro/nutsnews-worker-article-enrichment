import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadEnrichmentConfig,
  type EnrichmentConfig
} from "./config.js";
import { createArticleEnrichmentWorkHandler } from "./enrichment.js";
import type { EnrichmentDependencies } from "./dependencies.js";
import { createEnrichmentHttpServer } from "./http.js";
import { createEnrichmentPrometheusTelemetrySink } from "./metrics.js";
import {
  DefaultEnrichmentDnsPolicy,
  InMemoryEnrichmentBodyStore,
  NodeEnrichmentHttpClient,
  SimpleEnrichmentHtmlParser
} from "./production.js";
import { createProductionEnrichmentDurableAdapters } from "./production-adapters.js";
import { PayloadRabbitMqTransport } from "./rabbitmq-transport.js";
import {
  createEnrichmentFailClosedReconciler
} from "./reconciliation.js";
import { createEnrichmentService } from "./service.js";
import { createLocalEnrichmentDependencies } from "./test-doubles.js";

export {
  ENRICHMENT_CONFIG_SCHEMA,
  ENRICHMENT_SERVICE_NAME,
  ENRICHMENT_SERVICE_VERSION,
  EnrichmentConfigError,
  loadEnrichmentConfig,
  type EnrichmentConfig
} from "./config.js";
export type {
  EnrichmentBrokerOutbox,
  EnrichmentDatabaseTransaction,
  EnrichmentDatabaseTransactionRunner,
  EnrichmentDependencies,
  EnrichmentDependencyAdapterMode,
  EnrichmentDependencyAdapterModes,
  EnrichmentDependencyProbe,
  EnrichmentDnsPolicy,
  EnrichmentDnsPolicyDecision,
  EnrichmentHtmlParseInput,
  EnrichmentHtmlParser,
  EnrichmentHttpClient,
  EnrichmentHttpFetchRequest,
  EnrichmentHttpFetchResponse,
  EnrichmentImageCandidate,
  EnrichmentParsedMetadata,
  EnrichmentStateStore,
  EnrichmentStoredResult,
  EnrichmentWorkHandler,
  EnrichmentWorkTools
} from "./dependencies.js";
export {
  createArticleEnrichmentWorkHandler,
  type ArticleEnrichmentWorkHandlerOptions
} from "./enrichment.js";
export {
  createEnrichmentHttpServer,
  type EnrichmentHttpServer
} from "./http.js";
export {
  sha256Hex,
  stableUuid
} from "./ids.js";
export {
  ENRICHMENT_STAGE_LATENCY_BUCKETS_SECONDS,
  createEnrichmentPrometheusTelemetrySink,
  type EnrichmentHealthOutcome,
  type EnrichmentHealthProbe,
  type EnrichmentMetricsSink,
  type EnrichmentPrometheusTelemetrySink,
  type EnrichmentStageOutcome
} from "./metrics.js";
export {
  createEnrichmentService,
  type EnrichmentService
} from "./service.js";
export {
  DefaultEnrichmentDnsPolicy,
  EnrichmentHttpError,
  InMemoryEnrichmentBodyStore,
  NodeEnrichmentHttpClient,
  SimpleEnrichmentHtmlParser
} from "./production.js";
export {
  PostgresEnrichmentBrokerOutbox,
  PostgresEnrichmentStateStore,
  PostgresEnrichmentTransactionRunner,
  createProductionEnrichmentDurableAdapters,
  type ProductionEnrichmentDurableAdapters
} from "./production-adapters.js";
export {
  PayloadRabbitMqTransport
} from "./rabbitmq-transport.js";
export {
  ENRICHMENT_RECONCILIATION_CONFIRMATION,
  ENRICHMENT_RECONCILIATION_PATH,
  createEnrichmentFailClosedReconciler,
  type EnrichmentReconciliationReport,
  type EnrichmentReconciliationRequest,
  type EnrichmentReconciler
} from "./reconciliation.js";
export {
  ENRICHMENT_IDEMPOTENCY_CLAIM_LEASE_MS,
  InMemoryEnrichmentStateStore,
  LocalBrokerTransport,
  LocalEnrichmentBrokerOutbox,
  LocalEnrichmentDnsPolicy,
  LocalEnrichmentHtmlParser,
  LocalEnrichmentHttpClient,
  LocalEnrichmentTransactionRunner,
  LocalEnrichmentWorkHandler,
  ManualEnrichmentClock,
  createLocalEnrichmentDependencies,
  createMinimalEnrichmentDelivery,
  createMinimalEnrichmentEnvelope,
  createMinimalEnrichmentPayload
} from "./test-doubles.js";

export interface EnrichmentApplication {
  readonly config: EnrichmentConfig;
  diagnosticsUrl(path?: string): string;
  start(): Promise<void>;
  stop(): Promise<void>;
}

export interface EnrichmentApplicationOptions {
  readonly dependencies?: EnrichmentDependencies;
}

export class EnrichmentStartupTimeoutError extends Error {
  readonly timeoutMs: number;

  constructor(timeoutMs: number) {
    super(`Enrichment service startup exceeded ${String(timeoutMs)} milliseconds.`);
    this.name = "EnrichmentStartupTimeoutError";
    this.timeoutMs = timeoutMs;
  }
}

export function createEnrichmentApplication(
  config = loadEnrichmentConfig(),
  options: EnrichmentApplicationOptions = {}
): EnrichmentApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host,
    revision: config.buildRevision,
    deployment: config.dependencyMode === "production"
      ? "shadow"
      : config.environment === "test" ? "test" : "local",
    adapter: config.dependencyMode === "production" ? "production" : "in_memory"
  } as const;
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createEnrichmentPrometheusTelemetrySink({
        identity
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const reconciliationToken = reconciliationTokenFromEnv();
  const applicationDependencies = options.dependencies === undefined
    ? createApplicationDependencies(config, telemetry)
    : { dependencies: options.dependencies };
  const dependencies = applicationDependencies.dependencies;
  const service = createEnrichmentService({
    config,
    dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const httpServer = createEnrichmentHttpServer({
    config,
    service,
    reconciler: createEnrichmentFailClosedReconciler(SYSTEM_RUNTIME_CLOCK),
    ...(reconciliationToken === undefined ? {} : {
      reconciliationToken
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        await httpServer.close();
      },
      async () => {
        await service.stop();
      },
      async () => {
        await applicationDependencies.close?.();
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: {
        flush: async () => {
          try {
            await logSink.flush();
          } catch {
            // Telemetry flushing is best effort and must not block shutdown.
          }
        }
      }
    })
  });

  return {
    config,
    diagnosticsUrl: (path) => httpServer.url(path),
    async start(): Promise<void> {
      assertPackageCompatibility();
      await httpServer.listen();
      shutdown.start();

      try {
        await withStartupTimeout(service.start(), config.startupTimeoutMs);
      } catch (error: unknown) {
        shutdown.stop();
        await cleanupFailedStartup(service, httpServer, config.startupTimeoutMs);
        throw error;
      }
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
}

async function cleanupFailedStartup(
  service: ReturnType<typeof createEnrichmentService>,
  httpServer: ReturnType<typeof createEnrichmentHttpServer>,
  timeoutMs: number
): Promise<void> {
  await Promise.all([
    ignoreCleanupFailure(() => withCleanupTimeout(service.stop(), timeoutMs)),
    ignoreCleanupFailure(() => withCleanupTimeout(httpServer.close(), timeoutMs))
  ]);
}

async function ignoreCleanupFailure(operation: () => Promise<void>): Promise<void> {
  try {
    await operation();
  } catch {
    // Preserve the startup error after attempting every bounded cleanup step.
  }
}

async function withCleanupTimeout(operation: Promise<void>, timeoutMs: number): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  try {
    await Promise.race([
      operation,
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

function createApplicationDependencies(
  config: EnrichmentConfig,
  telemetry: RuntimeTelemetrySink | undefined
): { readonly dependencies: EnrichmentDependencies; readonly close?: () => Promise<void> } {
  const bodyStore = new InMemoryEnrichmentBodyStore();
  const productionBrokerTransport = config.dependencyMode === "production"
    ? new PayloadRabbitMqTransport({
        url: requiredEnv("NUTSNEWS_ENRICHMENT_RABBITMQ_URL"),
        prefetch: config.prefetch,
        connectTimeoutMs: config.startupTimeoutMs,
        clock: SYSTEM_RUNTIME_CLOCK,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      })
    : undefined;
  const productionNetworkAdapters = config.dependencyMode === "production"
    ? (() => {
        const dnsPolicy = new DefaultEnrichmentDnsPolicy();
        return {
          dnsPolicy,
          httpClient: new NodeEnrichmentHttpClient({
            bodyStore,
            redirectDnsPolicy: dnsPolicy
          })
        };
      })()
    : undefined;
  const productionDurableAdapters = config.dependencyMode === "production"
    ? createProductionEnrichmentDurableAdapters({
        databaseUrl: requiredEnv("NUTSNEWS_ENRICHMENT_DATABASE_URL"),
        applicationName: config.serviceName,
        maxConnections: Math.max(3, config.concurrency + 2),
        timeoutMs: config.startupTimeoutMs
      })
    : undefined;
  const localDependencies = createLocalEnrichmentDependencies({
    clock: SYSTEM_RUNTIME_CLOCK,
    ...(productionBrokerTransport === undefined ? {} : {
      brokerTransport: productionBrokerTransport
    }),
    ...(productionNetworkAdapters === undefined ? {} : {
      dnsPolicy: productionNetworkAdapters.dnsPolicy,
      httpClient: productionNetworkAdapters.httpClient,
      htmlParser: new SimpleEnrichmentHtmlParser(bodyStore)
    })
  });
  const baseDependencies = productionDurableAdapters === undefined
    ? localDependencies
    : {
        ...localDependencies,
        ...productionDurableAdapters
      };

  return {
    dependencies: {
      ...baseDependencies,
      workHandler: createArticleEnrichmentWorkHandler({
        config,
        dependencies: baseDependencies,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      })
    },
    ...(productionDurableAdapters === undefined ? {} : {
      close: async () => {
        await Promise.all([
          productionDurableAdapters.close(),
          productionNetworkAdapters?.httpClient.close()
        ]);
      }
    })
  };
}

async function withStartupTimeout(operation: Promise<void>, timeoutMs: number): Promise<void> {
  let timeout: NodeJS.Timeout | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => {
      reject(new EnrichmentStartupTimeoutError(timeoutMs));
    }, timeoutMs);
  });

  try {
    await Promise.race([
      operation,
      deadline
    ]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

function reconciliationTokenFromEnv(): string | undefined {
  const serviceToken = process.env.NUTSNEWS_ENRICHMENT_RECONCILIATION_TOKEN?.trim();
  const globalToken = process.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN?.trim();
  const token = serviceToken !== undefined && serviceToken.length > 0 ? serviceToken : globalToken;

  return token === undefined || token.length === 0 ? undefined : token;
}

function combineTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        try {
          await sink.emit(event);
        } catch {
          // Each telemetry sink is isolated so another sink can still receive the event.
        }
      }
    }
  };
}

export const SUPPORTED_CONTRACTS_PACKAGE_VERSION = "1.0.0";
export const SUPPORTED_RUNTIME_PACKAGE_VERSION = "1.0.0";

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;
  const runtimeContractsVersion: string = runtime.contractsPackageVersion;

  if (contractsVersion !== SUPPORTED_CONTRACTS_PACKAGE_VERSION) {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== SUPPORTED_RUNTIME_PACKAGE_VERSION) {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }

  if (runtimeContractsVersion !== SUPPORTED_CONTRACTS_PACKAGE_VERSION) {
    throw new Error(`Unsupported runtime contracts package version ${runtimeContractsVersion}.`);
  }
}

function requiredEnv(key: string): string {
  const value = process.env[key]?.trim();

  if (value === undefined || value.length === 0) {
    throw new Error(`${key} is required for production enrichment dependencies.`);
  }

  return value;
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createEnrichmentApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start enrichment");
    process.exitCode = 1;
  });
}
