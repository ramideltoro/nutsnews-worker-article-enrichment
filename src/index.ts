import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
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
import { createEnrichmentHttpServer } from "./http.js";
import {
  DefaultEnrichmentDnsPolicy,
  InMemoryEnrichmentBodyStore,
  NodeEnrichmentHttpClient,
  SimpleEnrichmentHtmlParser
} from "./production.js";
import { PayloadRabbitMqTransport } from "./rabbitmq-transport.js";
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
  PayloadRabbitMqTransport
} from "./rabbitmq-transport.js";
export {
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
  start(): Promise<void>;
  stop(): Promise<void>;
}

export function createEnrichmentApplication(config = loadEnrichmentConfig()): EnrichmentApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host
  };
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createPrometheusRuntimeTelemetrySink({
        identity
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const bodyStore = new InMemoryEnrichmentBodyStore();
  const productionBrokerTransport = config.dependencyMode === "production"
    ? new PayloadRabbitMqTransport({
        url: requiredEnv("NUTSNEWS_ENRICHMENT_RABBITMQ_URL"),
        prefetch: config.prefetch,
        clock: SYSTEM_RUNTIME_CLOCK
      })
    : undefined;
  const productionDnsPolicy = config.dependencyMode === "production"
    ? new DefaultEnrichmentDnsPolicy()
    : undefined;
  const baseDependencies = createLocalEnrichmentDependencies({
    clock: SYSTEM_RUNTIME_CLOCK,
    ...(productionBrokerTransport === undefined ? {} : {
      brokerTransport: productionBrokerTransport
    }),
    ...(productionDnsPolicy === undefined ? {} : {
      dnsPolicy: productionDnsPolicy,
      httpClient: new NodeEnrichmentHttpClient({
        bodyStore,
        redirectDnsPolicy: productionDnsPolicy
      }),
      htmlParser: new SimpleEnrichmentHtmlParser(bodyStore)
    })
  });
  const dependencies = {
    ...baseDependencies,
    workHandler: createArticleEnrichmentWorkHandler({
      config,
      dependencies: baseDependencies,
      ...(telemetry === undefined ? {} : {
        telemetry
      })
    })
  };
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
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: logSink
    })
  });

  return {
    config,
    async start(): Promise<void> {
      assertPackageCompatibility();
      await service.start();
      await httpServer.listen();
      shutdown.start();
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
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
        await sink.emit(event);
      }
    }
  };
}

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.4.0") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== "0.4.0") {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
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
