import os from "node:os";

export const ENRICHMENT_SERVICE_NAME = "nutsnews-worker-article-enrichment" as const;
export const ENRICHMENT_SERVICE_VERSION = "0.1.0" as const;

export type EnrichmentDependencyMode = "test" | "production";
export type EnrichmentTelemetryLogMode = "stdout" | "silent";

export interface EnrichmentConfigVariable {
  readonly name: string;
  readonly description: string;
  readonly requiredInProduction: boolean;
  readonly sensitive: boolean;
  readonly defaultValue?: string;
}

export const ENRICHMENT_CONFIG_SCHEMA = [
  variable("NUTSNEWS_ENVIRONMENT", "Runtime environment label for logs and metrics.", false, false, "local"),
  variable("NUTSNEWS_ENRICHMENT_HTTP_HOST", "Health and metrics bind host.", false, false, "0.0.0.0"),
  variable("NUTSNEWS_ENRICHMENT_HTTP_PORT", "Health and metrics bind port.", false, false, "8080"),
  variable("NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE", "Use test dependencies locally or require production dependency presence.", false, false, "test"),
  variable("NUTSNEWS_ENRICHMENT_DATABASE_URL", "Backend shadow database connection string for enrichment state.", true, true),
  variable("NUTSNEWS_ENRICHMENT_RABBITMQ_URL", "Private RabbitMQ connection string.", true, true),
  variable("NUTSNEWS_ENRICHMENT_CONCURRENCY", "Maximum concurrent enrichment message handlers.", false, false, "6"),
  variable("NUTSNEWS_ENRICHMENT_PREFETCH", "Broker prefetch bound for enrichment deliveries.", false, false, "12"),
  variable("NUTSNEWS_ENRICHMENT_CONNECT_TIMEOUT_MS", "Maximum outbound connect timeout in milliseconds.", false, false, "5000"),
  variable("NUTSNEWS_ENRICHMENT_READ_TIMEOUT_MS", "Maximum outbound read timeout in milliseconds.", false, false, "10000"),
  variable("NUTSNEWS_ENRICHMENT_TOTAL_TIMEOUT_MS", "Maximum total page fetch timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_ENRICHMENT_MAX_RESPONSE_BYTES", "Maximum article page response size accepted by enrichment.", false, false, "1048576"),
  variable("NUTSNEWS_ENRICHMENT_MAX_REDIRECTS", "Maximum article page redirect hops.", false, false, "3"),
  variable("NUTSNEWS_ENRICHMENT_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_ENRICHMENT_SHADOW_MODE", "Keep enrichment output isolated from legacy ingestion.", false, false, "true"),
  variable("NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_ENRICHMENT_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true")
] as const satisfies readonly EnrichmentConfigVariable[];

export interface EnrichmentConfig {
  readonly serviceName: typeof ENRICHMENT_SERVICE_NAME;
  readonly serviceVersion: typeof ENRICHMENT_SERVICE_VERSION;
  readonly environment: string;
  readonly host: string;
  readonly http: {
    readonly host: string;
    readonly port: number;
  };
  readonly dependencyMode: EnrichmentDependencyMode;
  readonly dependencies: {
    readonly databaseConfigured: boolean;
    readonly rabbitmqConfigured: boolean;
  };
  readonly concurrency: number;
  readonly prefetch: number;
  readonly fetch: {
    readonly connectTimeoutMs: number;
    readonly readTimeoutMs: number;
    readonly totalTimeoutMs: number;
    readonly maxResponseBytes: number;
    readonly maxRedirects: number;
  };
  readonly shutdownTimeoutMs: number;
  readonly shadowMode: boolean;
  readonly telemetryLogs: EnrichmentTelemetryLogMode;
  readonly metricsEnabled: boolean;
}

export class EnrichmentConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`Invalid enrichment configuration: ${issues.join("; ")}`);
    this.name = "EnrichmentConfigError";
    this.issues = issues;
  }
}

export function loadEnrichmentConfig(env: NodeJS.ProcessEnv = process.env): EnrichmentConfig {
  const issues: string[] = [];
  const dependencyMode = parseDependencyMode(env.NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE, issues);
  const dependencies = {
    databaseConfigured: hasValue(env.NUTSNEWS_ENRICHMENT_DATABASE_URL),
    rabbitmqConfigured: hasValue(env.NUTSNEWS_ENRICHMENT_RABBITMQ_URL)
  };

  if (dependencyMode === "production") {
    requireConfigured("NUTSNEWS_ENRICHMENT_DATABASE_URL", dependencies.databaseConfigured, issues);
    requireConfigured("NUTSNEWS_ENRICHMENT_RABBITMQ_URL", dependencies.rabbitmqConfigured, issues);
  }

  const concurrency = parseInteger(env.NUTSNEWS_ENRICHMENT_CONCURRENCY, "NUTSNEWS_ENRICHMENT_CONCURRENCY", 6, 1, 64, issues);
  const prefetch = parseInteger(env.NUTSNEWS_ENRICHMENT_PREFETCH, "NUTSNEWS_ENRICHMENT_PREFETCH", 12, 1, 256, issues);
  const fetch = {
    connectTimeoutMs: parseInteger(env.NUTSNEWS_ENRICHMENT_CONNECT_TIMEOUT_MS, "NUTSNEWS_ENRICHMENT_CONNECT_TIMEOUT_MS", 5_000, 250, 30_000, issues),
    readTimeoutMs: parseInteger(env.NUTSNEWS_ENRICHMENT_READ_TIMEOUT_MS, "NUTSNEWS_ENRICHMENT_READ_TIMEOUT_MS", 10_000, 1_000, 60_000, issues),
    totalTimeoutMs: parseInteger(env.NUTSNEWS_ENRICHMENT_TOTAL_TIMEOUT_MS, "NUTSNEWS_ENRICHMENT_TOTAL_TIMEOUT_MS", 30_000, 1_000, 120_000, issues),
    maxResponseBytes: parseInteger(env.NUTSNEWS_ENRICHMENT_MAX_RESPONSE_BYTES, "NUTSNEWS_ENRICHMENT_MAX_RESPONSE_BYTES", 1_048_576, 16_384, 16_777_216, issues),
    maxRedirects: parseInteger(env.NUTSNEWS_ENRICHMENT_MAX_REDIRECTS, "NUTSNEWS_ENRICHMENT_MAX_REDIRECTS", 3, 0, 10, issues)
  };
  const config: EnrichmentConfig = {
    serviceName: ENRICHMENT_SERVICE_NAME,
    serviceVersion: ENRICHMENT_SERVICE_VERSION,
    environment: nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local"),
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    http: {
      host: nonEmpty(env.NUTSNEWS_ENRICHMENT_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_ENRICHMENT_HTTP_PORT, "NUTSNEWS_ENRICHMENT_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    dependencies,
    concurrency,
    prefetch,
    fetch,
    shutdownTimeoutMs: parseInteger(env.NUTSNEWS_ENRICHMENT_SHUTDOWN_TIMEOUT_MS, "NUTSNEWS_ENRICHMENT_SHUTDOWN_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    shadowMode: parseBoolean(env.NUTSNEWS_ENRICHMENT_SHADOW_MODE, "NUTSNEWS_ENRICHMENT_SHADOW_MODE", true, issues),
    telemetryLogs: parseTelemetryLogMode(env.NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS, issues),
    metricsEnabled: parseBoolean(env.NUTSNEWS_ENRICHMENT_METRICS_ENABLED, "NUTSNEWS_ENRICHMENT_METRICS_ENABLED", true, issues)
  };

  if (config.prefetch < config.concurrency) {
    issues.push("NUTSNEWS_ENRICHMENT_PREFETCH must be greater than or equal to NUTSNEWS_ENRICHMENT_CONCURRENCY.");
  }

  if (config.fetch.totalTimeoutMs < config.fetch.connectTimeoutMs || config.fetch.totalTimeoutMs < config.fetch.readTimeoutMs) {
    issues.push("NUTSNEWS_ENRICHMENT_TOTAL_TIMEOUT_MS must be greater than or equal to connect and read timeouts.");
  }

  if (!config.shadowMode) {
    issues.push("NUTSNEWS_ENRICHMENT_SHADOW_MODE must remain true until backend-owned deployment enables cutover.");
  }

  if (issues.length > 0) {
    throw new EnrichmentConfigError(issues);
  }

  return config;
}

function variable(
  name: string,
  description: string,
  requiredInProduction: boolean,
  sensitive: boolean,
  defaultValue?: string
): EnrichmentConfigVariable {
  return {
    name,
    description,
    requiredInProduction,
    sensitive,
    ...(defaultValue === undefined ? {} : {
      defaultValue
    })
  };
}

function nonEmpty(value: string | undefined, fallback: string): string {
  if (value === undefined) {
    return fallback;
  }

  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : fallback;
}

function hasValue(value: string | undefined): boolean {
  return value !== undefined && value.trim().length > 0;
}

function parseDependencyMode(value: string | undefined, issues: string[]): EnrichmentDependencyMode {
  const normalized = nonEmpty(value, "test");

  if (normalized === "test" || normalized === "production") {
    return normalized;
  }

  issues.push("NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE must be test or production.");
  return "test";
}

function parseTelemetryLogMode(value: string | undefined, issues: string[]): EnrichmentTelemetryLogMode {
  const normalized = nonEmpty(value, "stdout");

  if (normalized === "stdout" || normalized === "silent") {
    return normalized;
  }

  issues.push("NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS must be stdout or silent.");
  return "stdout";
}

function parseBoolean(
  value: string | undefined,
  key: string,
  fallback: boolean,
  issues: string[]
): boolean {
  if (!hasValue(value)) {
    return fallback;
  }

  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();

  if (normalized === "true" || normalized === "1") {
    return true;
  }

  if (normalized === "false" || normalized === "0") {
    return false;
  }

  issues.push(`${key} must be true or false.`);
  return fallback;
}

function parseInteger(
  value: string | undefined,
  key: string,
  fallback: number,
  min: number,
  max: number,
  issues: string[]
): number {
  if (!hasValue(value)) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    issues.push(`${key} must be an integer between ${String(min)} and ${String(max)}.`);
    return fallback;
  }

  return parsed;
}

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE=production.`);
  }
}
