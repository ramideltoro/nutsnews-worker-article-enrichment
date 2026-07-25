import {
  describe,
  expect,
  it
} from "vitest";

import {
  EnrichmentConfigError,
  loadEnrichmentConfig
} from "../src/config.js";

describe("loadEnrichmentConfig", () => {
  it("loads local test defaults without secret values", () => {
    const config = loadEnrichmentConfig({
      HOSTNAME: "enrichment-host"
    });

    expect(config).toMatchObject({
      serviceName: "nutsnews-worker-article-enrichment",
      dependencyMode: "test",
      host: "enrichment-host",
      concurrency: 6,
      prefetch: 12,
      fetch: {
        connectTimeoutMs: 5_000,
        readTimeoutMs: 10_000,
        totalTimeoutMs: 30_000,
        maxResponseBytes: 1_048_576,
        maxRedirects: 3
      },
      shadowMode: true,
      dependencies: {
        databaseConfigured: false,
        rabbitmqConfigured: false
      }
    });
  });

  it("fails production config by missing secret names only", () => {
    expect(() => loadEnrichmentConfig({
      NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE: "production"
    })).toThrow(EnrichmentConfigError);

    try {
      loadEnrichmentConfig({
        NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE: "production"
      });
    } catch (error: unknown) {
      expect(error).toBeInstanceOf(EnrichmentConfigError);
      const configError = error as EnrichmentConfigError;

      expect(configError.issues).toEqual([
        "NUTSNEWS_ENRICHMENT_DATABASE_URL is required when NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE=production.",
        "NUTSNEWS_ENRICHMENT_RABBITMQ_URL is required when NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE=production."
      ]);
      expect(configError.message).not.toContain("postgres://");
      expect(configError.message).not.toContain("amqp://");
    }
  });

  it("rejects unsafe bounds and shadow cutover in this repo", () => {
    expect(() => loadEnrichmentConfig({
      NUTSNEWS_ENRICHMENT_CONCURRENCY: "12",
      NUTSNEWS_ENRICHMENT_PREFETCH: "4",
      NUTSNEWS_ENRICHMENT_CONNECT_TIMEOUT_MS: "9000",
      NUTSNEWS_ENRICHMENT_TOTAL_TIMEOUT_MS: "5000",
      NUTSNEWS_ENRICHMENT_MAX_RESPONSE_BYTES: "10",
      NUTSNEWS_ENRICHMENT_SHADOW_MODE: "false"
    })).toThrow(EnrichmentConfigError);
  });

  it("accepts explicit production dependency presence without retaining values", () => {
    const config = loadEnrichmentConfig({
      NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE: "production",
      NUTSNEWS_ENRICHMENT_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_ENRICHMENT_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });

    expect(config.dependencies).toEqual({
      databaseConfigured: true,
      rabbitmqConfigured: true
    });
    expect(JSON.stringify(config)).not.toContain("postgres://example.invalid");
    expect(JSON.stringify(config)).not.toContain("amqp://example.invalid");
  });
});
