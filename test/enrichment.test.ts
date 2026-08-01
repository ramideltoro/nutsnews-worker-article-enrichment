import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  getStagePayloadSchemaDefinition,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink,
  type BrokerPublishCommand
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { createArticleEnrichmentWorkHandler } from "../src/enrichment.js";
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
  createLocalEnrichmentDependencies,
  createMinimalEnrichmentDelivery
} from "../src/test-doubles.js";
import type { EnrichmentHttpFetchResponse } from "../src/dependencies.js";

describe("createArticleEnrichmentWorkHandler", () => {
  it("fetches, parses, ranks safe images, stores metadata, and publishes approval input", async () => {
    const context = createEnrichmentContext();
    const delivery = requestDelivery(1);

    context.htmlParser.parsed = {
      canonicalUrl: "https://articles.example.test/world/story-one",
      title: "Story One",
      description: "Bounded story description.",
      publishedAt: "2026-07-23T00:00:00.000Z",
      language: "en",
      imageCandidates: [
        {
          url: "/favicon.ico",
          source: "html",
          width: 64,
          height: 64
        },
        {
          url: "/images/pixel.gif",
          source: "html",
          width: 1,
          height: 1
        },
        {
          url: "/images/story.jpg?utm_source=feed&width=1200",
          source: "open_graph",
          width: 1200,
          height: 630
        },
        {
          url: "https://cdn.example.test/twitter.jpg",
          source: "twitter",
          width: 900,
          height: 500
        }
      ]
    };

    await context.service.start();

    try {
      await expect(context.broker.deliverEnrichment(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });

      expect(context.dnsPolicy.checkedUrls).toEqual([
        "https://articles.example.test/world/story-one"
      ]);
      expect(context.httpClient.requests[0]).toMatchObject({
        url: "https://articles.example.test/world/story-one",
        connectTimeoutMs: 5_000,
        readTimeoutMs: 10_000,
        totalTimeoutMs: 30_000,
        maxResponseBytes: 1_048_576,
        maxDecompressedBytes: 1_048_576,
        maxDecompressionRatio: 20,
        maxRedirects: 3,
        maxConcurrentSockets: 32,
        perHostConcurrency: 4,
        enforceRedirectDnsPolicy: true
      });
      expect(context.htmlParser.inputs[0]).toMatchObject({
        canonicalArticleId: "article-001",
        finalUrl: "https://articles.example.test/world/story-one",
        timeoutMs: 5_000,
        maxDomNodes: 50_000
      });
      expect(context.stateStore.results).toHaveLength(1);
      expect(context.stateStore.results[0]).toMatchObject({
        imageStatus: "hydrated",
        imageUrl: "https://articles.example.test/images/story.jpg?width=1200",
        outcome: "image-found"
      });

      const command = publishedCommand(context, 0);
      const schema = getStagePayloadSchemaDefinition(STAGE_PAYLOAD_SCHEMA_IDS.enrichmentResult);

      expect(command.payload).toMatchObject({
        schemaId: STAGE_PAYLOAD_SCHEMA_IDS.enrichmentResult,
        schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
        sourceMessageId: uuid(7_001),
        candidateId: "candidate-world-001",
        canonicalUrl: "https://articles.example.test/world/story-one",
        imageStatus: "hydrated",
        imageUrl: "https://articles.example.test/images/story.jpg?width=1200"
      });
      expect(command.envelope).toMatchObject({
        route: "approval",
        causationId: uuid(7_001),
        producer: {
          name: schema.producer,
          version: "0.1.0"
        },
        aggregate: {
          type: "article",
          id: "article-001",
          version: 1
        }
      });
      expect(schema.consumer).toBe(command.envelope.route);
      expect(context.outbox.records[0]?.command).toBe(command);
      expect(JSON.stringify(command.payload)).not.toContain("<html");
      expect(JSON.stringify(command.payload)).not.toContain("articleBody");
    } finally {
      await context.service.stop();
    }
  });

  it("reuses the recorded result when the response fingerprint is unchanged", async () => {
    const context = createEnrichmentContext();

    context.htmlParser.parsed = {
      title: "Reusable Story",
      imageCandidates: [
        {
          url: "https://images.example.test/reusable.jpg",
          source: "json_ld",
          width: 1000,
          height: 600
        }
      ]
    };

    await context.service.start();

    try {
      await context.broker.deliverEnrichment(requestDelivery(1, {
        requestId: "enrichment-req-reuse-a",
        candidateId: "candidate-reuse-a"
      }));
      await context.broker.deliverEnrichment(requestDelivery(2, {
        requestId: "enrichment-req-reuse-b",
        candidateId: "candidate-reuse-b"
      }));

      expect(context.httpClient.requests).toHaveLength(2);
      expect(context.htmlParser.inputs).toHaveLength(1);
      expect(context.stateStore.results).toHaveLength(1);
      expect(context.broker.published).toHaveLength(2);
      expect(context.broker.published[1]?.payload).toMatchObject({
        candidateId: "candidate-reuse-b",
        imageStatus: "hydrated",
        articleMetadataRef: context.stateStore.results[0]?.metadataRef
      });
    } finally {
      await context.service.stop();
    }
  });

  it("rejects mismatched envelope and payload idempotency keys before business side effects", async () => {
    const context = createEnrichmentContext();

    await context.service.start();

    try {
      await expect(context.broker.deliverEnrichment(requestDelivery(91, {
        idempotencyKey: "canonicalizer:enrichment:different-owner:91"
      }))).resolves.toMatchObject({
        action: "dlq",
        reason: "idempotency-key-mismatch"
      });
      expect(context.httpClient.requests).toHaveLength(0);
      expect(context.htmlParser.inputs).toHaveLength(0);
      expect(context.stateStore.results).toHaveLength(0);
      expect(context.transactionRunner.transactions).toHaveLength(0);
      expect(context.outbox.records).toHaveLength(0);
      expect(context.broker.published).toHaveLength(0);
    } finally {
      await context.service.stop();
    }
  });

  it.each([
    {
      name: "rss-only images",
      candidates: [
        {
          url: "https://images.example.test/rss.jpg",
          source: "rss" as const
        }
      ],
      status: "hydrated",
      imageUrl: "https://images.example.test/rss.jpg"
    },
    {
      name: "json-ld images",
      candidates: [
        {
          url: "https://images.example.test/jsonld.jpg",
          source: "json_ld" as const,
          width: 900,
          height: 500
        }
      ],
      status: "hydrated",
      imageUrl: "https://images.example.test/jsonld.jpg"
    },
    {
      name: "srcset relative URLs",
      candidates: [
        {
          url: "/media/srcset-1200.jpg",
          source: "srcset" as const,
          width: 1200,
          height: 675
        }
      ],
      status: "hydrated",
      imageUrl: "https://articles.example.test/media/srcset-1200.jpg"
    },
    {
      name: "no-image pages",
      candidates: [
        {
          url: "/placeholder.png",
          source: "html" as const,
          width: 640,
          height: 360
        }
      ],
      status: "no_thumbnail"
    }
  ])("covers legacy parity fixture: $name", async ({ candidates, imageUrl, status }) => {
    const context = createEnrichmentContext();

    context.htmlParser.parsed = {
      imageCandidates: candidates
    };

    await context.service.start();

    try {
      await context.broker.deliverEnrichment(requestDelivery(1));

      expect(context.broker.published[0]?.payload).toMatchObject({
        imageStatus: status,
        ...(imageUrl === undefined ? {} : {
          imageUrl
        })
      });
    } finally {
      await context.service.stop();
    }
  });

  it("records malformed HTML parser failures as bounded transient enrichment results", async () => {
    const context = createEnrichmentContext();

    context.htmlParser.error = new Error("malformed-html");

    await context.service.start();

    try {
      await context.broker.deliverEnrichment(requestDelivery(1));

      expect(context.stateStore.results[0]).toMatchObject({
        imageStatus: "transient_failure",
        outcome: "failed",
        metadataRef: {
          failureReason: "parse-error"
        }
      });
      expect(context.broker.published[0]?.payload).toMatchObject({
        imageStatus: "transient_failure"
      });
    } finally {
      await context.service.stop();
    }
  });

  it("skips unsafe DNS policy decisions without fetching or parsing", async () => {
    const context = createEnrichmentContext();

    context.dnsPolicy.decision = {
      allowed: false,
      reason: "private-address"
    };

    await context.service.start();

    try {
      await context.broker.deliverEnrichment(requestDelivery(1));

      expect(context.httpClient.requests).toHaveLength(0);
      expect(context.htmlParser.inputs).toHaveLength(0);
      expect(context.stateStore.results[0]).toMatchObject({
        imageStatus: "transient_failure",
        outcome: "skipped",
        metadataRef: {
          failureReason: "dns-policy:private-address"
        }
      });
      expect(context.broker.published[0]?.payload).toMatchObject({
        imageStatus: "transient_failure"
      });
    } finally {
      await context.service.stop();
    }
  });

  it("checks every redirect URL with DNS policy before parsing", async () => {
    const context = createEnrichmentContext();

    context.httpClient.response = {
      finalUrl: "http://169.254.169.254/latest/meta-data",
      statusCode: 200,
      headers: {
        "content-type": "text/html"
      },
      bodyBytes: 128,
      compressedBytes: 128,
      decompressedBytes: 128,
      encodingValid: true,
      redirects: [
        {
          url: "https://redirector.example.test/story"
        },
        {
          url: "http://169.254.169.254/latest/meta-data"
        }
      ],
      bodyRef: {
        kind: "backend-record",
        uri: "backend://worker-uplift/enrichment/hostile/redirect",
        mediaType: "text/html"
      }
    };
    context.dnsPolicy.decisions.set("http://169.254.169.254/latest/meta-data", {
      allowed: false,
      reason: "metadata-address"
    });

    await context.service.start();

    try {
      await context.broker.deliverEnrichment(requestDelivery(1));

      expect(context.dnsPolicy.checkedUrls).toEqual([
        "https://articles.example.test/world/story-one",
        "https://redirector.example.test/story",
        "http://169.254.169.254/latest/meta-data"
      ]);
      expect(context.htmlParser.inputs).toHaveLength(0);
      expect(context.stateStore.results[0]).toMatchObject({
        imageStatus: "transient_failure",
        outcome: "skipped",
        metadataRef: {
          failureReason: "redirect-dns-policy:metadata-address"
        }
      });
    } finally {
      await context.service.stop();
    }
  });

  it.each([
    {
      name: "redirect loops",
      response: {
        redirects: [
          {
            url: "https://loop.example.test/1"
          },
          {
            url: "https://loop.example.test/2"
          },
          {
            url: "https://loop.example.test/3"
          },
          {
            url: "https://loop.example.test/4"
          }
        ]
      },
      reason: "redirect-limit-exceeded"
    },
    {
      name: "decompression bombs",
      response: {
        bodyBytes: 256,
        compressedBytes: 100,
        decompressedBytes: 50_000
      },
      reason: "decompression-ratio-exceeded"
    },
    {
      name: "slow or huge bodies",
      response: {
        bodyBytes: 2_000_000,
        compressedBytes: 2_000_000,
        decompressedBytes: 2_000_000
      },
      reason: "body-size-limit-exceeded"
    },
    {
      name: "invalid encodings",
      response: {
        encodingValid: false
      },
      reason: "invalid-encoding"
    }
  ])("contains hostile fixture: $name", async ({ reason, response }) => {
    const context = createEnrichmentContext();

    context.httpClient.response = hostileResponse(response);

    await context.service.start();

    try {
      await context.broker.deliverEnrichment(requestDelivery(1));

      expect(context.htmlParser.inputs).toHaveLength(0);
      expect(context.stateStore.results[0]).toMatchObject({
        imageStatus: "transient_failure",
        outcome: "failed",
        metadataRef: {
          failureReason: reason
        }
      });
      expect(context.broker.published[0]?.payload).toMatchObject({
        imageStatus: "transient_failure"
      });
    } finally {
      await context.service.stop();
    }
  });

  it("classifies retryable fetch failures for retry and DLQ exhaustion without publishing approval work", async () => {
    const retryContext = createEnrichmentContext();

    retryContext.httpClient.error = retryableError("tls-temporary", 1_000);
    await retryContext.service.start();

    try {
      await expect(retryContext.broker.deliverEnrichment(requestDelivery(1))).resolves.toMatchObject({
        action: "retry",
        reason: "tls-temporary",
        retryAfterMs: 1_000
      });
      expect(retryContext.broker.published).toHaveLength(0);
      expect(retryContext.stateStore.results).toHaveLength(0);
    } finally {
      await retryContext.service.stop();
    }

    const exhaustedContext = createEnrichmentContext();

    exhaustedContext.httpClient.error = retryableError("tls-temporary");
    await exhaustedContext.service.start();

    try {
      await expect(exhaustedContext.broker.deliverEnrichment(requestDelivery(1, {}, {
        attempt: {
          count: 4,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z"
        }
      }))).resolves.toMatchObject({
        action: "dlq",
        reason: "tls-temporary"
      });
      expect(exhaustedContext.broker.published).toHaveLength(0);
      expect(exhaustedContext.stateStore.results).toHaveLength(0);
    } finally {
      await exhaustedContext.service.stop();
    }
  });

  it("classifies retryable parser timeouts without recording or publishing", async () => {
    const context = createEnrichmentContext();

    context.htmlParser.error = retryableError("parser-timeout", 500);

    await context.service.start();

    try {
      await expect(context.broker.deliverEnrichment(requestDelivery(1))).resolves.toMatchObject({
        action: "retry",
        reason: "parser-timeout",
        retryAfterMs: 500
      });
      expect(context.stateStore.results).toHaveLength(0);
      expect(context.broker.published).toHaveLength(0);
    } finally {
      await context.service.stop();
    }
  });

  it("acks replay without duplicating persisted result or downstream approval work", async () => {
    const context = createEnrichmentContext();
    const delivery = requestDelivery(1);

    context.htmlParser.parsed = {
      imageCandidates: [
        {
          url: "https://images.example.test/replay.jpg",
          source: "open_graph",
          width: 1000,
          height: 600
        }
      ]
    };

    await context.service.start();

    try {
      await expect(context.broker.deliverEnrichment(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      await expect(context.broker.deliverEnrichment(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "duplicate"
      });
      expect(context.stateStore.results).toHaveLength(1);
      expect(context.broker.published).toHaveLength(1);
      expect(context.outbox.records).toHaveLength(1);
    } finally {
      await context.service.stop();
    }
  });
});

function createEnrichmentContext() {
  const config = loadEnrichmentConfig({
    NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
    NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
  });
  const baseDependencies = createLocalEnrichmentDependencies();
  const telemetry = createBufferedRuntimeTelemetrySink();
  const dependencies = {
    ...baseDependencies,
    workHandler: createArticleEnrichmentWorkHandler({
      config,
      dependencies: baseDependencies,
      telemetry
    })
  };
  const service = createEnrichmentService({
    config,
    dependencies,
    telemetry
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    dnsPolicy: dependencies.dnsPolicy as LocalEnrichmentDnsPolicy,
    htmlParser: dependencies.htmlParser as LocalEnrichmentHtmlParser,
    httpClient: dependencies.httpClient as LocalEnrichmentHttpClient,
    outbox: dependencies.brokerOutbox as LocalEnrichmentBrokerOutbox,
    service,
    stateStore: dependencies.stateStore as InMemoryEnrichmentStateStore,
    telemetry,
    transactionRunner: dependencies.transactionRunner as LocalEnrichmentTransactionRunner
  };
}

function requestDelivery(
  sequence: number,
  payloadOverrides: Readonly<Record<string, unknown>> = {},
  envelopeOverrides: Partial<WorkerMessageEnvelope> = {}
) {
  const requestId = typeof payloadOverrides.requestId === "string" ? payloadOverrides.requestId : `enrichment-req-${String(sequence).padStart(3, "0")}`;
  const candidateId = typeof payloadOverrides.candidateId === "string" ? payloadOverrides.candidateId : "candidate-world-001";
  const idempotencyKey = `canonicalizer:enrichment:${requestId}:${String(sequence)}`;

  return createMinimalEnrichmentDelivery({
    envelope: {
      messageId: uuid(7_000 + sequence),
      causationId: uuid(8_000 + sequence),
      correlationId: uuid(9_000 + sequence),
      idempotencyKey,
      aggregate: {
        type: "article",
        id: "article-001",
        version: 1
      },
      ...envelopeOverrides
    },
    payload: {
      requestId,
      candidateId,
      stageExecutionId: uuid(10_000 + sequence),
      sourceMessageId: uuid(11_000 + sequence),
      idempotencyKey,
      ...payloadOverrides
    }
  });
}

function hostileResponse(overrides: Partial<EnrichmentHttpFetchResponse>): EnrichmentHttpFetchResponse {
  return {
    ...baseHostileResponse(),
    ...overrides
  };
}

function baseHostileResponse(): EnrichmentHttpFetchResponse {
  return {
    finalUrl: "https://articles.example.test/world/story-one",
    statusCode: 200,
    headers: {
      "content-type": "text/html"
    },
    bodyBytes: 512,
    compressedBytes: 512,
    decompressedBytes: 512,
    encodingValid: true,
    redirects: [],
    bodyRef: {
      kind: "backend-record" as const,
      uri: "backend://worker-uplift/enrichment/hostile/body",
      mediaType: "text/html"
    }
  };
}

function retryableError(reason: string, retryAfterMs?: number): Error & { retryable: true; reason: string; retryAfterMs?: number } {
  const error = new Error(reason) as Error & { retryable: true; reason: string; retryAfterMs?: number };

  error.retryable = true;
  error.reason = reason;

  if (retryAfterMs !== undefined) {
    error.retryAfterMs = retryAfterMs;
  }

  return error;
}

function publishedCommand(context: ReturnType<typeof createEnrichmentContext>, index: number): BrokerPublishCommand {
  const command = context.broker.published[index];

  if (command === undefined) {
    throw new Error(`Expected published command at index ${String(index)}.`);
  }

  return command;
}

function uuid(counter: number): string {
  return `018f1598-2dd5-7c4f-9f92-${counter.toString(16).padStart(12, "0")}`;
}
