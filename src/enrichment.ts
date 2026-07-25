import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerPublishCommand,
  type RuntimeMessageContext,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { EnrichmentConfig } from "./config.js";
import type {
  EnrichmentDependencies,
  EnrichmentHttpFetchResponse,
  EnrichmentImageCandidate,
  EnrichmentParsedMetadata,
  EnrichmentStoredResult,
  EnrichmentWorkHandler,
  EnrichmentWorkTools
} from "./dependencies.js";
import {
  sha256Hex,
  stableUuid
} from "./ids.js";

export interface ArticleEnrichmentWorkHandlerOptions {
  readonly config: EnrichmentConfig;
  readonly dependencies: EnrichmentDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
}

interface EnrichmentRequest {
  readonly requestId: string;
  readonly canonicalArticleId: string;
  readonly articleVersion: number;
  readonly candidateId: string;
  readonly canonicalUrl: string;
  readonly reason: "new" | "changed";
}

interface EnrichmentProcessResult {
  readonly result: EnrichmentStoredResult;
  readonly reused: boolean;
}

interface RankedImageCandidate {
  readonly url: string;
  readonly score: number;
}

const ENRICHMENT_QUEUE = "nutsnews.worker.enrichment.v1";
const TRACKING_PARAMS = [
  "fbclid",
  "gclid",
  "igshid",
  "mc_cid",
  "mc_eid",
  "ref",
  "utm_campaign",
  "utm_content",
  "utm_medium",
  "utm_source",
  "utm_term"
] as const;

export function createArticleEnrichmentWorkHandler(options: ArticleEnrichmentWorkHandlerOptions): EnrichmentWorkHandler {
  return {
    name: "article-enrichment-work-handler",
    handle: (context, tools) => handleArticleEnrichment(context, tools, options)
  };
}

async function handleArticleEnrichment(
  context: RuntimeMessageContext,
  tools: EnrichmentWorkTools,
  options: ArticleEnrichmentWorkHandlerOptions
) {
  const request = enrichmentRequestFromContext(context);
  const processed = await processRequest(request, tools, options);
  const command = approvalPublishCommand(context, processed.result, options.config);
  const receipt = await tools.publish(command);

  await tools.recordOutbox(command, receipt);
  await emitEnrichmentTelemetry(options, processed);

  return {
    status: "ok"
  } as const;
}

async function processRequest(
  request: EnrichmentRequest,
  tools: EnrichmentWorkTools,
  options: ArticleEnrichmentWorkHandlerOptions
): Promise<EnrichmentProcessResult> {
  const checked = await options.dependencies.dnsPolicy.checkUrl(request.canonicalUrl);

  if (!checked.allowed) {
    const result = await recordResult(toFailureResult(request, request.canonicalUrl, `dns-policy:${checked.reason}`, "skipped", options), tools, options);

    return {
      result,
      reused: false
    };
  }

  let response: EnrichmentHttpFetchResponse;

  try {
    response = await options.dependencies.httpClient.fetch({
      url: request.canonicalUrl,
      connectTimeoutMs: options.config.fetch.connectTimeoutMs,
      readTimeoutMs: options.config.fetch.readTimeoutMs,
      totalTimeoutMs: options.config.fetch.totalTimeoutMs,
      maxResponseBytes: options.config.fetch.maxResponseBytes,
      maxRedirects: options.config.fetch.maxRedirects
    });
  } catch {
    const result = await recordResult(toFailureResult(request, request.canonicalUrl, "fetch-error", "failed", options), tools, options);

    return {
      result,
      reused: false
    };
  }

  const contentFingerprint = responseFingerprint(request, response);
  const cached = await findStoredResult(request, contentFingerprint, tools, options);

  if (cached !== undefined) {
    return {
      result: {
        ...cached,
        requestId: request.requestId,
        candidateId: request.candidateId,
        recordedAt: runtimeNow(options.dependencies.clock)
      },
      reused: true
    };
  }

  if (!isHtmlResponse(response)) {
    const result = await recordResult(toFailureResult(request, response.finalUrl, "unsupported-content-type", "skipped", options, contentFingerprint), tools, options);

    return {
      result,
      reused: false
    };
  }

  let parsed: EnrichmentParsedMetadata;

  try {
    parsed = await options.dependencies.htmlParser.parse({
      canonicalArticleId: request.canonicalArticleId,
      finalUrl: response.finalUrl,
      htmlRef: response.bodyRef
    });
  } catch {
    const result = await recordResult(toFailureResult(request, response.finalUrl, "parse-error", "failed", options, contentFingerprint), tools, options);

    return {
      result,
      reused: false
    };
  }

  const selectedImage = selectImage(parsed.imageCandidates, response.finalUrl);
  const canonicalUrl = safeUrl(parsed.canonicalUrl, response.finalUrl);
  const result = await recordResult({
    requestId: request.requestId,
    canonicalArticleId: request.canonicalArticleId,
    articleVersion: request.articleVersion,
    candidateId: request.candidateId,
    canonicalUrl,
    finalUrl: response.finalUrl,
    contentFingerprint,
    imageStatus: selectedImage === undefined ? "no_thumbnail" : "hydrated",
    ...(selectedImage === undefined ? {} : {
      imageUrl: selectedImage.url
    }),
    outcome: selectedImage === undefined ? "partial" : "image-found",
    metadataRef: metadataRef(request, contentFingerprint, metadataValues(parsed)),
    recordedAt: runtimeNow(options.dependencies.clock)
  }, tools, options);

  return {
    result,
    reused: false
  };
}

function enrichmentRequestFromContext(context: RuntimeMessageContext): EnrichmentRequest {
  return {
    requestId: stringValue(context.payload.requestId, "requestId"),
    canonicalArticleId: stringValue(context.payload.canonicalArticleId, "canonicalArticleId"),
    articleVersion: positiveIntegerValue(context.payload.articleVersion, "articleVersion"),
    candidateId: stringValue(context.payload.candidateId, "candidateId"),
    canonicalUrl: stringValue(context.payload.canonicalUrl, "canonicalUrl"),
    reason: context.payload.reason === "changed" ? "changed" : "new"
  };
}

async function findStoredResult(
  request: EnrichmentRequest,
  contentFingerprint: string,
  tools: EnrichmentWorkTools,
  options: ArticleEnrichmentWorkHandlerOptions
): Promise<EnrichmentStoredResult | undefined> {
  return tools.withTransaction((transaction) => options.dependencies.stateStore.findResultByFingerprint(
    request.canonicalArticleId,
    request.articleVersion,
    contentFingerprint,
    transaction
  ));
}

async function recordResult(
  result: EnrichmentStoredResult,
  tools: EnrichmentWorkTools,
  options: ArticleEnrichmentWorkHandlerOptions
): Promise<EnrichmentStoredResult> {
  return tools.withTransaction(async (transaction) => {
    const existing = await options.dependencies.stateStore.findResultByFingerprint(
      result.canonicalArticleId,
      result.articleVersion,
      result.contentFingerprint,
      transaction
    );

    if (existing !== undefined) {
      return existing;
    }

    return options.dependencies.stateStore.recordResult(result, transaction);
  });
}

function toFailureResult(
  request: EnrichmentRequest,
  finalUrl: string,
  reason: string,
  outcome: "skipped" | "failed",
  options: ArticleEnrichmentWorkHandlerOptions,
  contentFingerprint = sha256Hex([
    request.canonicalArticleId,
    String(request.articleVersion),
    finalUrl,
    reason
  ].join("\u001f"))
): EnrichmentStoredResult {
  return {
    requestId: request.requestId,
    canonicalArticleId: request.canonicalArticleId,
    articleVersion: request.articleVersion,
    candidateId: request.candidateId,
    canonicalUrl: request.canonicalUrl,
    finalUrl,
    contentFingerprint,
    imageStatus: "transient_failure",
    outcome,
    metadataRef: metadataRef(request, contentFingerprint, {
      failureReason: reason
    }),
    recordedAt: runtimeNow(options.dependencies.clock)
  };
}

function responseFingerprint(request: EnrichmentRequest, response: EnrichmentHttpFetchResponse): string {
  return sha256Hex([
    request.canonicalArticleId,
    String(request.articleVersion),
    response.finalUrl,
    String(response.statusCode),
    String(response.bodyBytes),
    response.bodyRef.uri,
    headerValue(response.headers, "etag") ?? "",
    headerValue(response.headers, "last-modified") ?? "",
    headerValue(response.headers, "content-type") ?? ""
  ].join("\u001f"));
}

function isHtmlResponse(response: EnrichmentHttpFetchResponse): boolean {
  const contentType = headerValue(response.headers, "content-type")?.toLowerCase() ?? "";

  return response.statusCode >= 200 && response.statusCode < 300 && (contentType.includes("text/html") || contentType.includes("application/xhtml+xml"));
}

function headerValue(headers: Readonly<Record<string, string>>, key: string): string | undefined {
  const expected = key.toLowerCase();

  for (const [header, value] of Object.entries(headers)) {
    if (header.toLowerCase() === expected) {
      return value;
    }
  }

  return undefined;
}

function selectImage(candidates: readonly EnrichmentImageCandidate[], baseUrl: string): RankedImageCandidate | undefined {
  const ranked = candidates
    .map((candidate) => rankImage(candidate, baseUrl))
    .filter((candidate): candidate is RankedImageCandidate => candidate !== undefined)
    .sort((left, right) => right.score - left.score || left.url.localeCompare(right.url));

  return ranked[0];
}

function rankImage(candidate: EnrichmentImageCandidate, baseUrl: string): RankedImageCandidate | undefined {
  const url = normalizeImageUrl(candidate.url, baseUrl);

  if (url === undefined || isTiny(candidate) || isGenericImage(url)) {
    return undefined;
  }

  const area = (candidate.width ?? 0) * (candidate.height ?? 0);
  const boundedAreaScore = Math.min(Math.floor(area / 10_000), 25);

  return {
    url,
    score: sourcePriority(candidate.source) + boundedAreaScore
  };
}

function normalizeImageUrl(value: string, baseUrl: string): string | undefined {
  try {
    const url = new URL(value, baseUrl);

    if (url.protocol !== "https:" && url.protocol !== "http:") {
      return undefined;
    }

    url.hash = "";

    for (const key of Array.from(url.searchParams.keys())) {
      if (isTrackingParam(key)) {
        url.searchParams.delete(key);
      }
    }

    url.searchParams.sort();

    return url.toString();
  } catch {
    return undefined;
  }
}

function isTrackingParam(key: string): boolean {
  const normalized = key.toLowerCase();

  return normalized.startsWith("utm_") || TRACKING_PARAMS.includes(normalized as (typeof TRACKING_PARAMS)[number]);
}

function isTiny(candidate: EnrichmentImageCandidate): boolean {
  return (candidate.width !== undefined && candidate.width < 200) || (candidate.height !== undefined && candidate.height < 120);
}

function isGenericImage(url: string): boolean {
  const normalized = url.toLowerCase();

  return /(?:favicon|apple-touch-icon|mstile|sprite|placeholder|blank|spacer|pixel|tracker|tracking)/u.test(normalized);
}

function sourcePriority(source: EnrichmentImageCandidate["source"]): number {
  switch (source) {
    case "open_graph":
      return 60;
    case "twitter":
      return 55;
    case "json_ld":
      return 50;
    case "srcset":
      return 45;
    case "rss":
      return 40;
    case "html":
      return 20;
  }
}

function metadataRef(
  request: EnrichmentRequest,
  contentFingerprint: string,
  values: {
    readonly title?: string;
    readonly description?: string;
    readonly publishedAt?: string;
    readonly language?: string;
    readonly failureReason?: string;
  }
): EnrichmentStoredResult["metadataRef"] {
  return {
    kind: "backend-record",
    uri: `backend://worker-uplift/enrichment/${encodeURIComponent(request.canonicalArticleId)}/${contentFingerprint}`,
    mediaType: "application/json",
    contentFingerprint,
    canonicalArticleId: request.canonicalArticleId,
    articleVersion: request.articleVersion,
    ...(values.title === undefined ? {} : {
      title: values.title
    }),
    ...(values.description === undefined ? {} : {
      description: values.description
    }),
    ...(values.publishedAt === undefined ? {} : {
      publishedAt: values.publishedAt
    }),
    ...(values.language === undefined ? {} : {
      language: values.language
    }),
    ...(values.failureReason === undefined ? {} : {
      failureReason: values.failureReason
    })
  };
}

function metadataValues(parsed: EnrichmentParsedMetadata): {
  readonly title?: string;
  readonly description?: string;
  readonly publishedAt?: string;
  readonly language?: string;
} {
  return {
    ...(parsed.title === undefined ? {} : {
      title: parsed.title
    }),
    ...(parsed.description === undefined ? {} : {
      description: parsed.description
    }),
    ...(parsed.publishedAt === undefined ? {} : {
      publishedAt: parsed.publishedAt
    }),
    ...(parsed.language === undefined ? {} : {
      language: parsed.language
    })
  };
}

function approvalPublishCommand(
  context: RuntimeMessageContext,
  result: EnrichmentStoredResult,
  config: EnrichmentConfig
): BrokerPublishCommand {
  const route = getWorkerRoute("approval");
  const idempotencyKey = `enrichment:approval:${result.requestId}:${result.contentFingerprint.slice(0, 16)}`;
  const payload = approvalPayload(context, result, idempotencyKey);
  const validation = validateStagePayload(payload);

  if (!validation.ok) {
    throw new Error(`Invalid enrichment result payload: ${validation.issues.map((issue) => `${issue.path}:${issue.code}`).join(", ")}`);
  }

  return {
    envelope: assertWorkerEnvelope({
      schemaId: route.schemaId,
      schemaVersion: 1,
      route: "approval",
      messageId: stableUuid([
        "approval-message",
        idempotencyKey
      ]),
      causationId: context.envelope.messageId,
      correlationId: context.envelope.correlationId,
      traceparent: context.envelope.traceparent,
      ...(context.envelope.tracestate === undefined ? {} : {
        tracestate: context.envelope.tracestate
      }),
      idempotencyKey,
      aggregate: {
        type: "article",
        id: result.canonicalArticleId,
        version: result.articleVersion
      },
      occurredAt: result.recordedAt,
      attempt: {
        count: 1,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: result.recordedAt
      },
      producer: {
        name: config.serviceName,
        version: config.serviceVersion
      },
      payloadRef: {
        kind: "backend-record",
        uri: result.metadataRef.uri,
        mediaType: "application/json",
        sizeBytes: getStagePayloadSizeBytes(payload)
      }
    }),
    payload
  };
}

function approvalPayload(
  context: RuntimeMessageContext,
  result: EnrichmentStoredResult,
  idempotencyKey: string
): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.enrichmentResult,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: stringValue(context.payload.pipelineRunId, "pipelineRunId"),
    stageExecutionId: stableUuid([
      "approval-stage-execution",
      idempotencyKey
    ]),
    sourceMessageId: context.envelope.messageId,
    idempotencyKey,
    traceparent: context.envelope.traceparent,
    ...(context.envelope.tracestate === undefined ? {} : {
      tracestate: context.envelope.tracestate
    }),
    producedAt: result.recordedAt,
    candidateId: result.candidateId,
    canonicalUrl: result.canonicalUrl,
    imageStatus: result.imageStatus,
    ...(result.imageUrl === undefined ? {} : {
      imageUrl: result.imageUrl
    }),
    articleMetadataRef: result.metadataRef
  };
}

async function emitEnrichmentTelemetry(
  options: ArticleEnrichmentWorkHandlerOptions,
  processed: EnrichmentProcessResult
): Promise<void> {
  const result = processed.result;

  await emitRuntimeTelemetry(options.telemetry, {
    name: "runtime.dependency.observed",
    level: result.imageStatus === "transient_failure" ? "warn" : "info",
    at: runtimeNow(options.dependencies.clock),
    stage: "enrichment",
    queue: ENRICHMENT_QUEUE,
    outcome: result.imageStatus === "transient_failure" ? "failure" : "success",
    attributes: {
      event: "enrichment.article.processed",
      dependency: "article-enrichment",
      enrichmentOutcome: result.outcome,
      imageStatus: result.imageStatus,
      reusedResult: processed.reused,
      candidateId: result.candidateId,
      canonicalArticleId: result.canonicalArticleId,
      articleVersion: result.articleVersion
    }
  });
}

function safeUrl(value: string | undefined, fallback: string): string {
  try {
    const url = new URL(value ?? fallback, fallback);

    if (url.protocol === "http:" || url.protocol === "https:") {
      return url.toString();
    }
  } catch {
    return fallback;
  }

  return fallback;
}

function stringValue(value: unknown, key: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Enrichment payload is missing ${key}.`);
  }

  return value;
}

function positiveIntegerValue(value: unknown, key: string): number {
  if (!Number.isInteger(value) || typeof value !== "number" || value < 1) {
    throw new Error(`Enrichment payload is missing ${key}.`);
  }

  return value;
}
