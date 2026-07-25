import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock,
  RuntimeHandlerResult,
  RuntimeIdempotencyStore,
  RuntimeMessageContext
} from "@ramideltoro/nutsnews-worker-runtime";

export interface EnrichmentDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export interface EnrichmentStateStore extends RuntimeIdempotencyStore {
  readonly name: string;
  probe(): EnrichmentDependencyProbe | Promise<EnrichmentDependencyProbe>;
  findResultByFingerprint(
    canonicalArticleId: string,
    articleVersion: number,
    contentFingerprint: string,
    transaction: EnrichmentDatabaseTransaction
  ): Promise<EnrichmentStoredResult | undefined>;
  recordResult(result: EnrichmentStoredResult, transaction: EnrichmentDatabaseTransaction): Promise<EnrichmentStoredResult>;
}

export interface EnrichmentDatabaseTransaction {
  readonly transactionId: string;
}

export interface EnrichmentDatabaseTransactionRunner {
  readonly name: string;
  probe(): EnrichmentDependencyProbe | Promise<EnrichmentDependencyProbe>;
  withTransaction<T>(operation: (transaction: EnrichmentDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface EnrichmentBrokerOutbox {
  readonly name: string;
  probe(): EnrichmentDependencyProbe | Promise<EnrichmentDependencyProbe>;
  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
}

export interface EnrichmentHttpFetchRequest {
  readonly url: string;
  readonly connectTimeoutMs: number;
  readonly readTimeoutMs: number;
  readonly totalTimeoutMs: number;
  readonly maxResponseBytes: number;
  readonly maxDecompressedBytes: number;
  readonly maxDecompressionRatio: number;
  readonly maxRedirects: number;
  readonly maxConcurrentSockets: number;
  readonly perHostConcurrency: number;
  readonly enforceRedirectDnsPolicy: true;
}

export interface EnrichmentHttpFetchResponse {
  readonly finalUrl: string;
  readonly statusCode: number;
  readonly headers: Readonly<Record<string, string>>;
  readonly bodyBytes: number;
  readonly compressedBytes?: number;
  readonly decompressedBytes?: number;
  readonly encodingValid?: boolean;
  readonly redirects?: readonly {
    readonly url: string;
  }[];
  readonly bodyRef: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: string;
  };
}

export interface EnrichmentHttpClient {
  readonly name: string;
  probe(): EnrichmentDependencyProbe | Promise<EnrichmentDependencyProbe>;
  fetch(request: EnrichmentHttpFetchRequest): Promise<EnrichmentHttpFetchResponse>;
}

export interface EnrichmentDnsPolicyDecision {
  readonly allowed: boolean;
  readonly reason: "allowed" | "unsupported-scheme" | "private-address" | "loopback-address" | "link-local-address" | "metadata-address" | "dns-error";
}

export interface EnrichmentDnsPolicy {
  readonly name: string;
  probe(): EnrichmentDependencyProbe | Promise<EnrichmentDependencyProbe>;
  checkUrl(url: string): Promise<EnrichmentDnsPolicyDecision>;
}

export interface EnrichmentHtmlParseInput {
  readonly canonicalArticleId: string;
  readonly finalUrl: string;
  readonly timeoutMs: number;
  readonly maxDomNodes: number;
  readonly htmlRef: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: string;
  };
}

export interface EnrichmentImageCandidate {
  readonly url: string;
  readonly source: "open_graph" | "twitter" | "json_ld" | "rss" | "srcset" | "html";
  readonly width?: number;
  readonly height?: number;
}

export interface EnrichmentParsedMetadata {
  readonly canonicalUrl?: string;
  readonly title?: string;
  readonly description?: string;
  readonly publishedAt?: string;
  readonly language?: string;
  readonly imageCandidates: readonly EnrichmentImageCandidate[];
}

export interface EnrichmentStoredResult {
  readonly requestId: string;
  readonly canonicalArticleId: string;
  readonly articleVersion: number;
  readonly candidateId: string;
  readonly canonicalUrl: string;
  readonly finalUrl: string;
  readonly contentFingerprint: string;
  readonly imageStatus: "hydrated" | "no_thumbnail" | "transient_failure";
  readonly imageUrl?: string;
  readonly outcome: "image-found" | "partial" | "skipped" | "failed";
  readonly metadataRef: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: "application/json";
    readonly contentFingerprint: string;
    readonly canonicalArticleId: string;
    readonly articleVersion: number;
    readonly title?: string;
    readonly description?: string;
    readonly publishedAt?: string;
    readonly language?: string;
    readonly failureReason?: string;
  };
  readonly recordedAt: string;
}

export interface EnrichmentHtmlParser {
  readonly name: string;
  probe(): EnrichmentDependencyProbe | Promise<EnrichmentDependencyProbe>;
  parse(input: EnrichmentHtmlParseInput): Promise<EnrichmentParsedMetadata>;
}

export interface EnrichmentWorkTools {
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: EnrichmentDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface EnrichmentWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: EnrichmentWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface EnrichmentDependencies {
  readonly clock: RuntimeClock;
  readonly stateStore: EnrichmentStateStore;
  readonly transactionRunner: EnrichmentDatabaseTransactionRunner;
  readonly brokerOutbox: EnrichmentBrokerOutbox;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly httpClient: EnrichmentHttpClient;
  readonly dnsPolicy: EnrichmentDnsPolicy;
  readonly htmlParser: EnrichmentHtmlParser;
  readonly workHandler: EnrichmentWorkHandler;
}
