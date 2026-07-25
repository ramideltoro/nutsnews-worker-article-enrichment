import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createInMemoryIdempotencyStore,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport,
  type RuntimeClock,
  type RuntimeHandlerResult,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
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
  EnrichmentParsedMetadata,
  EnrichmentStateStore,
  EnrichmentWorkHandler,
  EnrichmentWorkTools
} from "./dependencies.js";

export class ManualEnrichmentClock implements RuntimeClock {
  private current: Date;

  constructor(initial = "2026-07-23T00:00:00.000Z") {
    this.current = new Date(initial);
  }

  now(): Date {
    return new Date(this.current.getTime());
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export class InMemoryEnrichmentStateStore implements EnrichmentStateStore {
  readonly name: string = "local-enrichment-state";
  status: EnrichmentDependencyProbe["status"] = "ok";
  private readonly store;

  constructor(clock: RuntimeClock = new ManualEnrichmentClock()) {
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): EnrichmentDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local enrichment state ready" : "local enrichment state degraded"
    };
  }

  claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    return this.store.claim(idempotencyKey, context);
  }

  markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    return this.store.markCompleted(idempotencyKey, completion);
  }

  markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    return this.store.markFailed(idempotencyKey, failure);
  }
}

export class LocalEnrichmentTransactionRunner implements EnrichmentDatabaseTransactionRunner {
  readonly name: string = "local-database-transactions";
  status: EnrichmentDependencyProbe["status"] = "ok";
  readonly transactions: EnrichmentDatabaseTransaction[] = [];

  probe(): EnrichmentDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local transaction runner ready" : "local transaction runner degraded"
    };
  }

  async withTransaction<T>(operation: (transaction: EnrichmentDatabaseTransaction) => Promise<T>): Promise<T> {
    const transaction = {
      transactionId: `local-transaction-${String(this.transactions.length + 1)}`
    };

    this.transactions.push(transaction);

    return operation(transaction);
  }
}

export class LocalEnrichmentBrokerOutbox implements EnrichmentBrokerOutbox {
  readonly name: string = "local-broker-outbox";
  status: EnrichmentDependencyProbe["status"] = "ok";
  readonly records: { readonly command: BrokerPublishCommand; readonly receipt: BrokerPublishReceipt }[] = [];

  probe(): EnrichmentDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local broker outbox ready" : "local broker outbox degraded"
    };
  }

  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    this.records.push({
      command,
      receipt
    });
    return Promise.resolve();
  }
}

export class LocalEnrichmentHttpClient implements EnrichmentHttpClient {
  readonly name: string = "local-http-client";
  status: EnrichmentDependencyProbe["status"] = "ok";
  readonly requests: EnrichmentHttpFetchRequest[] = [];
  response: EnrichmentHttpFetchResponse | undefined;

  probe(): EnrichmentDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local HTTP client ready" : "local HTTP client degraded"
    };
  }

  fetch(request: EnrichmentHttpFetchRequest): Promise<EnrichmentHttpFetchResponse> {
    this.requests.push(request);

    return Promise.resolve(this.response ?? {
      finalUrl: request.url,
      statusCode: 200,
      headers: {
        "content-type": "text/html; charset=utf-8"
      },
      bodyBytes: 0,
      bodyRef: {
        kind: "backend-record",
        uri: `backend://worker-uplift/enrichment/http/${encodeURIComponent(request.url)}`,
        mediaType: "text/html"
      }
    });
  }
}

export class LocalEnrichmentDnsPolicy implements EnrichmentDnsPolicy {
  readonly name: string = "local-dns-policy";
  status: EnrichmentDependencyProbe["status"] = "ok";
  readonly checkedUrls: string[] = [];
  decision: EnrichmentDnsPolicyDecision = {
    allowed: true,
    reason: "allowed"
  };

  probe(): EnrichmentDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local DNS policy ready" : "local DNS policy degraded"
    };
  }

  checkUrl(url: string): Promise<EnrichmentDnsPolicyDecision> {
    this.checkedUrls.push(url);
    return Promise.resolve(this.decision);
  }
}

export class LocalEnrichmentHtmlParser implements EnrichmentHtmlParser {
  readonly name: string = "local-html-parser";
  status: EnrichmentDependencyProbe["status"] = "ok";
  readonly inputs: EnrichmentHtmlParseInput[] = [];
  parsed: EnrichmentParsedMetadata = {
    imageCandidates: []
  };

  probe(): EnrichmentDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local HTML parser ready" : "local HTML parser degraded"
    };
  }

  parse(input: EnrichmentHtmlParseInput): Promise<EnrichmentParsedMetadata> {
    this.inputs.push(input);
    return Promise.resolve(this.parsed);
  }
}

export class LocalEnrichmentWorkHandler implements EnrichmentWorkHandler {
  readonly name: string = "local-enrichment-work-handler";
  readonly handled: RuntimeMessageContext[] = [];
  result: RuntimeHandlerResult = {
    status: "ok"
  };
  handleGate: Promise<void> | undefined;
  onHandleStart: (() => void) | undefined;

  async handle(context: RuntimeMessageContext, tools: EnrichmentWorkTools): Promise<RuntimeHandlerResult> {
    void tools;
    this.onHandleStart?.();
    await this.handleGate;
    this.handled.push(context);

    return this.result;
  }
}

export class LocalBrokerTransport implements RuntimeBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly published: BrokerPublishCommand[] = [];
  readonly assertedRoutes: WorkerRoute[] = [];
  private readonly consumers = new Map<WorkerStage, BrokerDeliveryHandler>();
  private deliveryCount = 0;
  private connected = false;
  private closed = false;

  get inFlightDeliveryCount(): number {
    return this.deliveryCount;
  }

  connect(): Promise<void> {
    this.connected = true;
    this.closed = false;
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.assertedRoutes.splice(0, this.assertedRoutes.length, ...routes);
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    if (!this.connected || this.closed) {
      throw new Error("Local broker transport is not connected.");
    }

    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    if (!this.connected || this.closed) {
      throw new Error("Local broker transport is not connected.");
    }

    this.consumers.set(stage, handler);

    return Promise.resolve({
      stage,
      cancel: () => {
        this.consumers.delete(stage);
        return Promise.resolve();
      }
    });
  }

  async deliver(stage: WorkerStage, delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
    const handler = this.consumers.get(stage);

    if (handler === undefined) {
      throw new Error(`No local consumer is registered for ${stage}.`);
    }

    this.deliveryCount += 1;

    try {
      return await handler(delivery);
    } finally {
      this.deliveryCount = Math.max(0, this.deliveryCount - 1);
    }
  }

  deliverEnrichment(delivery: RuntimeMessageDelivery = createMinimalEnrichmentDelivery()): Promise<RuntimeMessageProcessingResult> {
    return this.deliver("enrichment", delivery);
  }

  drain(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closed = true;
    this.connected = false;
    this.consumers.clear();
    return Promise.resolve();
  }
}

export interface LocalEnrichmentDependencyOptions {
  readonly clock?: RuntimeClock;
  readonly stateStore?: EnrichmentStateStore;
  readonly transactionRunner?: EnrichmentDatabaseTransactionRunner;
  readonly brokerOutbox?: EnrichmentBrokerOutbox;
  readonly brokerTransport?: RuntimeBrokerTransport;
  readonly httpClient?: EnrichmentHttpClient;
  readonly dnsPolicy?: EnrichmentDnsPolicy;
  readonly htmlParser?: EnrichmentHtmlParser;
  readonly workHandler?: EnrichmentWorkHandler;
}

export function createLocalEnrichmentDependencies(options: LocalEnrichmentDependencyOptions = {}): EnrichmentDependencies {
  const clock = options.clock ?? new ManualEnrichmentClock();

  return {
    clock,
    stateStore: options.stateStore ?? new InMemoryEnrichmentStateStore(clock),
    transactionRunner: options.transactionRunner ?? new LocalEnrichmentTransactionRunner(),
    brokerOutbox: options.brokerOutbox ?? new LocalEnrichmentBrokerOutbox(),
    brokerTransport: options.brokerTransport ?? new LocalBrokerTransport(),
    httpClient: options.httpClient ?? new LocalEnrichmentHttpClient(),
    dnsPolicy: options.dnsPolicy ?? new LocalEnrichmentDnsPolicy(),
    htmlParser: options.htmlParser ?? new LocalEnrichmentHtmlParser(),
    workHandler: options.workHandler ?? new LocalEnrichmentWorkHandler()
  };
}

export function createMinimalEnrichmentPayload(overrides: Readonly<Record<string, unknown>> = {}): Readonly<Record<string, unknown>> {
  const now = "2026-07-23T00:00:00.000Z";

  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.enrichmentResult,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4703",
    idempotencyKey: "canonicalizer:enrichment:candidate-world-001",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: now,
    candidateId: "candidate-world-001",
    canonicalUrl: "https://articles.example.test/world/story-one",
    imageStatus: "no_thumbnail",
    articleMetadataRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/enrichment/candidate-world-001/metadata",
      mediaType: "application/json"
    },
    ...overrides
  };
}

export function createMinimalEnrichmentEnvelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  const route = getWorkerRoute("enrichment");
  const now = "2026-07-23T00:00:00.000Z";

  return assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "enrichment",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4720",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4710",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4710",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "canonicalizer:enrichment:candidate-world-001",
    aggregate: {
      type: "candidate",
      id: "candidate-world-001",
      version: 1
    },
    occurredAt: now,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: now
    },
    producer: {
      name: "canonicalizer",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/canonicalizer/candidate-world-001",
      mediaType: "application/json",
      sizeBytes: 512
    },
    ...overrides
  });
}

export function createMinimalEnrichmentDelivery(
  overrides: {
    readonly envelope?: Partial<WorkerMessageEnvelope>;
    readonly payload?: Readonly<Record<string, unknown>>;
  } = {}
): RuntimeMessageDelivery {
  return {
    envelope: createMinimalEnrichmentEnvelope(overrides.envelope ?? {}),
    payload: createMinimalEnrichmentPayload(overrides.payload),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}
