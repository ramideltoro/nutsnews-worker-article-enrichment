import {
  WORKER_DELIVERY_BEHAVIOR,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimReleaseResult,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageDelivery
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadEnrichmentConfig } from "../src/config.js";
import { createEnrichmentService } from "../src/service.js";
import {
  ENRICHMENT_IDEMPOTENCY_CLAIM_LEASE_MS,
  InMemoryEnrichmentStateStore,
  LocalBrokerTransport,
  LocalEnrichmentWorkHandler,
  ManualEnrichmentClock,
  createLocalEnrichmentDependencies,
  createMinimalEnrichmentEnvelope,
  createMinimalEnrichmentPayload
} from "../src/test-doubles.js";

interface ValidatedEnrichmentDelivery extends RuntimeMessageDelivery {
  readonly envelope: WorkerMessageEnvelope;
}

describe("Runtime 1 idempotency conformance", () => {
  it("caps ownership at five minutes and atomically reclaims an expired claim with a fresh token", async () => {
    const clock = new ManualEnrichmentClock();
    const store = new InMemoryEnrichmentStateStore(clock);
    const firstDelivery = delivery(1);
    const secondDelivery = delivery(2);
    const first = await store.claim(firstDelivery.envelope.idempotencyKey, claimContext(firstDelivery));

    expect(store.claimLeaseMs).toBe(ENRICHMENT_IDEMPOTENCY_CLAIM_LEASE_MS);
    expect(() => new InMemoryEnrichmentStateStore(clock, ENRICHMENT_IDEMPOTENCY_CLAIM_LEASE_MS + 1)).toThrow(RangeError);
    expect(first).toMatchObject({
      status: "claimed",
      replay: false
    });
    await expect(store.claim(secondDelivery.envelope.idempotencyKey, claimContext(secondDelivery))).resolves.toMatchObject({
      status: "in-progress"
    });

    clock.advance(ENRICHMENT_IDEMPOTENCY_CLAIM_LEASE_MS - 1);
    await expect(store.claim(secondDelivery.envelope.idempotencyKey, claimContext(secondDelivery))).resolves.toMatchObject({
      status: "in-progress"
    });

    clock.advance(1);
    const expiredToken = claimToken(first);

    await expect(store.markCompleted(firstDelivery.envelope.idempotencyKey, completion(firstDelivery, expiredToken))).rejects.toThrow(/another delivery/u);
    await expect(store.markFailed(firstDelivery.envelope.idempotencyKey, failure(firstDelivery, expiredToken))).rejects.toThrow(/another delivery/u);
    await expect(store.releaseClaim(firstDelivery.envelope.idempotencyKey, failure(firstDelivery, expiredToken))).resolves.toEqual({
      status: "not-owned"
    });
    const reclaimed = await store.claim(secondDelivery.envelope.idempotencyKey, claimContext(secondDelivery));

    expect(reclaimed).toMatchObject({
      status: "claimed",
      replay: true
    });
    expect(claimToken(reclaimed)).not.toBe(claimToken(first));
  });

  it("rejects stale completion and failure tokens, refuses stale release, and preserves completion", async () => {
    const clock = new ManualEnrichmentClock();
    const store = new InMemoryEnrichmentStateStore(clock, 100);
    const firstDelivery = delivery(1);
    const secondDelivery = delivery(2);
    const key = firstDelivery.envelope.idempotencyKey;
    const first = await store.claim(key, claimContext(firstDelivery));

    clock.advance(100);
    const second = await store.claim(key, claimContext(secondDelivery));
    const firstToken = claimToken(first);
    const secondToken = claimToken(second);

    await expect(store.markCompleted(key, completion(firstDelivery, firstToken))).rejects.toThrow(/another delivery/u);
    await expect(store.markFailed(key, failure(firstDelivery, firstToken))).rejects.toThrow(/another delivery/u);
    await expect(store.releaseClaim(key, failure(firstDelivery, firstToken))).resolves.toEqual({
      status: "not-owned"
    });
    await expect(store.claim(key, claimContext(secondDelivery))).resolves.toMatchObject({
      status: "in-progress"
    });

    await store.markCompleted(key, completion(secondDelivery, secondToken));
    await expect(store.releaseClaim(key, failure(firstDelivery, firstToken))).resolves.toEqual({
      status: "preserved-completed"
    });
    await expect(store.releaseClaim(key, failure(secondDelivery, secondToken))).resolves.toEqual({
      status: "preserved-completed"
    });
    await expect(store.markFailed(key, failure(secondDelivery, secondToken))).rejects.toThrow(/another delivery/u);
    const duplicate = await store.claim(key, claimContext(firstDelivery));

    expect(duplicate.status).toBe("already-completed");
    expect(duplicate.status === "already-completed" ? duplicate.completion?.claimToken : undefined).toBe(secondToken);
  });

  it("does not release an ambiguous claim and recovers only after the bounded lease", async () => {
    const clock = new ManualEnrichmentClock();
    const store = new ClaimCommitThenRejectStore(clock, 100);
    const context = createConformanceContext(store, clock);
    const message = delivery(1);

    await context.service.start();

    try {
      await expect(context.broker.deliverEnrichment(message)).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-claim-error"
      });
      await expect(context.broker.deliverEnrichment(message)).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-in-progress"
      });
      expect(store.releaseFailures).toHaveLength(0);
      expect(context.workHandler.handled).toHaveLength(0);

      clock.advance(100);
      await expect(context.broker.deliverEnrichment(message)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      expect(store.claimedTokens).toHaveLength(2);
      expect(store.claimedTokens[1]).not.toBe(store.claimedTokens[0]);
      expect(context.workHandler.handled).toHaveLength(1);
      expect(terminalEvents(context.telemetry.events)).toEqual([
        "runtime.message.retry",
        "runtime.message.retry",
        "runtime.message.accepted"
      ]);
    } finally {
      await context.service.stop();
    }
  });

  it("conditionally releases a completion rejected before commit and reacquires with a new token", async () => {
    const clock = new ManualEnrichmentClock();
    const store = new CompletionRejectBeforeCommitStore(clock);
    const context = createConformanceContext(store, clock);
    const message = delivery(1);

    await context.service.start();

    try {
      await expect(context.broker.deliverEnrichment(message)).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-completion-error"
      });
      expect(store.releaseFailures).toHaveLength(1);
      expect(store.releaseFailures[0]?.claimToken).toBe(store.claimedTokens[0]);

      await expect(context.broker.deliverEnrichment(message)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      expect(store.claimedTokens).toHaveLength(2);
      expect(store.claimedTokens[1]).not.toBe(store.claimedTokens[0]);
      expect(context.workHandler.handled).toHaveLength(2);
      expect(terminalEvents(context.telemetry.events)).toEqual([
        "runtime.message.retry",
        "runtime.message.accepted"
      ]);
    } finally {
      await context.service.stop();
    }
  });

  it("acknowledges final-attempt completion committed before its response rejects", async () => {
    const clock = new ManualEnrichmentClock();
    const store = new CompletionCommitThenRejectStore(clock);
    const context = createConformanceContext(store, clock);
    const message = finalAttemptDelivery();

    await context.service.start();

    try {
      await expect(context.broker.deliverEnrichment(message)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      expect(store.releaseResults).toEqual([
        {
          status: "preserved-completed"
        }
      ]);
      expect(store.releaseFailures[0]?.claimToken).toBe(store.claimedTokens[0]);
      expect(context.workHandler.handled).toHaveLength(1);

      await expect(context.broker.deliverEnrichment(message)).resolves.toMatchObject({
        action: "ack",
        reason: "duplicate"
      });
      expect(context.workHandler.handled).toHaveLength(1);
      expect(terminalEvents(context.telemetry.events)).toEqual([
        "runtime.message.accepted",
        "runtime.message.duplicate"
      ]);
    } finally {
      await context.service.stop();
    }
  });

  it("cancels a production consumer when completion and conditional release storage fail", async () => {
    const clock = new ManualEnrichmentClock();
    const store = new CompletionAndReleaseRejectingStore(clock);
    const config = loadEnrichmentConfig({
      HOSTNAME: "enrichment-runtime1-production-test",
      NUTSNEWS_ENVIRONMENT: "production",
      NUTSNEWS_ENRICHMENT_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_ENRICHMENT_DATABASE_URL: "postgres://example.invalid/enrichment",
      NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE: "production",
      NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
      NUTSNEWS_ENRICHMENT_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_ENRICHMENT_STARTUP_TIMEOUT_MS: "100",
      NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalEnrichmentDependencies({
      clock,
      stateStore: store
    });

    setProductionAdapterMode(store);
    setProductionAdapterMode(dependencies.transactionRunner);
    setProductionAdapterMode(dependencies.brokerOutbox);
    const service = createEnrichmentService({
      config,
      dependencies
    });
    const broker = dependencies.brokerTransport as LocalBrokerTransport;

    await service.start();

    try {
      await expect(broker.deliverEnrichment(delivery(1))).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-completion-error"
      });
      expect(service.consumer).toBeUndefined();
      expect(service.broker.consumerStatus("enrichment").activeConsumers).toBe(0);
      expect((await service.health.readiness()).status).toBe("unhealthy");
    } finally {
      await service.stop();
    }
  });
});

class ClaimTrackingStore extends InMemoryEnrichmentStateStore {
  readonly claimedTokens: string[] = [];
  readonly releaseFailures: RuntimeIdempotencyFailure[] = [];
  readonly releaseResults: RuntimeIdempotencyClaimReleaseResult[] = [];

  override async claim(
    idempotencyKey: string,
    context: RuntimeIdempotencyClaimContext
  ): Promise<RuntimeIdempotencyClaimResult> {
    const result = await super.claim(idempotencyKey, context);

    if (result.status === "claimed") {
      this.claimedTokens.push(result.claimToken);
    }

    return result;
  }

  override async releaseClaim(
    idempotencyKey: string,
    claimFailure: RuntimeIdempotencyFailure
  ): Promise<RuntimeIdempotencyClaimReleaseResult> {
    this.releaseFailures.push(claimFailure);
    const result = await super.releaseClaim(idempotencyKey, claimFailure);

    this.releaseResults.push(result);
    return result;
  }
}

class ClaimCommitThenRejectStore extends ClaimTrackingStore {
  private rejectFirstClaimResponse = true;

  override async claim(
    idempotencyKey: string,
    context: RuntimeIdempotencyClaimContext
  ): Promise<RuntimeIdempotencyClaimResult> {
    const result = await super.claim(idempotencyKey, context);

    if (this.rejectFirstClaimResponse && result.status === "claimed") {
      this.rejectFirstClaimResponse = false;
      throw new Error("claim response lost after commit");
    }

    return result;
  }
}

class CompletionRejectBeforeCommitStore extends ClaimTrackingStore {
  private rejectFirstCompletion = true;

  override markCompleted(
    idempotencyKey: string,
    completed: RuntimeIdempotencyCompletion
  ): Promise<void> {
    if (this.rejectFirstCompletion) {
      this.rejectFirstCompletion = false;
      return Promise.reject(new Error("completion unavailable before commit"));
    }

    return super.markCompleted(idempotencyKey, completed);
  }
}

class CompletionCommitThenRejectStore extends ClaimTrackingStore {
  private rejectFirstCompletionResponse = true;

  override async markCompleted(
    idempotencyKey: string,
    completed: RuntimeIdempotencyCompletion
  ): Promise<void> {
    await super.markCompleted(idempotencyKey, completed);

    if (this.rejectFirstCompletionResponse) {
      this.rejectFirstCompletionResponse = false;
      throw new Error("completion response lost after commit");
    }
  }
}

class CompletionAndReleaseRejectingStore extends ClaimTrackingStore {
  override markCompleted(): Promise<void> {
    return Promise.reject(new Error("completion store unavailable"));
  }

  override releaseClaim(): Promise<RuntimeIdempotencyClaimReleaseResult> {
    return Promise.reject(new Error("conditional release store unavailable"));
  }
}

function createConformanceContext(
  stateStore: InMemoryEnrichmentStateStore,
  clock: ManualEnrichmentClock
) {
  const config = loadEnrichmentConfig({
    NUTSNEWS_ENRICHMENT_HTTP_PORT: "0",
    NUTSNEWS_ENRICHMENT_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalEnrichmentDependencies({
    clock,
    stateStore
  });
  const telemetry = createBufferedRuntimeTelemetrySink();
  const service = createEnrichmentService({
    config,
    dependencies,
    telemetry
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    service,
    telemetry,
    workHandler: dependencies.workHandler as LocalEnrichmentWorkHandler
  };
}

function delivery(sequence: number): ValidatedEnrichmentDelivery {
  const suffix = String(sequence).padStart(12, "0");

  return {
    envelope: createMinimalEnrichmentEnvelope({
      messageId: `018f1598-2dd5-7c4f-9f92-${suffix}`,
      causationId: `018f1598-2dd5-7c4f-9f91-${suffix}`,
      correlationId: "018f1598-2dd5-7c4f-9f90-000000000001",
      idempotencyKey: "canonicalizer:enrichment:runtime1-conformance"
    }),
    payload: createMinimalEnrichmentPayload({
      idempotencyKey: "canonicalizer:enrichment:runtime1-conformance",
      sourceMessageId: `018f1598-2dd5-7c4f-9f91-${suffix}`,
      stageExecutionId: `018f1598-2dd5-7c4f-9f89-${suffix}`
    }),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}

function finalAttemptDelivery(): ValidatedEnrichmentDelivery {
  const input = delivery(9);

  return {
    ...input,
    envelope: {
      ...input.envelope,
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: input.envelope.occurredAt,
        lastAttemptAt: "2026-07-23T00:05:00.000Z"
      }
    }
  };
}

function claimContext(message: ValidatedEnrichmentDelivery): RuntimeIdempotencyClaimContext {
  return {
    envelope: message.envelope,
    stage: "enrichment",
    receivedAt: message.receivedAt ?? message.envelope.occurredAt
  };
}

function claimToken(result: RuntimeIdempotencyClaimResult): string {
  if (result.status !== "claimed") {
    throw new Error(`Expected claimed result, received ${result.status}.`);
  }

  return result.claimToken;
}

function completion(
  message: ValidatedEnrichmentDelivery,
  ownedClaimToken: string
): RuntimeIdempotencyCompletion {
  return {
    completedAt: "2026-07-23T00:00:02.000Z",
    messageId: message.envelope.messageId,
    claimToken: ownedClaimToken,
    stage: "enrichment"
  };
}

function failure(
  message: ValidatedEnrichmentDelivery,
  ownedClaimToken: string
): RuntimeIdempotencyFailure {
  return {
    failedAt: "2026-07-23T00:00:02.000Z",
    messageId: message.envelope.messageId,
    claimToken: ownedClaimToken,
    stage: "enrichment",
    reason: "runtime1-conformance",
    retryable: true
  };
}

function terminalEvents(events: readonly { readonly name: string }[]): readonly string[] {
  return events
    .map((event) => event.name)
    .filter((name) => [
      "runtime.message.accepted",
      "runtime.message.duplicate",
      "runtime.message.invalid",
      "runtime.message.retry",
      "runtime.message.dlq"
    ].includes(name));
}

function setProductionAdapterMode(dependency: { readonly adapterMode: string }): void {
  Object.defineProperty(dependency, "adapterMode", {
    configurable: true,
    value: "production"
  });
}
