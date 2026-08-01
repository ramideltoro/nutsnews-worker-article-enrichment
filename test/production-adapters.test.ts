import { getWorkerRoute } from "@ramideltoro/nutsnews-worker-contracts";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  EnrichmentProductionAdapterUnavailableError,
  createUnavailableProductionEnrichmentDurableAdapters
} from "../src/production-adapters.js";
import {
  createMinimalEnrichmentEnvelope
} from "../src/test-doubles.js";

describe("unavailable production enrichment durable adapters", () => {
  it("reports every durable acknowledgement boundary unhealthy", async () => {
    const adapters = createUnavailableProductionEnrichmentDurableAdapters();

    expect(adapters.stateStore.adapterMode).toBe("unavailable");
    expect(adapters.transactionRunner.adapterMode).toBe("unavailable");
    expect(adapters.brokerOutbox.adapterMode).toBe("unavailable");
    expect(await adapters.stateStore.probe()).toMatchObject({
      status: "unhealthy"
    });
    expect(await adapters.transactionRunner.probe()).toMatchObject({
      status: "unhealthy"
    });
    expect(await adapters.brokerOutbox.probe()).toMatchObject({
      status: "unhealthy"
    });
  });

  it("rejects state, transaction, and outbox writes instead of using volatile memory", async () => {
    const adapters = createUnavailableProductionEnrichmentDurableAdapters();
    const envelope = createMinimalEnrichmentEnvelope();
    const route = getWorkerRoute("enrichment");

    await expect(adapters.stateStore.claim(envelope.idempotencyKey, {
      envelope,
      stage: "enrichment",
      receivedAt: envelope.occurredAt
    })).rejects.toMatchObject({
      name: "EnrichmentProductionAdapterUnavailableError",
      adapter: "state-store",
      operation: "claim"
    });
    await expect(adapters.stateStore.releaseClaim(envelope.idempotencyKey, {
      failedAt: envelope.occurredAt,
      messageId: envelope.messageId,
      claimToken: "unavailable-production-claim-token",
      stage: "enrichment",
      reason: "completion-response-unavailable",
      retryable: true
    })).rejects.toMatchObject({
      name: "EnrichmentProductionAdapterUnavailableError",
      adapter: "state-store",
      operation: "releaseClaim"
    });
    await expect(adapters.stateStore.markCompleted(envelope.idempotencyKey, {
      completedAt: envelope.occurredAt,
      messageId: envelope.messageId,
      claimToken: "unavailable-production-claim-token",
      stage: "enrichment"
    })).rejects.toMatchObject({
      name: "EnrichmentProductionAdapterUnavailableError",
      adapter: "state-store",
      operation: "markCompleted"
    });
    await expect(adapters.stateStore.markFailed(envelope.idempotencyKey, {
      failedAt: envelope.occurredAt,
      messageId: envelope.messageId,
      claimToken: "unavailable-production-claim-token",
      stage: "enrichment",
      reason: "production-adapter-unavailable",
      retryable: true
    })).rejects.toMatchObject({
      name: "EnrichmentProductionAdapterUnavailableError",
      adapter: "state-store",
      operation: "markFailed"
    });
    await expect(adapters.transactionRunner.withTransaction(() => Promise.resolve("unreachable"))).rejects.toBeInstanceOf(
      EnrichmentProductionAdapterUnavailableError
    );
    await expect(adapters.brokerOutbox.record({
      envelope,
      payload: {}
    }, {
      messageId: envelope.messageId,
      stage: envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: envelope.occurredAt
    })).rejects.toMatchObject({
      name: "EnrichmentProductionAdapterUnavailableError",
      adapter: "broker-outbox",
      operation: "record"
    });
  });
});
