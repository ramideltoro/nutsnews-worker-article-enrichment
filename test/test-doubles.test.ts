import {
  getWorkerRoute,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  LocalBrokerTransport,
  LocalEnrichmentBrokerOutbox,
  LocalEnrichmentDnsPolicy,
  LocalEnrichmentHtmlParser,
  LocalEnrichmentHttpClient,
  LocalEnrichmentTransactionRunner,
  createMinimalEnrichmentDelivery
} from "../src/test-doubles.js";

describe("enrichment test doubles", () => {
  it("requires a registered local broker consumer before delivery", async () => {
    const broker = new LocalBrokerTransport();

    await broker.connect();

    await expect(broker.deliverEnrichment(createMinimalEnrichmentDelivery())).rejects.toThrow("No local consumer is registered for enrichment.");
  });

  it("records local transaction and outbox boundaries without external dependencies", async () => {
    const runner = new LocalEnrichmentTransactionRunner();
    const outbox = new LocalEnrichmentBrokerOutbox();
    const route = getWorkerRoute("approval");
    const command = {
      envelope: {
        schemaId: route.schemaId,
        schemaVersion: 1,
        route: "approval",
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4801",
        causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4720",
        correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4710",
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        idempotencyKey: "enrichment:approval:candidate-world-001",
        aggregate: {
          type: "candidate",
          id: "candidate-world-001",
          version: 1
        },
        occurredAt: "2026-07-23T00:00:00.000Z",
        attempt: {
          count: 1,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z"
        },
        producer: {
          name: "enrichment",
          version: "0.1.0"
        },
        payloadRef: {
          kind: "backend-record",
          uri: "backend://worker-uplift/enrichment/candidate-world-001/approval-request",
          mediaType: "application/json",
          sizeBytes: 512
        }
      } satisfies WorkerMessageEnvelope,
      payload: {}
    };

    await expect(runner.withTransaction((transaction) => Promise.resolve(transaction.transactionId))).resolves.toBe("local-transaction-1");
    await outbox.record(command, {
      messageId: command.envelope.messageId,
      stage: "approval",
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });

    expect(runner.transactions).toHaveLength(1);
    expect(outbox.records).toHaveLength(1);
  });

  it("provides injectable HTTP, DNS, and HTML parser doubles", async () => {
    const httpClient = new LocalEnrichmentHttpClient();
    const dnsPolicy = new LocalEnrichmentDnsPolicy();
    const htmlParser = new LocalEnrichmentHtmlParser();

    await expect(dnsPolicy.checkUrl("https://articles.example.test/story")).resolves.toEqual({
      allowed: true,
      reason: "allowed"
    });
    await expect(httpClient.fetch({
      url: "https://articles.example.test/story",
      connectTimeoutMs: 5_000,
      readTimeoutMs: 10_000,
      totalTimeoutMs: 30_000,
      maxResponseBytes: 1_048_576,
      maxRedirects: 3
    })).resolves.toMatchObject({
      finalUrl: "https://articles.example.test/story",
      statusCode: 200,
      bodyBytes: 0
    });
    await expect(htmlParser.parse({
      canonicalArticleId: "article-001",
      finalUrl: "https://articles.example.test/story",
      htmlRef: {
        kind: "backend-record",
        uri: "backend://worker-uplift/enrichment/article-001/html",
        mediaType: "text/html"
      }
    })).resolves.toEqual({
      imageCandidates: []
    });

    expect(dnsPolicy.checkedUrls).toEqual([
      "https://articles.example.test/story"
    ]);
    expect(httpClient.requests).toHaveLength(1);
    expect(htmlParser.inputs).toHaveLength(1);
  });
});
