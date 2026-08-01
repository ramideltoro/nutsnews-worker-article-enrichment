import type { Pool } from "pg";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import type { EnrichmentStoredResult } from "../src/dependencies.js";
import {
  PostgresEnrichmentStateStore,
  PostgresEnrichmentTransactionRunner
} from "../src/production-adapters.js";
import { createMinimalEnrichmentEnvelope } from "../src/test-doubles.js";

describe("production enrichment durable adapters", () => {
  it("claims a new delivery in one committed PostgreSQL transaction", async () => {
    const statements: string[] = [];
    const client = {
      query: vi.fn((sql: string) => {
        statements.push(sql.trim().split(/\s+/u)[0] ?? "");
        return Promise.resolve(sql.includes("INSERT INTO")
          ? { rowCount: 1, rows: [{ received_at: new Date("2026-08-01T20:00:00.000Z") }] }
          : { rowCount: null, rows: [] });
      }),
      release: vi.fn()
    };
    const pool = { connect: vi.fn(() => Promise.resolve(client)) } as unknown as Pool;
    const envelope = createMinimalEnrichmentEnvelope();

    const claim = await new PostgresEnrichmentStateStore(pool).claim(envelope.idempotencyKey, {
      envelope,
      stage: "enrichment",
      receivedAt: "2026-08-01T20:00:00.000Z"
    });

    expect(claim).toMatchObject({ status: "claimed", replay: false });
    expect(statements).toEqual(["BEGIN", "INSERT", "COMMIT"]);
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("commits successful state work and rolls back failed state work", async () => {
    const statements: string[] = [];
    const client = {
      query: vi.fn((sql: string) => {
        statements.push(sql);
        return Promise.resolve({ rowCount: null, rows: [] });
      }),
      release: vi.fn()
    };
    const pool = { connect: vi.fn(() => Promise.resolve(client)) } as unknown as Pool;
    const runner = new PostgresEnrichmentTransactionRunner(pool);

    await expect(runner.withTransaction((transaction) => Promise.resolve(transaction.transactionId))).resolves.toEqual(expect.any(String));
    await expect(runner.withTransaction(() => Promise.reject(new Error("write failed")))).rejects.toThrow("write failed");

    expect(statements.filter((statement) => statement === "COMMIT")).toHaveLength(1);
    expect(statements.filter((statement) => statement === "ROLLBACK")).toHaveLength(1);
    expect(client.release).toHaveBeenCalledTimes(2);
  });

  it("stores and reloads the complete enrichment result snapshot", async () => {
    const result = enrichmentResult();
    const client = {
      query: vi.fn((_sql: string, values?: readonly unknown[]) => Promise.resolve({
        rowCount: 1,
        rows: [{
          diagnostic_metadata: {
            contentFingerprint: result.contentFingerprint,
            resultSnapshot: result
          }
        }],
        values
      })),
      release: vi.fn()
    };
    const pool = {} as Pool;
    const store = new PostgresEnrichmentStateStore(pool);
    const transaction = {
      transactionId: "test-transaction",
      client
    } as never;

    await expect(store.recordResult(result, transaction)).resolves.toEqual(result);
    await expect(store.findResultByFingerprint(
      result.canonicalArticleId,
      result.articleVersion,
      result.contentFingerprint,
      transaction
    )).resolves.toEqual(result);

    const insertValues = client.query.mock.calls[0]?.[1];
    expect(insertValues?.[4]).toBe("enriched");
    expect(JSON.parse(String(insertValues?.[5]))).toMatchObject({ resultSnapshot: result });
  });
});

function enrichmentResult(): EnrichmentStoredResult {
  return {
    requestId: "7436d506-53f9-4bf9-b938-dddecbb4320a",
    canonicalArticleId: "77a68bea-1bb2-4f17-9051-16ba99984948",
    articleVersion: 1,
    candidateId: "candidate-1",
    canonicalUrl: "https://example.com/news/1",
    finalUrl: "https://example.com/news/1",
    contentFingerprint: "a".repeat(64),
    imageStatus: "hydrated",
    imageUrl: "https://example.com/image.jpg",
    outcome: "image-found",
    metadataRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/enrichment/77a68bea/result",
      mediaType: "application/json",
      contentFingerprint: "a".repeat(64),
      canonicalArticleId: "77a68bea-1bb2-4f17-9051-16ba99984948",
      articleVersion: 1,
      title: "A real article title",
      description: "A real article description",
      publishedAt: "2026-08-01T19:55:00.000Z",
      language: "en"
    },
    recordedAt: "2026-08-01T20:00:00.000Z"
  };
}
