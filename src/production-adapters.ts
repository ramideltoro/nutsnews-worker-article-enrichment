import { randomUUID } from "node:crypto";

import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeIdempotencyClaimContext,
  RuntimeIdempotencyClaimReleaseResult,
  RuntimeIdempotencyClaimResult,
  RuntimeIdempotencyCompletion,
  RuntimeIdempotencyFailure
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  Pool,
  type PoolClient,
  type QueryResultRow
} from "pg";

import type {
  EnrichmentBrokerOutbox,
  EnrichmentDatabaseTransaction,
  EnrichmentDatabaseTransactionRunner,
  EnrichmentDependencies,
  EnrichmentDependencyProbe,
  EnrichmentStateStore,
  EnrichmentStoredResult
} from "./dependencies.js";
import { sha256Hex } from "./ids.js";

const SCHEMA = "worker_uplift_enrichment";
const IDEMPOTENCY_LEASE_MS = 300_000;

interface PgEnrichmentTransaction extends EnrichmentDatabaseTransaction {
  readonly client: PoolClient;
}

interface InboxRow extends QueryResultRow {
  readonly status: string;
  readonly received_at: Date;
  readonly processed_at: Date | null;
  readonly lease_active: boolean;
}

interface ResultRow extends QueryResultRow {
  readonly diagnostic_metadata: unknown;
}

interface StatusRow extends QueryResultRow {
  readonly status: string;
}

export interface ProductionEnrichmentDurableAdapters extends Pick<
  EnrichmentDependencies,
  "stateStore" | "transactionRunner" | "brokerOutbox"
> {
  close(): Promise<void>;
}

export function createProductionEnrichmentDurableAdapters(options: {
  readonly databaseUrl: string;
  readonly applicationName: string;
  readonly maxConnections: number;
  readonly timeoutMs: number;
}): ProductionEnrichmentDurableAdapters {
  const pool = new Pool({
    connectionString: options.databaseUrl,
    application_name: options.applicationName,
    max: options.maxConnections,
    connectionTimeoutMillis: options.timeoutMs,
    query_timeout: options.timeoutMs,
    statement_timeout: options.timeoutMs,
    idleTimeoutMillis: 30_000,
    allowExitOnIdle: true
  });
  pool.on("error", () => undefined);
  return {
    stateStore: new PostgresEnrichmentStateStore(pool),
    transactionRunner: new PostgresEnrichmentTransactionRunner(pool),
    brokerOutbox: new PostgresEnrichmentBrokerOutbox(pool),
    close: () => pool.end()
  };
}

export class PostgresEnrichmentTransactionRunner implements EnrichmentDatabaseTransactionRunner {
  readonly name = "postgres-enrichment-transactions";
  readonly adapterMode = "production" as const;

  constructor(private readonly pool: Pool) {}

  probe(): Promise<EnrichmentDependencyProbe> {
    return probe(this.pool, "enrichment PostgreSQL transactions ready");
  }

  async withTransaction<T>(operation: (transaction: EnrichmentDatabaseTransaction) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await operation({
        transactionId: randomUUID(),
        client
      } as PgEnrichmentTransaction);
      await client.query("COMMIT");
      return result;
    } catch (error: unknown) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}

export class PostgresEnrichmentStateStore implements EnrichmentStateStore {
  readonly name = "postgres-enrichment-state";
  readonly adapterMode = "production" as const;
  readonly idempotencyLeaseMs = IDEMPOTENCY_LEASE_MS;

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<EnrichmentDependencyProbe> {
    try {
      const result = await this.pool.query<{ readonly ready: boolean }>(
        `SELECT has_schema_privilege(current_user, '${SCHEMA}', 'USAGE')
          AND has_table_privilege(current_user, '${SCHEMA}.inbox', 'SELECT,INSERT,UPDATE')
          AND has_table_privilege(current_user, '${SCHEMA}.enrichment_records', 'SELECT,INSERT,UPDATE') AS ready`
      );
      return result.rows[0]?.ready === true
        ? { status: "ok", summary: "enrichment PostgreSQL state ready" }
        : { status: "unhealthy", summary: "enrichment PostgreSQL state scope incomplete" };
    } catch {
      return { status: "unhealthy", summary: "enrichment PostgreSQL state probe failed" };
    }
  }

  async claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    const client = await this.pool.connect();
    const token = randomUUID();
    try {
      await client.query("BEGIN");
      const inserted = await client.query<{ readonly received_at: Date }>(
        `INSERT INTO ${SCHEMA}.inbox (
           message_id, pipeline_run_id, stage_execution_id, source_stage, source_message_id,
           entity_kind, entity_id, schema_version, operation_version, idempotency_key,
           payload_ref, payload_digest, received_at, status, diagnostic_metadata
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::timestamptz,'processing',
           jsonb_build_object(
             'claimToken', $14::text,
             'claimLeaseExpiresAtEpochMs', floor(extract(epoch from clock_timestamp()) * 1000) + $15::bigint
           ))
         ON CONFLICT (idempotency_key) DO NOTHING RETURNING received_at`,
        [
          context.envelope.messageId,
          context.envelope.correlationId,
          context.envelope.messageId,
          context.envelope.producer.name,
          context.envelope.causationId,
          context.envelope.aggregate.type,
          context.envelope.aggregate.id,
          context.envelope.schemaVersion,
          Math.max(1, context.envelope.aggregate.version),
          idempotencyKey,
          context.envelope.payloadRef.uri,
          context.envelope.payloadRef.digest ?? sha256Hex(JSON.stringify(context.envelope.payloadRef)),
          context.receivedAt,
          token,
          this.idempotencyLeaseMs
        ]
      );
      let result: RuntimeIdempotencyClaimResult;
      if ((inserted.rowCount ?? 0) > 0) {
        result = { status: "claimed", firstSeenAt: context.receivedAt, replay: false, claimToken: token };
      } else {
        const existing = await client.query<InboxRow>(
          `SELECT status, received_at, processed_at,
                  status='processing'
                    AND COALESCE((diagnostic_metadata->>'claimLeaseExpiresAtEpochMs')::bigint, 0)
                        > floor(extract(epoch from clock_timestamp()) * 1000) AS lease_active
           FROM ${SCHEMA}.inbox WHERE idempotency_key=$1 FOR UPDATE`,
          [idempotencyKey]
        );
        const row = existing.rows[0];
        if (row === undefined) {
          result = { status: "in-progress", firstSeenAt: context.receivedAt };
        } else if (row.status === "processed") {
          result = {
            status: "already-completed",
            firstSeenAt: row.received_at.toISOString(),
            completedAt: (row.processed_at ?? row.received_at).toISOString()
          };
        } else if (row.lease_active) {
          result = { status: "in-progress", firstSeenAt: row.received_at.toISOString() };
        } else {
          const updated = await client.query(
            `UPDATE ${SCHEMA}.inbox
             SET status='processing', diagnostic_metadata=diagnostic_metadata || jsonb_build_object(
                   'claimToken', $2::text,
                   'claimLeaseExpiresAtEpochMs', floor(extract(epoch from clock_timestamp()) * 1000) + $3::bigint,
                   'replayMessageId', $4::text
                 ),
                 sanitized_error_code=NULL, sanitized_error_message=NULL
             WHERE idempotency_key=$1 AND status <> 'processed'`,
            [idempotencyKey, token, this.idempotencyLeaseMs, context.envelope.messageId]
          );
          result = (updated.rowCount ?? 0) === 1
            ? { status: "claimed", firstSeenAt: row.received_at.toISOString(), replay: true, claimToken: token }
            : { status: "in-progress", firstSeenAt: row.received_at.toISOString() };
        }
      }
      await client.query("COMMIT");
      return result;
    } catch (error: unknown) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    const result = await this.pool.query(
      `UPDATE ${SCHEMA}.inbox SET status='processed', processed_at=$3::timestamptz,
       diagnostic_metadata=diagnostic_metadata || $4::jsonb
       WHERE idempotency_key=$1 AND status='processing'
         AND diagnostic_metadata->>'claimToken'=$2`,
      [idempotencyKey, completion.claimToken, completion.completedAt, JSON.stringify({ completion })]
    );
    requireOwned(result.rowCount, "complete");
  }

  async markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    const result = await this.pool.query(
      `UPDATE ${SCHEMA}.inbox SET status='failed', sanitized_error_code=$3,
       diagnostic_metadata=diagnostic_metadata || $4::jsonb
       WHERE idempotency_key=$1 AND status='processing'
         AND diagnostic_metadata->>'claimToken'=$2`,
      [idempotencyKey, failure.claimToken, safeCode(failure.reason), JSON.stringify({ failure })]
    );
    requireOwned(result.rowCount, "fail");
  }

  async releaseClaim(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<RuntimeIdempotencyClaimReleaseResult> {
    const completed = await this.pool.query<StatusRow>(
      `SELECT status FROM ${SCHEMA}.inbox WHERE idempotency_key=$1`,
      [idempotencyKey]
    );
    if (completed.rows[0]?.status === "processed") {
      return { status: "preserved-completed" };
    }
    const result = await this.pool.query(
      `UPDATE ${SCHEMA}.inbox SET status='failed', sanitized_error_code=$3,
       diagnostic_metadata=diagnostic_metadata || $4::jsonb
       WHERE idempotency_key=$1 AND status='processing'
         AND diagnostic_metadata->>'claimToken'=$2`,
      [idempotencyKey, failure.claimToken, safeCode(failure.reason), JSON.stringify({ failure })]
    );
    return (result.rowCount ?? 0) === 1 ? { status: "released" } : { status: "not-owned" };
  }

  async findResultByFingerprint(
    canonicalArticleId: string,
    articleVersion: number,
    contentFingerprint: string,
    transaction: EnrichmentDatabaseTransaction
  ): Promise<EnrichmentStoredResult | undefined> {
    const result = await transactionClient(transaction).query<ResultRow>(
      `SELECT diagnostic_metadata FROM ${SCHEMA}.enrichment_records
       WHERE article_identity_hash=$1 AND enrichment_version=$2
         AND diagnostic_metadata->>'contentFingerprint'=$3
       LIMIT 1`,
      [canonicalArticleId, articleVersion, contentFingerprint]
    );
    return storedResult(result.rows[0]?.diagnostic_metadata);
  }

  async recordResult(result: EnrichmentStoredResult, transaction: EnrichmentDatabaseTransaction): Promise<EnrichmentStoredResult> {
    const client = transactionClient(transaction);
    const saved = await client.query<ResultRow>(
      `INSERT INTO ${SCHEMA}.enrichment_records (
         article_identity_hash,enrichment_version,image_url_ref,metadata_ref,status,diagnostic_metadata,created_at)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::timestamptz)
       ON CONFLICT (article_identity_hash,enrichment_version) DO UPDATE SET
         image_url_ref=EXCLUDED.image_url_ref,
         metadata_ref=EXCLUDED.metadata_ref,
         status=EXCLUDED.status,
         diagnostic_metadata=EXCLUDED.diagnostic_metadata,
         created_at=EXCLUDED.created_at
       WHERE COALESCE(
         (${SCHEMA}.enrichment_records.diagnostic_metadata->'resultSnapshot'->>'recordedAt')::timestamptz,
         '-infinity'::timestamptz
       ) <= EXCLUDED.created_at
       RETURNING diagnostic_metadata`,
      [
        result.canonicalArticleId,
        result.articleVersion,
        result.imageUrl ?? null,
        result.metadataRef.uri,
        resultStatus(result),
        JSON.stringify({
          contentFingerprint: result.contentFingerprint,
          resultSnapshot: result
        }),
        result.recordedAt
      ]
    );
    const stored = storedResult(saved.rows[0]?.diagnostic_metadata);
    if (stored !== undefined) {
      return stored;
    }
    const current = await client.query<ResultRow>(
      `SELECT diagnostic_metadata FROM ${SCHEMA}.enrichment_records
       WHERE article_identity_hash=$1 AND enrichment_version=$2`,
      [result.canonicalArticleId, result.articleVersion]
    );
    const currentResult = storedResult(current.rows[0]?.diagnostic_metadata);
    if (currentResult === undefined) {
      throw new Error("enrichment result write did not return a durable result");
    }
    return currentResult;
  }
}

export class PostgresEnrichmentBrokerOutbox implements EnrichmentBrokerOutbox {
  readonly name = "postgres-enrichment-outbox";
  readonly adapterMode = "production" as const;

  constructor(private readonly pool: Pool) {}

  probe(): Promise<EnrichmentDependencyProbe> {
    return probe(this.pool, "enrichment PostgreSQL outbox ready");
  }

  async record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    await this.pool.query(
      `INSERT INTO ${SCHEMA}.outbox (
       outbox_message_id,pipeline_run_id,stage_execution_id,destination_stage,routing_key,
       entity_kind,entity_id,schema_version,operation_version,idempotency_key,payload_ref,payload_digest,
       published_at,confirmed_at,status,diagnostic_metadata)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::timestamptz,$13::timestamptz,'confirmed',$14::jsonb)
       ON CONFLICT (idempotency_key) DO UPDATE SET
         outbox_message_id=EXCLUDED.outbox_message_id,
         published_at=EXCLUDED.published_at,
         confirmed_at=EXCLUDED.confirmed_at,
         status='confirmed',
         diagnostic_metadata=EXCLUDED.diagnostic_metadata`,
      [
        receipt.messageId,
        command.envelope.correlationId,
        command.envelope.messageId,
        command.envelope.route,
        "nutsnews.worker.approval.v1",
        command.envelope.aggregate.type,
        command.envelope.aggregate.id,
        command.envelope.schemaVersion,
        Math.max(1, command.envelope.aggregate.version),
        command.envelope.idempotencyKey,
        command.envelope.payloadRef.uri,
        command.envelope.payloadRef.digest ?? sha256Hex(JSON.stringify(command.payload)),
        receipt.confirmedAt,
        JSON.stringify({ envelope: command.envelope, payload: command.payload })
      ]
    );
  }
}

function transactionClient(transaction: EnrichmentDatabaseTransaction): PoolClient {
  const client = (transaction as Partial<PgEnrichmentTransaction>).client;
  if (client === undefined) {
    throw new Error("enrichment PostgreSQL operation requires an active transaction");
  }
  return client;
}

async function probe(pool: Pool, summary: string): Promise<EnrichmentDependencyProbe> {
  try {
    await pool.query("SELECT 1");
    return { status: "ok", summary };
  } catch {
    return { status: "unhealthy", summary: "enrichment PostgreSQL probe failed" };
  }
}

function storedResult(value: unknown): EnrichmentStoredResult | undefined {
  const snapshot = record(record(value).resultSnapshot);
  return typeof snapshot.requestId === "string"
    && typeof snapshot.canonicalArticleId === "string"
    && Number.isSafeInteger(snapshot.articleVersion)
    && typeof snapshot.contentFingerprint === "string"
    && typeof snapshot.recordedAt === "string"
    ? snapshot as unknown as EnrichmentStoredResult
    : undefined;
}

function resultStatus(result: EnrichmentStoredResult): "enriched" | "partial" | "failed" | "skipped" {
  return result.outcome === "image-found" ? "enriched" : result.outcome;
}

function requireOwned(rowCount: number | null, operation: string): void {
  if ((rowCount ?? 0) !== 1) {
    throw new Error(`enrichment idempotency ownership lost during ${operation}`);
  }
}

function record(value: unknown): Readonly<Record<string, unknown>> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Readonly<Record<string, unknown>>
    : {};
}

function safeCode(value: string): string {
  return value.replace(/[^a-zA-Z0-9_.:-]/gu, "_").slice(0, 128);
}
