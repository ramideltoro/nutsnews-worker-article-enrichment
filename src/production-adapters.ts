import type {
  RuntimeIdempotencyClaimResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  EnrichmentBrokerOutbox,
  EnrichmentDatabaseTransactionRunner,
  EnrichmentDependencies,
  EnrichmentDependencyProbe,
  EnrichmentStateStore,
  EnrichmentStoredResult
} from "./dependencies.js";

export class EnrichmentProductionAdapterUnavailableError extends Error {
  readonly adapter: string;
  readonly operation: string;

  constructor(adapter: string, operation: string) {
    super(`Production enrichment ${adapter} adapter is unavailable for ${operation}.`);
    this.name = "EnrichmentProductionAdapterUnavailableError";
    this.adapter = adapter;
    this.operation = operation;
  }
}

export class UnsupportedProductionEnrichmentStateStore implements EnrichmentStateStore {
  readonly name = "unsupported-production-enrichment-state";
  readonly adapterMode = "unavailable" as const;

  probe(): EnrichmentDependencyProbe {
    return unavailableProbe("Durable PostgreSQL enrichment state adapter is not implemented; production consumption is disabled");
  }

  claim(): Promise<RuntimeIdempotencyClaimResult> {
    return unavailable("state-store", "claim");
  }

  markCompleted(): Promise<void> {
    return unavailable("state-store", "markCompleted");
  }

  markFailed(): Promise<void> {
    return unavailable("state-store", "markFailed");
  }

  findResultByFingerprint(): Promise<EnrichmentStoredResult | undefined> {
    return unavailable("state-store", "findResultByFingerprint");
  }

  recordResult(): Promise<EnrichmentStoredResult> {
    return unavailable("state-store", "recordResult");
  }
}

export class UnsupportedProductionEnrichmentTransactionRunner implements EnrichmentDatabaseTransactionRunner {
  readonly name = "unsupported-production-enrichment-transactions";
  readonly adapterMode = "unavailable" as const;

  probe(): EnrichmentDependencyProbe {
    return unavailableProbe("Durable PostgreSQL transaction adapter is not implemented; production consumption is disabled");
  }

  withTransaction<T>(): Promise<T> {
    return unavailable("transaction-runner", "withTransaction");
  }
}

export class UnsupportedProductionEnrichmentBrokerOutbox implements EnrichmentBrokerOutbox {
  readonly name = "unsupported-production-enrichment-outbox";
  readonly adapterMode = "unavailable" as const;

  probe(): EnrichmentDependencyProbe {
    return unavailableProbe("Durable PostgreSQL broker outbox adapter is not implemented; production consumption is disabled");
  }

  record(): Promise<void> {
    return unavailable("broker-outbox", "record");
  }
}

export function createUnavailableProductionEnrichmentDurableAdapters(): Pick<
  EnrichmentDependencies,
  "stateStore" | "transactionRunner" | "brokerOutbox"
> {
  return {
    stateStore: new UnsupportedProductionEnrichmentStateStore(),
    transactionRunner: new UnsupportedProductionEnrichmentTransactionRunner(),
    brokerOutbox: new UnsupportedProductionEnrichmentBrokerOutbox()
  };
}

function unavailableProbe(summary: string): EnrichmentDependencyProbe {
  return {
    status: "unhealthy",
    summary
  };
}

function unavailable<T>(adapter: string, operation: string): Promise<T> {
  return Promise.reject(new EnrichmentProductionAdapterUnavailableError(adapter, operation));
}
