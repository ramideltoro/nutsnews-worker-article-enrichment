# Architecture Notes

## Scope

The article enrichment service owns the worker-uplift service boundary that consumes enrichment-stage messages on the contracted `enrichment` route. Issue #101 creates the deployable shell, dependency interfaces, health endpoints, CI/container baseline, and bounded network configuration. Later issues add article page fetch, metadata extraction, image ranking, durable result persistence, and approval request publication.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@0.4.0`
- Input route boundary: `getWorkerRoute("enrichment")`
- Downstream publish route boundary: `getWorkerRoute("approval")`
- Health: separate liveness, startup, and readiness probes
- Metrics: bounded Prometheus text from the shared runtime sink
- Shutdown: stop accepting deliveries, wait for in-flight handlers, cancel consumers, close broker lifecycle

## Shell Flow

1. Validate value-free configuration and secret presence by variable name.
2. Assert exact contracts/runtime package versions.
3. Start the shared broker lifecycle and assert enrichment/approval topology.
4. Register an `enrichment` consumer through the shared runtime message processor.
5. Validate incoming envelopes and enrichment-stage payloads before delegated work.
6. Claim the durable idempotency interface before delegating to the injected handler.
7. Expose durable transaction and broker outbox tools to the handler.
8. Probe HTTP client, DNS policy, HTML parser, state, transaction, and outbox dependencies for readiness.
9. Drain in-flight handlers before broker shutdown.

The bootstrap handler is intentionally local and value-free. It does not fetch article pages, parse HTML, hydrate images, call AI providers, approve content, translate content, persist backend article rows, or publish user-facing articles.

## Dependency Interfaces

The repository defines narrow interfaces for:

- broker transport;
- enrichment state/idempotency;
- database transaction runner;
- broker outbox;
- HTTP article page client;
- DNS and SSRF policy checks;
- HTML metadata parser;
- enrichment work handler.

Local doubles back tests and health probes without production dependencies. Backend-owned deployment configuration supplies real database and RabbitMQ values later.

## Safety Bounds

`NUTSNEWS_ENRICHMENT_CONCURRENCY` caps concurrent enrichment handlers. `NUTSNEWS_ENRICHMENT_PREFETCH` must be greater than or equal to concurrency.

Outbound page fetch bounds are configured with connect/read/total timeouts, response-size limit, and redirect cap. These are configuration surfaces only in #101; network business logic is added by later enrichment issues.

`NUTSNEWS_ENRICHMENT_SHADOW_MODE` remains required so bootstrap deployment cannot become the production legacy ingestion path by accident.
