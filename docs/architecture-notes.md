# Architecture Notes

## Scope

The article enrichment service owns the worker-uplift service boundary that consumes canonicalizer-owned `enrichmentRequest` messages on the contracted `enrichment` route, fetches bounded article-page metadata through injected interfaces, stores durable metadata references, and publishes `enrichmentResult` messages for approval.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@0.4.0`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@0.5.0`
- Runtime contract override: force runtime's nested contracts dependency to `0.4.0` so payload validation accepts `enrichmentRequest`
- Input route boundary: `getWorkerRoute("enrichment")`
- Downstream publish route boundary: `getWorkerRoute("approval")`
- Health: separate liveness, startup, and readiness probes; readiness requires an active `enrichment` main-queue consumer
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

The handler is value-free at the broker boundary: it never emits full HTML, article bodies, credentials, or production secret values to RabbitMQ or logs. It does not call AI providers, approve content, translate content, persist backend article rows, or publish user-facing articles.

## Enrichment Flow

1. Validate the incoming `enrichmentRequest` payload through the shared runtime processor.
2. Check DNS/SSRF policy before any article page fetch.
3. Fetch the canonical URL with configured timeout, redirect, response-size, decompression, socket, and per-host concurrency bounds.
4. Re-check DNS/SSRF policy for every observed redirect and final URL before parsing.
5. Reject hostile responses that exceed redirect, body, decompression-ratio, or encoding bounds.
6. Compute a content fingerprint from safe response metadata and durable body reference.
7. Reuse an existing stored enrichment result when the fingerprint is unchanged.
8. Parse metadata through the injected HTML parser interface with parser timeout and DOM-node budgets, without carrying full HTML on RabbitMQ.
9. Normalize image candidates, strip tracking parameters, reject icons/tiny/generic tracker candidates, and rank RSS, Open Graph, Twitter, JSON-LD, srcset, and HTML sources.
10. Record a bounded metadata reference and publish an `enrichmentResult` payload to approval.
11. Return runtime retry for retryable fetch/parser failures so retry exhaustion reaches the enrichment DLQ without duplicate approval publication.

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

Outbound page fetch bounds are configured with connect/read/total timeouts, response-size/decompressed-size limits, decompression-ratio limit, redirect cap, socket cap, and per-host concurrency cap. Parser bounds include timeout and DOM-node budgets.

Hostile fixtures cover redirect loops, metadata-address redirects, decompression bombs, oversized bodies, invalid encodings, malformed parse output, parser timeouts, and flaky TLS/fetch failures. Runtime replay tests prove duplicate deliveries do not create duplicate stored results or downstream approval publishes.

`NUTSNEWS_ENRICHMENT_SHADOW_MODE` remains required so bootstrap deployment cannot become the production legacy ingestion path by accident.
