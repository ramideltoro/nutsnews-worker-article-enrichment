# Architecture Notes

## Scope

The article enrichment service owns the worker-uplift service boundary that consumes canonicalizer-owned `enrichmentRequest` messages on the contracted `enrichment` route, fetches bounded article-page metadata through injected interfaces, stores durable metadata references, and publishes `enrichmentResult` messages for approval.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@0.4.0`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@0.5.0`
- Runtime contract override: force runtime's nested contracts dependency to `0.4.0` so payload validation accepts `enrichmentRequest`
- Input route boundary: `getWorkerRoute("enrichment")`
- Downstream publish route boundary: `getWorkerRoute("approval")`
- Health: bind HTTP diagnostics before broker startup; expose separate liveness, startup, and readiness probes; readiness requires an active `enrichment` main-queue consumer and production durable adapters
- Metrics: bounded shared-runtime metrics plus canonical stage outcome counters, a fixed-bucket seconds histogram, distinct health-probe gauges, truthful consumer state, and `expected_active=0` while shadowed
- Telemetry failure policy: all telemetry, logging, metrics, and flush operations are best effort and cannot alter broker acknowledgement or durable message state
- Shutdown: stop accepting deliveries, wait for in-flight handlers, cancel consumers, close broker lifecycle

## Shell Flow

1. Validate value-free configuration and secret presence by variable name.
2. Assert exact contracts/runtime package versions.
3. Bind the HTTP diagnostics listener before attempting broker startup, bound startup with `NUTSNEWS_ENRICHMENT_STARTUP_TIMEOUT_MS`, and close diagnostics through an independently bounded cleanup path if startup fails.
4. Verify that production state, transaction, and broker-outbox adapters all identify as `production` and pass bounded health probes; otherwise keep the broker idle and expose unhealthy readiness.
5. Start the shared broker lifecycle and assert enrichment/approval topology only when durable acknowledgement adapters are eligible.
6. Register an `enrichment` consumer through the shared runtime message processor.
7. Validate incoming envelopes and enrichment-stage payloads before delegated work.
8. Claim the durable idempotency interface before delegating to the injected handler.
9. Convert Runtime 0.5 claim, completion, and failure-record store exceptions into explicitly classified retry/DLQ dispositions instead of allowing a delivery to escape without a broker action.
10. Emit exactly one terminal `success`, `duplicate`, `invalid`, `retry`, or `dlq` lifecycle event for every started delivery; derive canonical counters and fixed-bucket latency from those events.
11. Expose durable transaction and broker outbox tools to the handler.
12. Bound HTTP client, DNS policy, HTML parser, state, transaction, and outbox readiness probes while keeping liveness and startup independent.
13. Re-check durable production adapters before every accepted delivery; cancel and unregister the consumer on degradation so reconnect cannot restore unsafe consumption.
14. Drain in-flight handlers before broker shutdown and set the consumer gauge to zero.

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

Local doubles back tests and health probes without production dependencies. Their adapter mode is `local`, so production dependency mode cannot connect the broker or register a consumer with ephemeral acknowledgement state. The current production durable adapter factory deliberately returns unhealthy, operation-rejecting `unavailable` implementations for state, transactions, and outbox; backend-owned work must supply real `production` implementations before the shadow consumer can run.

`NUTSNEWS_ENVIRONMENT=production` also requires `NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE=production`; a missing or mistyped dependency mode cannot activate local in-memory acknowledgement state under a production environment label.

## Safety Bounds

`NUTSNEWS_ENRICHMENT_CONCURRENCY` caps concurrent enrichment handlers. `NUTSNEWS_ENRICHMENT_PREFETCH` must be greater than or equal to concurrency.

Outbound page fetch bounds are configured with connect/read/total timeouts, response-size/decompressed-size limits, decompression-ratio limit, redirect cap, socket cap, and per-host concurrency cap. Parser bounds include timeout and DOM-node budgets.

Hostile fixtures cover redirect loops, metadata-address redirects, decompression bombs, oversized bodies, invalid encodings, malformed parse output, parser timeouts, and flaky TLS/fetch failures. Runtime replay tests prove duplicate deliveries do not create duplicate stored results or downstream approval publishes.

`NUTSNEWS_ENRICHMENT_SHADOW_MODE` remains required so bootstrap deployment cannot become the production legacy ingestion path by accident.

The shadow deployment exports `nutsnews_worker_expected_active=0`. Grafana consumer, missing-series, and freshness rules must gate on that signal until a protected backend cutover changes production ownership. Metric labels are limited to bounded operational dimensions; identifiers are retained only as structured log metadata.

Health gauges are one-hot and present before the first scrape: liveness initializes `ok`, startup and readiness initialize `unhealthy`, startup transitions with `start()`/`stop()`, and readiness is promoted only by the real readiness probe. Duration-less dependency events are kept out of the legacy runtime duration summary; only explicitly measured dependency durations may populate it.

HTTP-first startup makes the initial fail-closed state observable while a broker connection is pending. Startup is time-bounded and raises a named error after the configured deadline; failed-startup cleanup closes the diagnostics listener concurrently and within its own bound even if transport shutdown never settles.
