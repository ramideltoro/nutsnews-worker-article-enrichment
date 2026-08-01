# nutsnews-worker-article-enrichment

Deployable worker-uplift article enrichment service shell for NutsNews.

## Responsibility

Own the enrichment service boundary that consumes contracted `enrichmentRequest` messages, fetches bounded article-page metadata through injected network/parser interfaces, and publishes shadow-safe enrichment results for approval.

The service now performs DNS/SSRF policy checks, bounded article page fetch orchestration, parser-backed metadata extraction, image candidate normalization/ranking, durable result reuse by content fingerprint, and contract-backed `enrichmentResult` publication.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-article-enrichment:${GITHUB_SHA}`. This repository is deployable only through backend-owned infrastructure.

The image runs as a non-root user, exposes port `8080`, and serves:

- `GET /live`
- `GET /startup`
- `GET /ready`
- `GET /metrics`
- `GET /config-schema`

## Runtime Dependencies

The service consumes exact immutable worker-uplift package versions:

- `@ramideltoro/nutsnews-worker-contracts@1.0.0`
- `@ramideltoro/nutsnews-worker-runtime@1.0.0`

Local and CI installs use the owner-scoped GitHub Packages npm registry. No package token value is committed.

Runtime `1.0.0` pins Contracts `1.0.0` directly, so no nested dependency override is required. Startup verifies the installed package pair and refuses any version drift.

`/ready` is unhealthy whenever the `enrichment` main queue has zero active consumers. Every dependency readiness probe is bounded by `NUTSNEWS_ENRICHMENT_STARTUP_TIMEOUT_MS`. Consumer cancellation and channel-drop recovery emit bounded structured runtime events and Prometheus consumer-state metrics. The HTTP diagnostics listener binds before broker startup, and broker startup is bounded by the same deadline, so `/live`, `/startup`, `/ready`, and `/metrics` expose fail-closed state while startup is pending. Failed-startup cleanup closes diagnostics independently even if broker cleanup stalls.

Production mode does not fall back to the local in-memory acknowledgement path, and `NUTSNEWS_ENVIRONMENT=production` is rejected unless dependency mode is also `production`. A defensive service-level check also keeps startup and readiness unhealthy, prevents consumer startup, and rejects direct processing if an injected configuration bypasses the environment parser. Until real PostgreSQL state, transaction, and broker-outbox adapters are implemented, all three production durable adapters report `unavailable`, broker topology is not asserted, no consumer is registered, and readiness remains unhealthy. Even adapters identified as `production` must pass bounded startup probes before the broker can connect. If a production adapter degrades after startup, the current delivery receives an explicit retry/DLQ disposition and the consumer is cancelled and unregistered so RabbitMQ recovery cannot silently resume it. Liveness and startup remain available for diagnostics while this shadow-only service is safely inactive when its configuration mode is valid.

`/metrics` also exports the Grafana worker-uplift contract:

- `nutsnews_worker_uplift_stage_events_total{environment,service,outcome}` exposes the bounded `success`, `duplicate`, `invalid`, `retry`, `dlq`, and `failure` outcome set from the first scrape, then counts exactly one classified terminal lifecycle outcome for every started enrichment delivery;
- `nutsnews_worker_uplift_stage_latency_seconds` is a fixed-bucket histogram with `0.005`, `0.01`, `0.025`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2.5`, `5`, `10`, `30`, `60`, `120`, and `300` second boundaries plus `+Inf`;
- `nutsnews_worker_health_probe{environment,service,probe,outcome}` keeps liveness, startup, and readiness distinct;
- `nutsnews_worker_health_check` and `nutsnews_worker_health_check_duration_seconds` expose Runtime-owned, allowlisted per-check readiness state and latency without duplicating the compatibility probe family;
- Runtime-owned `nutsnews_worker_consumers{environment,host,service,version,stage,queue,outcome}` reports the main-queue consumer count without a parallel compatibility family;
- Runtime-owned `nutsnews_worker_last_success_timestamp_seconds{environment,service}` advances monotonically on accepted and duplicate deliveries; and
- Runtime-owned `nutsnews_worker_expected_active{environment,service}` defaults to `0` while this service is shadow-only, so consumer and freshness alerting remains gated until protected cutover.

Only bounded operational dimensions are metric labels. Message, article, feed, idempotency, correlation, and trace identifiers remain structured log fields and are never Prometheus labels.

The liveness/startup/readiness gauges are present on the first scrape: liveness starts healthy while startup and readiness start fail-closed, startup follows the service lifecycle, and readiness changes only from an evaluated readiness result or a known consumer shutdown. A consumer-loss event immediately demotes both aggregate readiness and the existing Runtime `rabbitmq-consumer` check on the next scrape without adding a fabricated health-check duration; the next real readiness evaluation records fresh check latency. Runtime `1.0.0` uses opaque claim tokens for compare-and-set completion, failure, and conditional release; ambiguous claim failures are never released, stale owners cannot alter a newer claim, and a completion that committed before its response failed remains acknowledged. Production adapters must atomically reclaim expired claims with a lease no longer than five minutes; this repository's unavailable production adapter remains fail-closed until a backend-owned implementation satisfies that contract. Each started delivery still emits exactly one terminal lifecycle outcome. Telemetry, log, metric, and telemetry-flush failures are best effort and cannot change message acknowledgement, idempotency, retry, or DLQ behavior. Duration-less dependency events remain available in structured logs but are not forwarded into duration histograms, and startup does not emit a fabricated zero-millisecond dependency observation.

## Configuration

The value-free configuration schema lives in `src/config.ts` and is exposed at `/config-schema`. Production deployments must provide dependency values through backend-owned deployment configuration, not this repository.

Important variables:

- `NUTSNEWS_ENVIRONMENT`: `production` requires production dependency mode
- `NUTSNEWS_ENRICHMENT_BUILD_REVISION`: production requires the immutable lowercase 40-character Git SHA baked into the image
- `NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE`: `test` or `production`
- `NUTSNEWS_ENRICHMENT_DATABASE_URL`
- `NUTSNEWS_ENRICHMENT_RABBITMQ_URL`
- `NUTSNEWS_ENRICHMENT_CONCURRENCY`
- `NUTSNEWS_ENRICHMENT_PREFETCH`
- `NUTSNEWS_ENRICHMENT_CONNECT_TIMEOUT_MS`
- `NUTSNEWS_ENRICHMENT_READ_TIMEOUT_MS`
- `NUTSNEWS_ENRICHMENT_TOTAL_TIMEOUT_MS`
- `NUTSNEWS_ENRICHMENT_MAX_RESPONSE_BYTES`
- `NUTSNEWS_ENRICHMENT_MAX_DECOMPRESSED_BYTES`
- `NUTSNEWS_ENRICHMENT_MAX_DECOMPRESSION_RATIO`
- `NUTSNEWS_ENRICHMENT_MAX_REDIRECTS`
- `NUTSNEWS_ENRICHMENT_MAX_CONCURRENT_SOCKETS`
- `NUTSNEWS_ENRICHMENT_PER_HOST_CONCURRENCY`
- `NUTSNEWS_ENRICHMENT_PARSER_TIMEOUT_MS`
- `NUTSNEWS_ENRICHMENT_MAX_DOM_NODES`
- `NUTSNEWS_ENRICHMENT_STARTUP_TIMEOUT_MS`
- `NUTSNEWS_ENRICHMENT_SHADOW_MODE`

`NUTSNEWS_ENRICHMENT_SHADOW_MODE` must remain `true` until backend-owned cutover work explicitly changes the deployment contract.

## Service Boundary

The service registers the contracted `enrichment` consumer route and downstream `approval` publish route through the shared runtime broker lifecycle. The shared message processor validates worker envelopes and enrichment-stage payloads, applies the durable idempotency interface, emits exactly one terminal lifecycle outcome for every started delivery, delegates work to the injected enrichment handler, and drains in-flight deliveries during shutdown.

The enrichment handler:

- accepts canonicalizer-owned `enrichmentRequest` payloads and never emits full HTML bodies;
- checks DNS/SSRF policy before fetch and for every observed redirect/final URL;
- fetches article pages with configured connect/read/total timeouts, redirect cap, response-size/decompression limits, and socket/per-host concurrency bounds;
- parses bounded metadata through the injected HTML parser interface;
- propagates parser timeout and DOM-node budgets to the parser;
- classifies retryable fetch/parser failures into runtime retry/DLQ handling without publishing approval work;
- normalizes relative image URLs, strips tracking parameters, rejects icons/tiny/generic tracker candidates, and ranks RSS, Open Graph, Twitter, JSON-LD, srcset, and HTML image sources;
- stores bounded metadata references by content fingerprint and reuses unchanged results;
- publishes contracted `enrichmentResult` payloads with `hydrated`, `no_thumbnail`, or `transient_failure` image status.

The repository includes test interfaces and local doubles for:

- broker transport;
- enrichment state/idempotency;
- database transaction runner;
- broker outbox;
- HTTP article page client;
- DNS/SSRF policy;
- HTML metadata parser;
- enrichment work handler.

Those local state, transaction, and outbox doubles are accepted only when `NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE=test`. Adapter identity is part of each durable dependency interface, and production consumer registration requires every durable adapter to identify as `production`.

The current DNS policy rejects protected literal and resolved addresses before the initial request and each redirect, but the HTTP connection does not yet pin the validated address through connection establishment. Closing that DNS-rebinding time-of-check/time-of-use gap is deferred and required before this fetch path can be considered production-ready. The unavailable durable production adapters keep the consumer disabled in the meantime.

The repository does not implement identity, canonical dedupe, AI decisioning, approval, translation, persistence, publication, or user-facing article publication.

## Development

```sh
export NODE_AUTH_TOKEN="<GitHub classic PAT with read:packages>"
npm ci
npm run ci
docker build --secret id=npm_token,env=NODE_AUTH_TOKEN -t nutsnews-worker-article-enrichment:local .
```

`npm run ci` runs linting, strict type checking, unit tests, integration tests, build, CycloneDX SBOM generation, and a production dependency audit.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

Backend deployments consume immutable SHA-tagged GHCR images. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for CI when package access is granted to this repository. Workflows use least-privilege permissions, request `packages: read` for package install jobs, and request `packages: write` only for image publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
