# nutsnews-worker-article-enrichment

Deployable worker-uplift article enrichment service shell for NutsNews.

## Responsibility

Own the enrichment service boundary that consumes contracted enrichment-stage messages, prepares for bounded article page metadata extraction, and keeps enrichment output shadow-safe.

Issue #101 bootstraps the deployable shell, not the metadata extraction business logic. The repository now provides value-free configuration, health and metrics endpoints, graceful drain, exact contracts/runtime dependencies, container publishing, and injectable HTTP, DNS policy, HTML parser, durable state, and broker interfaces.

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

- `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- `@ramideltoro/nutsnews-worker-runtime@0.4.0`

Local and CI installs use the owner-scoped GitHub Packages npm registry. No package token value is committed.

## Configuration

The value-free configuration schema lives in `src/config.ts` and is exposed at `/config-schema`. Production deployments must provide dependency values through backend-owned deployment configuration, not this repository.

Important variables:

- `NUTSNEWS_ENRICHMENT_DEPENDENCY_MODE`: `test` or `production`
- `NUTSNEWS_ENRICHMENT_DATABASE_URL`
- `NUTSNEWS_ENRICHMENT_RABBITMQ_URL`
- `NUTSNEWS_ENRICHMENT_CONCURRENCY`
- `NUTSNEWS_ENRICHMENT_PREFETCH`
- `NUTSNEWS_ENRICHMENT_CONNECT_TIMEOUT_MS`
- `NUTSNEWS_ENRICHMENT_READ_TIMEOUT_MS`
- `NUTSNEWS_ENRICHMENT_TOTAL_TIMEOUT_MS`
- `NUTSNEWS_ENRICHMENT_MAX_RESPONSE_BYTES`
- `NUTSNEWS_ENRICHMENT_MAX_REDIRECTS`
- `NUTSNEWS_ENRICHMENT_SHADOW_MODE`

`NUTSNEWS_ENRICHMENT_SHADOW_MODE` must remain `true` until backend-owned cutover work explicitly changes the deployment contract.

## Service Boundary

The service registers the contracted `enrichment` consumer route and downstream `approval` publish route through the shared runtime broker lifecycle. The message processor validates worker envelopes and enrichment-stage payloads, applies the durable idempotency interface, delegates work to the injected enrichment handler, and drains in-flight deliveries during shutdown.

The repository includes test interfaces and local doubles for:

- broker transport;
- enrichment state/idempotency;
- database transaction runner;
- broker outbox;
- HTTP article page client;
- DNS/SSRF policy;
- HTML metadata parser;
- enrichment work handler.

The repository does not implement identity, canonical dedupe, AI decisioning, approval, translation, persistence, publication, or production page-fetch business logic in this bootstrap issue.

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
