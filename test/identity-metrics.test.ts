import {
  describe,
  expect,
  it
} from "vitest";

import { ENRICHMENT_CONFIG_SCHEMA } from "../src/config.js";
import { createEnrichmentPrometheusTelemetrySink } from "../src/metrics.js";

const BUILD_REVISION = "0123456789abcdef0123456789abcdef01234567";

describe("enrichment immutable telemetry identity", () => {
  it("exports exactly one expected-active, build, and deployment series with truthful shadow identity", () => {
    const output = createEnrichmentPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-enrichment",
        version: "0.1.0",
        environment: "production",
        host: "backend-vps",
        revision: BUILD_REVISION,
        deployment: "shadow",
        adapter: "mixed"
      }
    }).collect();
    const identitySamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_build_info{")
      || line.startsWith("nutsnews_worker_deployment_info{"));
    const expectedActiveSamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_expected_active{"));

    expect(identitySamples).toHaveLength(2);
    expect(expectedActiveSamples).toEqual([
      'nutsnews_worker_expected_active{environment="production",service="enrichment"} 0'
    ]);
    expect(identitySamples.join("\n")).toContain(`revision="${BUILD_REVISION}"`);
    expect(identitySamples.join("\n")).toContain('deployment="shadow"');
    expect(identitySamples.join("\n")).toContain('adapter="mixed"');
    expect(identitySamples.join("\n")).not.toContain("unknown");
  });

  it("declares the immutable revision as required and non-sensitive in production", () => {
    expect(ENRICHMENT_CONFIG_SCHEMA.find((variable) => variable.name === "NUTSNEWS_ENRICHMENT_BUILD_REVISION")).toMatchObject({
      requiredInProduction: true,
      sensitive: false
    });
  });
});
