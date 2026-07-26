import {
  describe,
  expect,
  it
} from "vitest";

import { createEnrichmentFailClosedReconciler } from "../src/reconciliation.js";
import { ManualEnrichmentClock } from "../src/test-doubles.js";

describe("enrichment reconciliation", () => {
  it("fails closed instead of synthesizing approval requests from partial metadata", async () => {
    const reconciler = createEnrichmentFailClosedReconciler(new ManualEnrichmentClock());

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      service: "enrichment",
      status: "failed_closed",
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.errors[0]).toContain("refusing to synthesize");
  });
});
