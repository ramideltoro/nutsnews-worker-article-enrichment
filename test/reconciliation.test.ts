import {
  describe,
  expect,
  it
} from "vitest";

import { createEnrichmentFailClosedReconciler } from "../src/reconciliation.js";
import { ManualEnrichmentClock } from "../src/test-doubles.js";

describe("enrichment reconciliation", () => {
  it("reports a bounded no-op dry-run when no service-owned replay candidates exist", async () => {
    const reconciler = createEnrichmentFailClosedReconciler(new ManualEnrichmentClock());

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      service: "enrichment",
      status: "dry_run",
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.errors).toEqual([]);
  });
});
