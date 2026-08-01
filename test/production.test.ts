import {
  describe,
  expect,
  it
} from "vitest";

import {
  DefaultEnrichmentDnsPolicy,
  InMemoryEnrichmentBodyStore,
  SimpleEnrichmentHtmlParser,
  createProtectedAddressLookup
} from "../src/production.js";

describe("production enrichment dependencies", () => {
  it("extracts metadata and image candidates from Open Graph HTML", async () => {
    const bodyStore = new InMemoryEnrichmentBodyStore();
    const parser = new SimpleEnrichmentHtmlParser(bodyStore);
    const bodyRef = bodyStore.put({
      finalUrl: "https://backend.nutsnews.com/__worker-fixtures/story",
      mediaType: "text/html",
      body: `<!doctype html>
        <html lang="en">
          <head>
            <title>Fixture title fallback</title>
            <link rel="canonical" href="https://backend.nutsnews.com/story">
            <meta property="og:title" content="Fixture students build library boxes">
            <meta name="description" content="A concise fixture description.">
            <meta property="og:image" content="/fixtures/story.jpg?utm_source=feed">
          </head>
        </html>`
    });

    await expect(parser.parse({
      canonicalArticleId: "article-fixture",
      finalUrl: "https://backend.nutsnews.com/__worker-fixtures/story",
      timeoutMs: 5_000,
      maxDomNodes: 100,
      htmlRef: bodyRef
    })).resolves.toMatchObject({
      canonicalUrl: "https://backend.nutsnews.com/story",
      title: "Fixture students build library boxes",
      description: "A concise fixture description.",
      language: "en",
      imageCandidates: [
        {
          url: "/fixtures/story.jpg?utm_source=feed",
          source: "open_graph"
        }
      ]
    });
  });

  it("blocks private literal addresses in production DNS policy", async () => {
    const policy = new DefaultEnrichmentDnsPolicy();

    await expect(policy.checkUrl("http://169.254.169.254/latest/meta-data")).resolves.toEqual({
      allowed: false,
      reason: "metadata-address"
    });
  });

  it("fails the socket lookup before connect when DNS resolves to a protected address", async () => {
    const lookup = createProtectedAddressLookup((_hostname, callback) => {
      callback(null, [{ address: "169.254.169.254", family: 4 }]);
    });

    const error = await new Promise<NodeJS.ErrnoException | null>((resolve) => {
      lookup("attacker.example", { all: false }, (lookupError) => {
        resolve(lookupError);
      });
    });

    expect(error).toMatchObject({ code: "EACCES" });
  });
});
