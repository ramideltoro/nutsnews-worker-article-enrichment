import dns from "node:dns/promises";
import net from "node:net";
import { TextDecoder } from "node:util";

import type {
  EnrichmentDependencyProbe,
  EnrichmentDnsPolicy,
  EnrichmentDnsPolicyDecision,
  EnrichmentHtmlParseInput,
  EnrichmentHtmlParser,
  EnrichmentHttpClient,
  EnrichmentHttpFetchRequest,
  EnrichmentHttpFetchResponse,
  EnrichmentImageCandidate,
  EnrichmentParsedMetadata
} from "./dependencies.js";
import { sha256Hex } from "./ids.js";

interface StoredHtmlBody {
  readonly body: string;
  readonly mediaType: string;
}

interface HtmlTag {
  readonly name: string;
  readonly attrs: Readonly<Record<string, string>>;
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export class EnrichmentHttpError extends Error {
  readonly reason: string;
  readonly retryable: boolean;
  readonly retryAfterMs?: number;

  constructor(
    reason: string,
    message: string,
    options: {
      readonly retryable: boolean;
      readonly retryAfterMs?: number;
    }
  ) {
    super(message);
    this.name = "EnrichmentHttpError";
    this.reason = reason;
    this.retryable = options.retryable;

    if (options.retryAfterMs !== undefined) {
      this.retryAfterMs = options.retryAfterMs;
    }
  }
}

export class InMemoryEnrichmentBodyStore {
  private readonly bodies = new Map<string, StoredHtmlBody>();

  put(input: {
    readonly finalUrl: string;
    readonly body: string;
    readonly mediaType: string;
  }): EnrichmentHttpFetchResponse["bodyRef"] {
    const uri = `backend://worker-uplift/enrichment/http/${encodeURIComponent(input.finalUrl)}/${sha256Hex(input.body)}`;

    this.bodies.set(uri, {
      body: input.body,
      mediaType: input.mediaType
    });

    return {
      kind: "backend-record",
      uri,
      mediaType: input.mediaType
    };
  }

  get(uri: string): StoredHtmlBody | undefined {
    return this.bodies.get(uri);
  }
}

export class NodeEnrichmentHttpClient implements EnrichmentHttpClient {
  readonly name = "node-enrichment-http-client";

  private readonly bodyStore: InMemoryEnrichmentBodyStore;
  private readonly fetchImpl: FetchLike;
  private readonly redirectDnsPolicy: EnrichmentDnsPolicy;

  constructor(options: {
    readonly bodyStore: InMemoryEnrichmentBodyStore;
    readonly fetch?: FetchLike;
    readonly redirectDnsPolicy?: EnrichmentDnsPolicy;
  }) {
    this.bodyStore = options.bodyStore;
    this.fetchImpl = options.fetch ?? fetch;
    this.redirectDnsPolicy = options.redirectDnsPolicy ?? new DefaultEnrichmentDnsPolicy();
  }

  probe(): EnrichmentDependencyProbe {
    return {
      status: "ok",
      summary: "node enrichment HTTP client ready"
    };
  }

  async fetch(request: EnrichmentHttpFetchRequest): Promise<EnrichmentHttpFetchResponse> {
    const deadlineAt = Date.now() + request.totalTimeoutMs;
    let currentUrl = new URL(request.url);
    const redirects: { readonly url: string }[] = [];

    for (;;) {
      const remainingTotalTimeoutMs = deadlineAt - Date.now();

      if (remainingTotalTimeoutMs <= 0) {
        throw new EnrichmentHttpError("total-timeout", "Enrichment fetch total timeout exceeded.", {
          retryable: true
        });
      }

      const response = await this.fetchOnce(currentUrl.toString(), Math.min(request.connectTimeoutMs, remainingTotalTimeoutMs));

      if (isRedirect(response.status)) {
        const location = response.headers.get("location");

        if (location === null) {
          throw new EnrichmentHttpError("redirect-location-missing", "Enrichment redirect response did not include a Location header.", {
            retryable: false
          });
        }

        if (redirects.length >= request.maxRedirects) {
          throw new EnrichmentHttpError("redirect-limit-exceeded", "Enrichment redirect limit exceeded.", {
            retryable: false
          });
        }

        const nextUrl = new URL(location, currentUrl);

        await this.assertRedirectAllowed(nextUrl.toString());

        redirects.push({
          url: nextUrl.toString()
        });
        currentUrl = nextUrl;
        continue;
      }

      const contentLength = parseContentLength(response.headers.get("content-length"));

      if (contentLength !== undefined && contentLength > request.maxResponseBytes) {
        throw new EnrichmentHttpError("response-too-large", "Enrichment response declared a body larger than the configured byte limit.", {
          retryable: false
        });
      }

      const body = await readBody(response, Math.min(request.readTimeoutMs, Math.max(1, deadlineAt - Date.now())));

      if (body.byteLength > request.maxDecompressedBytes || body.byteLength > request.maxResponseBytes) {
        throw new EnrichmentHttpError("response-too-large", "Enrichment response exceeded the configured byte limit.", {
          retryable: false
        });
      }

      if (contentLength !== undefined && contentLength > 0 && body.byteLength / contentLength > request.maxDecompressionRatio) {
        throw new EnrichmentHttpError("decompression-ratio-exceeded", "Enrichment response exceeded the configured decompression ratio.", {
          retryable: false
        });
      }

      const mediaType = mediaTypeFromHeaders(response.headers);
      const bodyText = new TextDecoder("utf-8", {
        fatal: false
      }).decode(body);
      const bodyRef = this.bodyStore.put({
        finalUrl: currentUrl.toString(),
        body: bodyText,
        mediaType
      });

      return {
        finalUrl: currentUrl.toString(),
        statusCode: response.status,
        headers: responseHeaders(response.headers),
        bodyBytes: body.byteLength,
        ...(contentLength === undefined ? {} : {
          compressedBytes: contentLength
        }),
        decompressedBytes: body.byteLength,
        encodingValid: true,
        redirects,
        bodyRef
      };
    }
  }

  private async fetchOnce(url: string, timeoutMs: number): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
    }, timeoutMs);

    try {
      return await this.fetchImpl(url, {
        headers: {
          "accept": "text/html, application/xhtml+xml;q=0.9, */*;q=0.1",
          "user-agent": "NutsNewsWorkerEnrichment/0.1"
        },
        redirect: "manual",
        signal: controller.signal
      });
    } catch (error: unknown) {
      if (error instanceof Error && error.name === "AbortError") {
        throw new EnrichmentHttpError("connect-timeout", "Enrichment fetch connection timeout exceeded.", {
          retryable: true
        });
      }

      throw new EnrichmentHttpError("network-error", "Enrichment fetch failed before a response was received.", {
        retryable: true
      });
    } finally {
      clearTimeout(timeout);
    }
  }

  private async assertRedirectAllowed(url: string): Promise<void> {
    const decision = await this.redirectDnsPolicy.checkUrl(url);

    if (!decision.allowed) {
      throw new EnrichmentHttpError(`redirect-dns-policy:${decision.reason}`, "Enrichment redirect target failed network policy.", {
        retryable: false
      });
    }
  }
}

export class DefaultEnrichmentDnsPolicy implements EnrichmentDnsPolicy {
  readonly name = "default-enrichment-dns-policy";

  probe(): EnrichmentDependencyProbe {
    return {
      status: "ok",
      summary: "default enrichment DNS policy ready"
    };
  }

  async checkUrl(value: string): Promise<EnrichmentDnsPolicyDecision> {
    let url: URL;

    try {
      url = new URL(value);
    } catch {
      return {
        allowed: false,
        reason: "dns-error"
      };
    }

    if (url.protocol !== "https:" && url.protocol !== "http:") {
      return {
        allowed: false,
        reason: "unsupported-scheme"
      };
    }

    const hostname = normalizeHostname(url.hostname);
    const lowerHostname = hostname.toLowerCase();

    if (lowerHostname === "localhost" || lowerHostname.endsWith(".localhost")) {
      return {
        allowed: false,
        reason: "loopback-address"
      };
    }

    if (lowerHostname === "metadata.google.internal" || lowerHostname.endsWith(".metadata.google.internal")) {
      return {
        allowed: false,
        reason: "metadata-address"
      };
    }

    const literalReason = protectedAddressReason(hostname);

    if (literalReason !== undefined) {
      return {
        allowed: false,
        reason: literalReason
      };
    }

    try {
      const addresses = await dns.lookup(hostname, {
        all: true,
        verbatim: true
      });
      const blockedReason = addresses.map((address) => protectedAddressReason(address.address)).find((reason) => reason !== undefined);

      if (blockedReason !== undefined) {
        return {
          allowed: false,
          reason: blockedReason
        };
      }
    } catch {
      return {
        allowed: false,
        reason: "dns-error"
      };
    }

    return {
      allowed: true,
      reason: "allowed"
    };
  }
}

export class SimpleEnrichmentHtmlParser implements EnrichmentHtmlParser {
  readonly name = "simple-enrichment-html-parser";

  private readonly bodyStore: InMemoryEnrichmentBodyStore;

  constructor(bodyStore: InMemoryEnrichmentBodyStore) {
    this.bodyStore = bodyStore;
  }

  probe(): EnrichmentDependencyProbe {
    return {
      status: "ok",
      summary: "simple enrichment HTML parser ready"
    };
  }

  parse(input: EnrichmentHtmlParseInput): Promise<EnrichmentParsedMetadata> {
    const stored = this.bodyStore.get(input.htmlRef.uri);

    if (stored === undefined) {
      throw new Error("Enrichment HTML body is missing from the production body store.");
    }

    const tags = htmlTags(stored.body, input.maxDomNodes);
    const canonicalUrl = linkHref(tags, "canonical");
    const title = firstDefined(
      metaContent(tags, [
        "og:title",
        "twitter:title"
      ]),
      titleText(stored.body)
    );
    const description = metaContent(tags, [
      "description",
      "og:description",
      "twitter:description"
    ]);
    const publishedAt = metaContent(tags, [
      "article:published_time",
      "date",
      "dc.date"
    ]);
    const language = htmlLanguage(tags);

    return Promise.resolve({
      ...(canonicalUrl === undefined ? {} : {
        canonicalUrl
      }),
      ...(title === undefined ? {} : {
        title
      }),
      ...(description === undefined ? {} : {
        description
      }),
      ...(publishedAt === undefined ? {} : {
        publishedAt
      }),
      ...(language === undefined ? {} : {
        language
      }),
      imageCandidates: imageCandidates(tags)
    });
  }
}

function isRedirect(statusCode: number): boolean {
  return statusCode === 301 || statusCode === 302 || statusCode === 303 || statusCode === 307 || statusCode === 308;
}

async function readBody(response: Response, timeoutMs: number): Promise<Uint8Array> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  try {
    return new Uint8Array(await Promise.race([
      response.arrayBuffer(),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => {
          reject(new EnrichmentHttpError("read-timeout", "Enrichment response read timeout exceeded.", {
            retryable: true
          }));
        }, timeoutMs);
      })
    ]));
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

function parseContentLength(value: string | null): number | undefined {
  if (value === null || value.trim().length === 0) {
    return undefined;
  }

  const parsed = Number(value);

  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function responseHeaders(headers: Headers): Readonly<Record<string, string>> {
  const output: Record<string, string> = {};

  for (const [key, value] of headers.entries()) {
    output[key.toLowerCase()] = value;
  }

  return output;
}

function mediaTypeFromHeaders(headers: Headers): string {
  return headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() ?? "application/octet-stream";
}

function htmlTags(html: string, maxDomNodes: number): readonly HtmlTag[] {
  const tags: HtmlTag[] = [];
  const tagRegex = /<([a-zA-Z][a-zA-Z0-9:-]*)\b([^>]*)>/gu;
  let match: RegExpExecArray | null;

  while ((match = tagRegex.exec(html)) !== null) {
    if (tags.length >= maxDomNodes) {
      throw new Error("Enrichment HTML parser DOM node budget exceeded.");
    }

    const name = match[1];
    const attrs = match[2];

    if (name === undefined || attrs === undefined) {
      continue;
    }

    tags.push({
      name: name.toLowerCase(),
      attrs: htmlAttributes(attrs)
    });
  }

  return tags;
}

function htmlAttributes(value: string): Readonly<Record<string, string>> {
  const attrs: Record<string, string> = {};
  const attrRegex = /([^\s"'<>/=]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+))/gu;
  let match: RegExpExecArray | null;

  while ((match = attrRegex.exec(value)) !== null) {
    const rawKey = match[1];

    if (rawKey === undefined) {
      continue;
    }

    attrs[rawKey.toLowerCase()] = decodeHtml(match[2] ?? match[3] ?? match[4] ?? "");
  }

  return attrs;
}

function metaContent(tags: readonly HtmlTag[], names: readonly string[]): string | undefined {
  const expected = names.map((name) => name.toLowerCase());

  for (const tag of tags) {
    if (tag.name !== "meta") {
      continue;
    }

    const key = tag.attrs.property ?? tag.attrs.name;
    const content = tag.attrs.content;

    if (key !== undefined && content !== undefined && expected.includes(key.toLowerCase())) {
      return cleanText(content, 2_048);
    }
  }

  return undefined;
}

function linkHref(tags: readonly HtmlTag[], rel: string): string | undefined {
  const expected = rel.toLowerCase();

  for (const tag of tags) {
    if (tag.name !== "link") {
      continue;
    }

    const relation = tag.attrs.rel;
    const href = tag.attrs.href;

    if (relation !== undefined && href !== undefined && relation.toLowerCase().split(/\s+/u).includes(expected)) {
      return href;
    }
  }

  return undefined;
}

function htmlLanguage(tags: readonly HtmlTag[]): string | undefined {
  const tag = tags.find((candidate) => candidate.name === "html");
  const language = tag?.attrs.lang ?? tag?.attrs["xml:lang"];
  const cleaned = language === undefined ? undefined : cleanText(language, 32);

  return cleaned?.toLowerCase();
}

function titleText(html: string): string | undefined {
  const match = /<title\b[^>]*>([\s\S]*?)<\/title>/iu.exec(html);
  const raw = match?.[1];

  return raw === undefined ? undefined : cleanText(decodeHtml(raw.replace(/<[^>]+>/gu, " ")), 512);
}

function imageCandidates(tags: readonly HtmlTag[]): readonly EnrichmentImageCandidate[] {
  const candidates: EnrichmentImageCandidate[] = [];

  for (const tag of tags) {
    if (tag.name === "meta") {
      appendMetaImageCandidate(candidates, tag);
      continue;
    }

    if (tag.name === "img") {
      appendHtmlImageCandidate(candidates, tag);
      continue;
    }

    if (tag.name === "source" && tag.attrs.srcset !== undefined) {
      appendSrcsetCandidates(candidates, tag.attrs.srcset);
    }
  }

  return dedupeImages(candidates);
}

function appendMetaImageCandidate(candidates: EnrichmentImageCandidate[], tag: HtmlTag): void {
  const key = tag.attrs.property ?? tag.attrs.name;
  const content = tag.attrs.content;

  if (key === undefined || content === undefined || content.trim().length === 0) {
    return;
  }

  const normalized = key.toLowerCase();

  if (normalized === "og:image" || normalized === "og:image:url" || normalized === "og:image:secure_url") {
    candidates.push({
      url: content,
      source: "open_graph"
    });
    return;
  }

  if (normalized === "twitter:image" || normalized === "twitter:image:src") {
    candidates.push({
      url: content,
      source: "twitter"
    });
  }
}

function appendHtmlImageCandidate(candidates: EnrichmentImageCandidate[], tag: HtmlTag): void {
  const src = tag.attrs.src;

  if (src !== undefined && src.trim().length > 0) {
    candidates.push({
      url: src,
      source: "html",
      ...optionalPositiveDimension("width", tag.attrs.width),
      ...optionalPositiveDimension("height", tag.attrs.height)
    });
  }

  if (tag.attrs.srcset !== undefined) {
    appendSrcsetCandidates(candidates, tag.attrs.srcset);
  }
}

function appendSrcsetCandidates(candidates: EnrichmentImageCandidate[], srcset: string): void {
  for (const entry of srcset.split(",")) {
    const [url, descriptor] = entry.trim().split(/\s+/u);

    if (url === undefined || url.length === 0) {
      continue;
    }

    const width = descriptor?.endsWith("w") === true ? Number(descriptor.slice(0, -1)) : undefined;

    candidates.push({
      url,
      source: "srcset",
      ...(width === undefined || !Number.isFinite(width) || width <= 0 ? {} : {
        width
      })
    });
  }
}

function optionalPositiveDimension(
  key: "width" | "height",
  value: string | undefined
): Partial<Pick<EnrichmentImageCandidate, "width" | "height">> {
  if (value === undefined) {
    return {};
  }

  const parsed = Number(value);

  return Number.isFinite(parsed) && parsed > 0 ? {
    [key]: parsed
  } : {};
}

function dedupeImages(candidates: readonly EnrichmentImageCandidate[]): readonly EnrichmentImageCandidate[] {
  const seen = new Set<string>();
  const output: EnrichmentImageCandidate[] = [];

  for (const candidate of candidates) {
    const key = `${candidate.source}:${candidate.url}`;

    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    output.push(candidate);
  }

  return output;
}

function cleanText(value: string, maxLength: number): string | undefined {
  const normalized = value.replace(/\s+/gu, " ").trim();

  if (normalized.length === 0) {
    return undefined;
  }

  return normalized.length > maxLength ? normalized.slice(0, maxLength) : normalized;
}

function decodeHtml(value: string): string {
  return value.replace(/&(amp|quot|#39|lt|gt);/giu, (entity) => {
    switch (entity.toLowerCase()) {
      case "&amp;":
        return "&";
      case "&quot;":
        return "\"";
      case "&#39;":
        return "'";
      case "&lt;":
        return "<";
      case "&gt;":
        return ">";
      default:
        return entity;
    }
  });
}

function firstDefined<T>(...values: readonly (T | undefined)[]): T | undefined {
  return values.find((value): value is T => value !== undefined);
}

function normalizeHostname(hostname: string): string {
  if (hostname.startsWith("[") && hostname.endsWith("]")) {
    return hostname.slice(1, -1);
  }

  return hostname;
}

function protectedAddressReason(address: string): EnrichmentDnsPolicyDecision["reason"] | undefined {
  if (net.isIPv4(address)) {
    return protectedIpv4Reason(address);
  }

  if (net.isIPv6(address)) {
    return protectedIpv6Reason(address);
  }

  return undefined;
}

function protectedIpv4Reason(address: string): EnrichmentDnsPolicyDecision["reason"] | undefined {
  const octets = address.split(".").map((part) => Number(part));
  const [first, second, third, fourth] = octets;

  if (first === undefined || second === undefined || third === undefined || fourth === undefined) {
    return "private-address";
  }

  if (first === 127) {
    return "loopback-address";
  }

  if (first === 169 && second === 254 && third === 169 && fourth === 254) {
    return "metadata-address";
  }

  if (first === 169 && second === 254) {
    return "link-local-address";
  }

  if (first === 10 ||
    (first === 172 && second >= 16 && second <= 31) ||
    (first === 192 && second === 168) ||
    (first === 100 && second >= 64 && second <= 127) ||
    (first === 198 && (second === 18 || second === 19)) ||
    first === 0) {
    return "private-address";
  }

  return undefined;
}

function protectedIpv6Reason(address: string): EnrichmentDnsPolicyDecision["reason"] | undefined {
  const normalized = address.toLowerCase();

  if (normalized === "::1") {
    return "loopback-address";
  }

  if (normalized === "fd00:ec2::254") {
    return "metadata-address";
  }

  if (normalized.startsWith("fe80:")) {
    return "link-local-address";
  }

  if (normalized.startsWith("fc") || normalized.startsWith("fd")) {
    return "private-address";
  }

  return undefined;
}
