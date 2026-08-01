import { readFile } from "node:fs/promises";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import { getRuntimePackageMetadata } from "@ramideltoro/nutsnews-worker-runtime";
import { describe, expect, it } from "vitest";

import {
  SUPPORTED_CONTRACTS_PACKAGE_VERSION,
  SUPPORTED_RUNTIME_PACKAGE_VERSION
} from "../src/index.js";

describe("package compatibility", () => {
  it("accepts the exact contracts/runtime 1.0 release pair", () => {
    const contracts = getContractPackageMetadata();
    const runtime = getRuntimePackageMetadata();

    expect(contracts.packageVersion).toBe(SUPPORTED_CONTRACTS_PACKAGE_VERSION);
    expect(runtime.packageVersion).toBe(SUPPORTED_RUNTIME_PACKAGE_VERSION);
    expect(runtime.contractsPackageVersion).toBe(SUPPORTED_CONTRACTS_PACKAGE_VERSION);
    expect(SUPPORTED_CONTRACTS_PACKAGE_VERSION).toBe("1.0.0");
    expect(SUPPORTED_RUNTIME_PACKAGE_VERSION).toBe("1.0.0");
  });

  it("locks one exact published package pair with immutable registry integrity and no override", async () => {
    const [manifest, lock] = await Promise.all([
      readJson<PackageManifest>("../package.json"),
      readJson<PackageLock>("../package-lock.json")
    ]);
    const contractsPath = "node_modules/@ramideltoro/nutsnews-worker-contracts";
    const runtimePath = "node_modules/@ramideltoro/nutsnews-worker-runtime";
    const contracts = lock.packages[contractsPath];
    const runtime = lock.packages[runtimePath];
    const contractsCopies = Object.keys(lock.packages).filter((path) => path === contractsPath
      || path.endsWith(`/node_modules/@ramideltoro/nutsnews-worker-contracts`));

    expect(manifest.dependencies).toMatchObject({
      "@ramideltoro/nutsnews-worker-contracts": "1.0.0",
      "@ramideltoro/nutsnews-worker-runtime": "1.0.0"
    });
    expect(Object.hasOwn(manifest, "overrides")).toBe(false);
    expect(lock.packages[""]?.dependencies).toEqual(manifest.dependencies);
    expect(contractsCopies).toEqual([
      contractsPath
    ]);
    expect(contracts).toMatchObject({
      version: "1.0.0"
    });
    expect(contracts?.resolved).toMatch(/^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-contracts\/1\.0\.0\/[0-9a-f]{40}$/u);
    expect(contracts?.integrity).toMatch(/^sha512-[A-Za-z0-9+/]+={0,2}$/u);
    expect(runtime?.version).toBe("1.0.0");
    expect(runtime?.dependencies?.["@ramideltoro/nutsnews-worker-contracts"]).toBe("1.0.0");
    expect(runtime?.resolved).toMatch(/^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-runtime\/1\.0\.0\/[0-9a-f]{40}$/u);
    expect(runtime?.integrity).toMatch(/^sha512-[A-Za-z0-9+/]+={0,2}$/u);
  });
});

interface PackageManifest {
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly overrides?: unknown;
}

interface PackageLockEntry {
  readonly version?: string;
  readonly resolved?: string;
  readonly integrity?: string;
  readonly dependencies?: Readonly<Record<string, string>>;
}

interface PackageLock {
  readonly packages: Readonly<Record<string, PackageLockEntry>>;
}

async function readJson<T>(path: string): Promise<T> {
  return JSON.parse(await readFile(new URL(path, import.meta.url), "utf8")) as T;
}
