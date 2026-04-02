import { keccak_256 } from "@noble/hashes/sha3";
import { utf8ToBytes } from "@noble/hashes/utils";

export const ZERO_BYTES32 =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

export interface BuildAmbassadorRegistrationHashesInput {
  slug: string;
}

export interface AmbassadorRegistrationHashes {
  slug: string;
  slugHash: string;
  metaHash: string;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

export function normalizeSlug(value: string): string {
  return assertNonEmpty(value, "slug")
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, "");
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function keccakUtf8ToHex(value: string): string {
  const bytes = utf8ToBytes(String(value || ""));
  return `0x${bytesToHex(keccak_256(bytes))}`;
}

export function buildAmbassadorRegistrationHashes(
  input: BuildAmbassadorRegistrationHashesInput
): AmbassadorRegistrationHashes {
  const slug = normalizeSlug(input.slug);

  if (!slug) {
    throw new Error("slug is required");
  }

  return {
    slug,
    slugHash: keccakUtf8ToHex(slug),
    metaHash: ZERO_BYTES32
  };
}
