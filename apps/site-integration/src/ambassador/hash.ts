import { keccak_256 } from "@noble/hashes/sha3";
import { utf8ToBytes } from "@noble/hashes/utils";
import { assertValidSlug, normalizeSlug } from "../../../../shared/utils/slug";

export interface AmbassadorRegistrationHashes {
  slug: string;
  slugHash: string;
  metaHash: string;
}

export interface BuildAmbassadorRegistrationHashesInput {
  slug: string;
  meta?: string | null;
}

const ZERO_BYTES32 = `0x${"0".repeat(64)}`;

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function keccakUtf8ToBytes32Hex(value: string): string {
  const hash = keccak_256(utf8ToBytes(value));
  return `0x${bytesToHex(hash)}`;
}

function normalizeMeta(value: string | null | undefined): string | null {
  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized || null;
}

export function buildAmbassadorRegistrationHashes(
  input: BuildAmbassadorRegistrationHashesInput
): AmbassadorRegistrationHashes {
  const slug = assertValidSlug(normalizeSlug(input.slug));
  const meta = normalizeMeta(input.meta);

  return {
    slug,
    slugHash: keccakUtf8ToBytes32Hex(slug),
    metaHash: meta ? keccakUtf8ToBytes32Hex(meta) : ZERO_BYTES32
  };
}

export function getZeroBytes32(): string {
  return ZERO_BYTES32;
}
