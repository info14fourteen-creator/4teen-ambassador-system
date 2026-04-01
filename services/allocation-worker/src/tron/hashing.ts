import crypto from "node:crypto";
import { keccak_256 } from "@noble/hashes/sha3";
import { utf8ToBytes } from "@noble/hashes/utils";

export interface PurchaseIdInput {
  txHash: string;
  buyerWallet: string;
}

export interface AttributionHashing {
  hashSlugToBytes32Hex(slug: string): string;
  derivePurchaseId(input: PurchaseIdInput): string;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function stripHexPrefix(value: string): string {
  return value.startsWith("0x") ? value.slice(2) : value;
}

function isHex(value: string): boolean {
  return /^[0-9a-fA-F]+$/.test(value);
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function toBytes32HexFromUtf8(value: string): string {
  const bytes = utf8ToBytes(value);
  const hash = keccak_256(bytes);
  return `0x${bytesToHex(hash)}`;
}

function normalizeSlugForHashing(slug: string): string {
  return assertNonEmpty(slug, "slug").toLowerCase();
}

function normalizeTxHash(txHash: string): string {
  const normalized = assertNonEmpty(txHash, "txHash").toLowerCase();
  const stripped = stripHexPrefix(normalized);

  if (!isHex(stripped)) {
    throw new Error("txHash must be a hex string");
  }

  return stripped;
}

function normalizeWalletForPurchaseId(wallet: string): string {
  return assertNonEmpty(wallet, "buyerWallet");
}

export function hashSlugToBytes32Hex(slug: string): string {
  return toBytes32HexFromUtf8(normalizeSlugForHashing(slug));
}

export function derivePurchaseId(input: PurchaseIdInput): string {
  const txHash = normalizeTxHash(input.txHash);
  const buyerWallet = normalizeWalletForPurchaseId(input.buyerWallet).toLowerCase();

  return `0x${crypto
    .createHash("sha256")
    .update(`${txHash}:${buyerWallet}`)
    .digest("hex")}`;
}

export class TronHashing implements AttributionHashing {
  hashSlugToBytes32Hex(slug: string): string {
    return hashSlugToBytes32Hex(slug);
  }

  derivePurchaseId(input: PurchaseIdInput): string {
    return derivePurchaseId(input);
  }
}
