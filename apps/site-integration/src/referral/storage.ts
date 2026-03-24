import { assertValidSlug, normalizeSlug } from "../../../../shared/utils/slug";

export const REFERRAL_STORAGE_KEY = "fourteen_referral_first_touch_v1";
export const DEFAULT_REFERRAL_TTL_MS = 30 * 24 * 60 * 60 * 1000;

export interface StoredReferralRecord {
  slug: string;
  capturedAt: number;
  expiresAt: number;
  source: "query";
}

export interface SaveReferralInput {
  slug: string;
  now?: number;
  ttlMs?: number;
}

function isBrowser(): boolean {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

function safeParseRecord(raw: string | null): StoredReferralRecord | null {
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw) as Partial<StoredReferralRecord>;

    if (!parsed || typeof parsed !== "object") return null;
    if (typeof parsed.slug !== "string") return null;
    if (typeof parsed.capturedAt !== "number") return null;
    if (typeof parsed.expiresAt !== "number") return null;
    if (parsed.source !== "query") return null;

    const normalizedSlug = normalizeSlug(parsed.slug);
    const safeSlug = assertValidSlug(normalizedSlug);

    return {
      slug: safeSlug,
      capturedAt: parsed.capturedAt,
      expiresAt: parsed.expiresAt,
      source: "query"
    };
  } catch {
    return null;
  }
}

export function getStoredReferralRaw(): StoredReferralRecord | null {
  if (!isBrowser()) return null;

  const raw = window.localStorage.getItem(REFERRAL_STORAGE_KEY);
  return safeParseRecord(raw);
}

export function isReferralExpired(record: StoredReferralRecord, now = Date.now()): boolean {
  return record.expiresAt <= now;
}

export function getStoredReferral(now = Date.now()): StoredReferralRecord | null {
  const record = getStoredReferralRaw();

  if (!record) return null;

  if (isReferralExpired(record, now)) {
    clearStoredReferral();
    return null;
  }

  return record;
}

export function saveReferral(input: SaveReferralInput): StoredReferralRecord {
  if (!isBrowser()) {
    throw new Error("Referral storage is only available in the browser");
  }

  const now = input.now ?? Date.now();
  const ttlMs = input.ttlMs ?? DEFAULT_REFERRAL_TTL_MS;
  const slug = assertValidSlug(input.slug);

  if (ttlMs <= 0) {
    throw new Error("Referral TTL must be greater than zero");
  }

  const record: StoredReferralRecord = {
    slug,
    capturedAt: now,
    expiresAt: now + ttlMs,
    source: "query"
  };

  window.localStorage.setItem(REFERRAL_STORAGE_KEY, JSON.stringify(record));

  return record;
}

export function clearStoredReferral(): void {
  if (!isBrowser()) return;
  window.localStorage.removeItem(REFERRAL_STORAGE_KEY);
}
