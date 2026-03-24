import { REFERRAL_STORAGE_KEY } from "../../../../../shared/config/referral";
import { assertValidSlug, normalizeSlug } from "../../../../../shared/utils/slug";

export interface CabinetStoredReferralRecord {
  slug: string;
  capturedAt: number;
  expiresAt: number;
  source: "query";
}

function isBrowser(): boolean {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

function safeParseReferral(raw: string | null): CabinetStoredReferralRecord | null {
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as Partial<CabinetStoredReferralRecord>;

    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    if (typeof parsed.slug !== "string") {
      return null;
    }

    if (typeof parsed.capturedAt !== "number") {
      return null;
    }

    if (typeof parsed.expiresAt !== "number") {
      return null;
    }

    if (parsed.source !== "query") {
      return null;
    }

    const slug = assertValidSlug(normalizeSlug(parsed.slug));

    return {
      slug,
      capturedAt: parsed.capturedAt,
      expiresAt: parsed.expiresAt,
      source: "query"
    };
  } catch {
    return null;
  }
}

export function getStoredReferralRaw(): CabinetStoredReferralRecord | null {
  if (!isBrowser()) {
    return null;
  }

  return safeParseReferral(window.localStorage.getItem(REFERRAL_STORAGE_KEY));
}

export function isStoredReferralExpired(
  record: CabinetStoredReferralRecord,
  now = Date.now()
): boolean {
  return record.expiresAt <= now;
}

export function getStoredReferral(now = Date.now()): CabinetStoredReferralRecord | null {
  const record = getStoredReferralRaw();

  if (!record) {
    return null;
  }

  if (isStoredReferralExpired(record, now)) {
    clearStoredReferral();
    return null;
  }

  return record;
}

export function getStoredReferralSlug(now = Date.now()): string | null {
  const record = getStoredReferral(now);
  return record?.slug ?? null;
}

export function clearStoredReferral(): void {
  if (!isBrowser()) {
    return;
  }

  window.localStorage.removeItem(REFERRAL_STORAGE_KEY);
}
