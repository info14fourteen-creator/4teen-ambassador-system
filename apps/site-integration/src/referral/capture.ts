import { isValidSlug, normalizeSlug } from "../../../../shared/utils/slug";
import { DEFAULT_REFERRAL_TTL_MS, StoredReferralRecord, getStoredReferral } from "./storage";
import { applyFirstTouch, ApplyFirstTouchResult } from "./firstTouch";

export const REFERRAL_QUERY_PARAM = "r";

export interface CaptureReferralOptions {
  search?: string;
  ttlMs?: number;
  now?: number;
}

export interface CaptureReferralResult {
  foundSlug: string | null;
  applied: ApplyFirstTouchResult | null;
  activeReferral: StoredReferralRecord | null;
}

export function readReferralSlugFromSearch(search?: string): string | null {
  const rawSearch =
    typeof search === "string"
      ? search
      : typeof window !== "undefined"
        ? window.location.search
        : "";

  const params = new URLSearchParams(rawSearch);
  const rawSlug = params.get(REFERRAL_QUERY_PARAM);

  if (!rawSlug) return null;

  const normalized = normalizeSlug(rawSlug);

  if (!isValidSlug(normalized)) return null;

  return normalized;
}

export function captureReferralFromUrl(options: CaptureReferralOptions = {}): CaptureReferralResult {
  const now = options.now ?? Date.now();
  const ttlMs = options.ttlMs ?? DEFAULT_REFERRAL_TTL_MS;

  const foundSlug = readReferralSlugFromSearch(options.search);

  if (!foundSlug) {
    return {
      foundSlug: null,
      applied: null,
      activeReferral: getStoredReferral(now)
    };
  }

  const applied = applyFirstTouch({
    slug: foundSlug,
    ttlMs,
    now
  });

  return {
    foundSlug,
    applied,
    activeReferral: applied.record
  };
}
