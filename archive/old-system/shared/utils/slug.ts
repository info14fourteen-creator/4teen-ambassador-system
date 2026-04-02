import {
  REFERRAL_SLUG_MAX_LENGTH,
  REFERRAL_SLUG_MIN_LENGTH,
  REFERRAL_SLUG_PATTERN
} from "../config/referral";

export function normalizeSlug(input: string): string {
  return String(input || "").trim().toLowerCase();
}

export function isValidSlug(slug: string): boolean {
  const normalized = normalizeSlug(slug);

  if (normalized.length < REFERRAL_SLUG_MIN_LENGTH) return false;
  if (normalized.length > REFERRAL_SLUG_MAX_LENGTH) return false;

  return REFERRAL_SLUG_PATTERN.test(normalized);
}

export function assertValidSlug(slug: string): string {
  const normalized = normalizeSlug(slug);

  if (!isValidSlug(normalized)) {
    throw new Error("Invalid referral slug");
  }

  return normalized;
}
