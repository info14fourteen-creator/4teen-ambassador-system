export const SLUG_MIN_LENGTH = 3;
export const SLUG_MAX_LENGTH = 64;

const SLUG_PATTERN = /^[a-z0-9](?:[a-z0-9-]{1,62}[a-z0-9])?$/;

export function normalizeSlug(input: string): string {
  return String(input || "").trim().toLowerCase();
}

export function isValidSlug(slug: string): boolean {
  const normalized = normalizeSlug(slug);

  if (normalized.length < SLUG_MIN_LENGTH) return false;
  if (normalized.length > SLUG_MAX_LENGTH) return false;

  return SLUG_PATTERN.test(normalized);
}

export function assertValidSlug(slug: string): string {
  const normalized = normalizeSlug(slug);

  if (!isValidSlug(normalized)) {
    throw new Error("Invalid referral slug");
  }

  return normalized;
}
