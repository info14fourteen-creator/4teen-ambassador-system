export const REFERRAL_QUERY_PARAM = "r";
export const REFERRAL_STORAGE_KEY = "fourteen_referral_first_touch_v1";

export const REFERRAL_TTL_DAYS = 30;
export const REFERRAL_TTL_MS = REFERRAL_TTL_DAYS * 24 * 60 * 60 * 1000;

export const REFERRAL_SLUG_MIN_LENGTH = 3;
export const REFERRAL_SLUG_MAX_LENGTH = 64;

export const REFERRAL_SLUG_PATTERN = /^[a-z0-9](?:[a-z0-9-]{1,62}[a-z0-9])?$/;
