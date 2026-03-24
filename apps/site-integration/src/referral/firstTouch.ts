import {
  DEFAULT_REFERRAL_TTL_MS,
  StoredReferralRecord,
  getStoredReferral,
  saveReferral
} from "./storage";
import { assertValidSlug } from "../../../../shared/utils/slug";

export interface ApplyFirstTouchInput {
  slug: string;
  ttlMs?: number;
  now?: number;
}

export interface ApplyFirstTouchResult {
  status: "stored" | "kept-existing";
  record: StoredReferralRecord;
}

export function applyFirstTouch(input: ApplyFirstTouchInput): ApplyFirstTouchResult {
  const now = input.now ?? Date.now();
  const ttlMs = input.ttlMs ?? DEFAULT_REFERRAL_TTL_MS;
  const incomingSlug = assertValidSlug(input.slug);

  const existing = getStoredReferral(now);

  if (existing) {
    return {
      status: "kept-existing",
      record: existing
    };
  }

  const record = saveReferral({
    slug: incomingSlug,
    now,
    ttlMs
  });

  return {
    status: "stored",
    record
  };
}
