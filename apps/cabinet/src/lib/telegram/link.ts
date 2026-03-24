import { buildTronscanAddressUrl } from "../../../../../shared/config/contracts";

export const TELEGRAM_BOT_USERNAME = "fourteen_ambassador_bot";

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

export function buildTelegramBotLink(startParam?: string): string {
  const botUsername = assertNonEmpty(TELEGRAM_BOT_USERNAME, "TELEGRAM_BOT_USERNAME");

  if (!startParam) {
    return `https://t.me/${botUsername}`;
  }

  return `https://t.me/${botUsername}?start=${encodeURIComponent(startParam)}`;
}

export function buildTelegramBindStartParam(wallet: string): string {
  return `bind_${assertNonEmpty(wallet, "wallet")}`;
}

export function buildTelegramBindLink(wallet: string): string {
  return buildTelegramBotLink(buildTelegramBindStartParam(wallet));
}

export function buildReferralSharePath(slug: string): string {
  const safeSlug = assertNonEmpty(slug, "slug");
  return `/?r=${encodeURIComponent(safeSlug)}`;
}

export function buildReferralShareUrl(slug: string, origin?: string): string {
  const path = buildReferralSharePath(slug);

  if (origin && String(origin).trim()) {
    return `${String(origin).replace(/\/+$/, "")}${path}`;
  }

  if (typeof window !== "undefined" && window.location?.origin) {
    return `${window.location.origin}${path}`;
  }

  return path;
}

export function buildWalletExplorerUrl(wallet: string): string {
  return buildTronscanAddressUrl(assertNonEmpty(wallet, "wallet"));
}
