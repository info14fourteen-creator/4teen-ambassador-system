import { buildTronscanAddressUrl } from "../../../../../shared/config/contracts";

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

export function buildWalletExplorerUrl(wallet: string): string {
  return buildTronscanAddressUrl(assertNonEmpty(wallet, "wallet"));
}
