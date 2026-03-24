export const TRON_NETWORK = "mainnet";

export const FOURTEEN_CONTROLLER_CONTRACT = "TF8yhohRfMxsdVRr7fFrYLh5fxK8sAFkeZ";
export const FOURTEEN_TOKEN_CONTRACT = "TMLXiCW2ZAkvjmn79ZXa4vdHX5BE3n9x4A";

export const TRONSCAN_BASE_URL = "https://tronscan.org/#";
export const TRONSCAN_ADDRESS_URL = `${TRONSCAN_BASE_URL}/address`;
export const TRONSCAN_TRANSACTION_URL = `${TRONSCAN_BASE_URL}/transaction`;

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

export function buildTronscanAddressUrl(address: string): string {
  return `${TRONSCAN_ADDRESS_URL}/${assertNonEmpty(address, "address")}`;
}

export function buildTronscanTransactionUrl(txid: string): string {
  return `${TRONSCAN_TRANSACTION_URL}/${assertNonEmpty(txid, "txid")}`;
}
