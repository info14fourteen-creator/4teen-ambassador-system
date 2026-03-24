import { FOURTEEN_CONTROLLER_CONTRACT } from "../../../../../shared/config/contracts";

declare global {
  interface Window {
    tronWeb?: any;
    tronLink?: any;
  }
}

export interface ControllerContractMethods {
  getAmbassadorByWallet(wallet: string): Promise<any>;
  getAmbassadorBySlug(slug: string): Promise<any>;
  getAmbassadorStats(wallet: string): Promise<any>;
  getRewardSummary(wallet: string): Promise<any>;
  withdrawRewards(): Promise<any>;
}

export interface AmbassadorIdentity {
  wallet: string;
  slug: string;
  exists: boolean;
  telegramBound: boolean;
  telegramUsername: string;
}

export interface AmbassadorStats {
  totalReferrals: number;
  totalQualifiedPurchases: number;
  totalRewardSun: string;
  totalRewardTrx: string;
  totalWithdrawnSun: string;
  totalWithdrawnTrx: string;
  availableSun: string;
  availableTrx: string;
}

export interface RewardSummary {
  availableSun: string;
  availableTrx: string;
  withdrawnSun: string;
  withdrawnTrx: string;
  lifetimeSun: string;
  lifetimeTrx: string;
}

export interface WithdrawResult {
  txid: string;
}

function assertBrowser(): void {
  if (typeof window === "undefined") {
    throw new Error("Browser environment is required");
  }
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function safeNumber(value: any): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }

  return 0;
}

function safeString(value: any): string {
  if (value == null) return "0";
  return String(value);
}

function sunToTrxString(value: any): string {
  const raw = safeString(value);

  if (!raw || raw === "0") return "0";

  const negative = raw.startsWith("-");
  const digits = negative ? raw.slice(1) : raw;

  const padded = digits.padStart(7, "0");
  const whole = padded.slice(0, -6) || "0";
  const fraction = padded.slice(-6).replace(/0+$/, "");

  const result = fraction ? `${whole}.${fraction}` : whole;
  return negative ? `-${result}` : result;
}

async function getTronWeb(): Promise<any> {
  assertBrowser();

  const tronWeb = window.tronWeb;

  if (!tronWeb || !tronWeb.defaultAddress?.base58) {
    throw new Error("Tron wallet is not connected");
  }

  return tronWeb;
}

export async function getConnectedWalletAddress(): Promise<string> {
  const tronWeb = await getTronWeb();
  return assertNonEmpty(tronWeb.defaultAddress.base58, "wallet");
}

async function getControllerContractInstance(): Promise<any> {
  const tronWeb = await getTronWeb();
  return tronWeb.contract().at(FOURTEEN_CONTROLLER_CONTRACT);
}

function pickTupleValue(source: any, index: number, fallbackKey?: string): any {
  if (Array.isArray(source)) {
    return source[index];
  }

  if (source && typeof source === "object") {
    if (fallbackKey && fallbackKey in source) {
      return source[fallbackKey];
    }

    const values = Object.values(source);
    return values[index];
  }

  return undefined;
}

function mapAmbassadorIdentity(raw: any, wallet: string): AmbassadorIdentity {
  const slug = String(
    pickTupleValue(raw, 0, "slug") ??
    pickTupleValue(raw, 1, "referralSlug") ??
    ""
  ).trim();

  const telegramUsername = String(
    pickTupleValue(raw, 2, "telegramUsername") ??
    pickTupleValue(raw, 3, "telegram") ??
    ""
  ).trim();

  const telegramBound = Boolean(
    pickTupleValue(raw, 4, "telegramBound") ??
    (telegramUsername ? true : false)
  );

  return {
    wallet,
    slug,
    exists: Boolean(slug),
    telegramBound,
    telegramUsername
  };
}

function mapStats(raw: any): AmbassadorStats {
  const totalReferrals = safeNumber(
    pickTupleValue(raw, 0, "totalReferrals")
  );

  const totalQualifiedPurchases = safeNumber(
    pickTupleValue(raw, 1, "totalQualifiedPurchases")
  );

  const totalRewardSun = safeString(
    pickTupleValue(raw, 2, "totalRewardSun") ??
    pickTupleValue(raw, 2, "totalRewards")
  );

  const totalWithdrawnSun = safeString(
    pickTupleValue(raw, 3, "totalWithdrawnSun") ??
    pickTupleValue(raw, 3, "withdrawnRewards")
  );

  const availableSun = safeString(
    pickTupleValue(raw, 4, "availableSun") ??
    pickTupleValue(raw, 4, "claimableRewards")
  );

  return {
    totalReferrals,
    totalQualifiedPurchases,
    totalRewardSun,
    totalRewardTrx: sunToTrxString(totalRewardSun),
    totalWithdrawnSun,
    totalWithdrawnTrx: sunToTrxString(totalWithdrawnSun),
    availableSun,
    availableTrx: sunToTrxString(availableSun)
  };
}

function mapRewardSummary(raw: any): RewardSummary {
  const availableSun = safeString(
    pickTupleValue(raw, 0, "availableSun") ??
    pickTupleValue(raw, 0, "availableRewards")
  );

  const withdrawnSun = safeString(
    pickTupleValue(raw, 1, "withdrawnSun") ??
    pickTupleValue(raw, 1, "withdrawnRewards")
  );

  const lifetimeSun = safeString(
    pickTupleValue(raw, 2, "lifetimeSun") ??
    pickTupleValue(raw, 2, "totalRewards")
  );

  return {
    availableSun,
    availableTrx: sunToTrxString(availableSun),
    withdrawnSun,
    withdrawnTrx: sunToTrxString(withdrawnSun),
    lifetimeSun,
    lifetimeTrx: sunToTrxString(lifetimeSun)
  };
}

export async function readAmbassadorIdentity(
  wallet?: string
): Promise<AmbassadorIdentity> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const raw = await contract.getAmbassadorByWallet(resolvedWallet).call();

  return mapAmbassadorIdentity(raw, resolvedWallet);
}

export async function readAmbassadorBySlug(slug: string): Promise<any> {
  const contract = await getControllerContractInstance();
  return contract.getAmbassadorBySlug(assertNonEmpty(slug, "slug")).call();
}

export async function readAmbassadorStats(
  wallet?: string
): Promise<AmbassadorStats> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const raw = await contract.getAmbassadorStats(resolvedWallet).call();

  return mapStats(raw);
}

export async function readRewardSummary(
  wallet?: string
): Promise<RewardSummary> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const raw = await contract.getRewardSummary(resolvedWallet).call();

  return mapRewardSummary(raw);
}

export async function withdrawRewards(): Promise<WithdrawResult> {
  const contract = await getControllerContractInstance();
  const txid = await contract.withdrawRewards().send();

  return {
    txid: assertNonEmpty(txid, "txid")
  };
}

export async function readAmbassadorDashboard(wallet?: string): Promise<{
  identity: AmbassadorIdentity;
  stats: AmbassadorStats;
  rewards: RewardSummary;
}> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const [identity, stats, rewards] = await Promise.all([
    readAmbassadorIdentity(resolvedWallet),
    readAmbassadorStats(resolvedWallet),
    readRewardSummary(resolvedWallet)
  ]);

  return {
    identity,
    stats,
    rewards
  };
}
