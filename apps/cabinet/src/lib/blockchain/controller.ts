import { FOURTEEN_CONTROLLER_CONTRACT } from "../../../../../shared/config/contracts";

declare global {
  interface Window {
    tronWeb?: any;
    tronLink?: any;
  }
}

const ZERO_BYTES32 =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

export interface AmbassadorIdentity {
  wallet: string;
  exists: boolean;
  active: boolean;
  selfRegistered: boolean;
  manualAssigned: boolean;
  overrideEnabled: boolean;
  level: number;
  effectiveLevel: number;
  currentLevel: number;
  overrideLevel: number;
  rewardPercent: number;
  createdAt: number;
  slugHash: string;
  metaHash: string;
}

export interface AmbassadorStats {
  totalBuyers: number;
  trackedVolumeSun: string;
  trackedVolumeTrx: string;
  claimableRewardsSun: string;
  claimableRewardsTrx: string;
  lifetimeRewardsSun: string;
  lifetimeRewardsTrx: string;
  withdrawnRewardsSun: string;
  withdrawnRewardsTrx: string;
}

export interface AmbassadorLevelProgress {
  currentLevel: number;
  buyersCount: number;
  nextThreshold: number;
  remainingToNextLevel: number;
}

export interface AmbassadorWithdrawalQueue {
  availableOnChainSun: string;
  availableOnChainTrx: string;
  availableOnChainCount: number;

  allocatedInDbSun: string;
  allocatedInDbTrx: string;
  allocatedInDbCount: number;

  pendingBackendSyncSun: string;
  pendingBackendSyncTrx: string;
  pendingBackendSyncCount: number;

  requestedForProcessingSun: string;
  requestedForProcessingTrx: string;
  requestedForProcessingCount: number;

  hasProcessingWithdrawal: boolean;
}

export interface AmbassadorDashboard {
  identity: AmbassadorIdentity;
  stats: AmbassadorStats;
  progress: AmbassadorLevelProgress;
  withdrawalQueue: AmbassadorWithdrawalQueue;
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

function safeString(value: any, fallback = "0"): string {
  if (value == null) return fallback;
  return String(value);
}

function safeSunString(value: any, fallback = "0"): string {
  const raw = safeString(value, fallback).trim();
  return /^\d+$/.test(raw) ? raw : fallback;
}

function safeNumber(value: any): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "bigint") {
    return Number(value);
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return 0;
}

function safeBoolean(value: any): boolean {
  return Boolean(value);
}

function pickTupleValue(source: any, index: number, key?: string): any {
  if (Array.isArray(source)) {
    return source[index];
  }

  if (source && typeof source === "object") {
    if (key && key in source) {
      return source[key];
    }

    const numericKey = String(index);
    if (numericKey in source) {
      return source[numericKey];
    }

    const values = Object.values(source);
    return values[index];
  }

  return undefined;
}

export function sunToTrxString(value: any): string {
  const raw = safeString(value, "0").trim();

  if (!raw || raw === "0") {
    return "0";
  }

  const negative = raw.startsWith("-");
  const digits = negative ? raw.slice(1) : raw;

  if (!/^\d+$/.test(digits)) {
    return "0";
  }

  const padded = digits.padStart(7, "0");
  const whole = padded.slice(0, -6) || "0";
  const fraction = padded.slice(-6).replace(/0+$/, "");
  const result = fraction ? `${whole}.${fraction}` : whole;

  return negative ? `-${result}` : result;
}

function normalizeHex32(value: any): string {
  const raw = safeString(value, ZERO_BYTES32).trim().toLowerCase();
  return raw || ZERO_BYTES32;
}

function normalizeMetaHash(value: any): string {
  const raw = normalizeHex32(value);
  return raw === ZERO_BYTES32 ? "—" : raw;
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
  return await tronWeb.contract().at(FOURTEEN_CONTROLLER_CONTRACT);
}

export function levelToLabel(level: number): string {
  if (level === 0) return "Bronze";
  if (level === 1) return "Silver";
  if (level === 2) return "Gold";
  if (level === 3) return "Platinum";
  return `Unknown (${level})`;
}

function mapIdentity(wallet: string, coreRaw: any, profileRaw: any): AmbassadorIdentity {
  const exists = safeBoolean(pickTupleValue(coreRaw, 0, "exists"));
  const active = safeBoolean(pickTupleValue(coreRaw, 1, "active"));
  const effectiveLevel = safeNumber(pickTupleValue(coreRaw, 2, "effectiveLevel"));
  const rewardPercent = safeNumber(pickTupleValue(coreRaw, 3, "rewardPercent"));
  const createdAt = safeNumber(pickTupleValue(coreRaw, 4, "createdAt"));

  const selfRegistered = safeBoolean(pickTupleValue(profileRaw, 0, "selfRegistered"));
  const manualAssigned = safeBoolean(pickTupleValue(profileRaw, 1, "manualAssigned"));
  const overrideEnabled = safeBoolean(pickTupleValue(profileRaw, 2, "overrideEnabled"));
  const currentLevel = safeNumber(pickTupleValue(profileRaw, 3, "currentLevel"));
  const overrideLevel = safeNumber(pickTupleValue(profileRaw, 4, "overrideLevel"));
  const slugHash = normalizeHex32(pickTupleValue(profileRaw, 5, "slugHash"));
  const metaHash = normalizeMetaHash(pickTupleValue(profileRaw, 6, "metaHash"));

  return {
    wallet,
    exists,
    active,
    selfRegistered,
    manualAssigned,
    overrideEnabled,
    level: effectiveLevel,
    effectiveLevel,
    currentLevel,
    overrideLevel,
    rewardPercent,
    createdAt,
    slugHash,
    metaHash
  };
}

function mapStats(statsRaw: any): AmbassadorStats {
  const totalBuyers = safeNumber(pickTupleValue(statsRaw, 0, "totalBuyers"));
  const trackedVolumeSun = safeSunString(
    pickTupleValue(statsRaw, 1, "totalVolumeSun") ??
      pickTupleValue(statsRaw, 1, "trackedVolumeSun"),
    "0"
  );
  const lifetimeRewardsSun = safeSunString(
    pickTupleValue(statsRaw, 2, "totalRewardsAccruedSun") ??
      pickTupleValue(statsRaw, 2, "lifetimeRewardsSun"),
    "0"
  );
  const withdrawnRewardsSun = safeSunString(
    pickTupleValue(statsRaw, 3, "totalRewardsClaimedSun") ??
      pickTupleValue(statsRaw, 3, "withdrawnRewardsSun"),
    "0"
  );
  const claimableRewardsSun = safeSunString(
    pickTupleValue(statsRaw, 4, "claimableRewardsSun"),
    "0"
  );

  return {
    totalBuyers,
    trackedVolumeSun,
    trackedVolumeTrx: sunToTrxString(trackedVolumeSun),
    claimableRewardsSun,
    claimableRewardsTrx: sunToTrxString(claimableRewardsSun),
    lifetimeRewardsSun,
    lifetimeRewardsTrx: sunToTrxString(lifetimeRewardsSun),
    withdrawnRewardsSun,
    withdrawnRewardsTrx: sunToTrxString(withdrawnRewardsSun)
  };
}

function mapProgress(progressRaw: any): AmbassadorLevelProgress {
  return {
    currentLevel: safeNumber(pickTupleValue(progressRaw, 0, "currentLevel")),
    buyersCount: safeNumber(pickTupleValue(progressRaw, 1, "buyersCount")),
    nextThreshold: safeNumber(pickTupleValue(progressRaw, 2, "nextThreshold")),
    remainingToNextLevel: safeNumber(pickTupleValue(progressRaw, 3, "remainingToNextLevel"))
  };
}

function mapWithdrawalQueue(raw: any, stats: AmbassadorStats): AmbassadorWithdrawalQueue {
  const availableOnChainSun = safeSunString(
    pickTupleValue(raw, 0, "availableOnChainSun") ?? stats.claimableRewardsSun,
    "0"
  );
  const pendingBackendSyncSun = safeSunString(
    pickTupleValue(raw, 1, "pendingBackendSyncSun"),
    "0"
  );
  const requestedForProcessingSun = safeSunString(
    pickTupleValue(raw, 2, "requestedForProcessingSun"),
    "0"
  );

  const availableOnChainCount = safeNumber(
    pickTupleValue(raw, 3, "availableOnChainCount")
  );
  const pendingBackendSyncCount = safeNumber(
    pickTupleValue(raw, 4, "pendingBackendSyncCount")
  );
  const requestedForProcessingCount = safeNumber(
    pickTupleValue(raw, 5, "requestedForProcessingCount")
  );
  const hasProcessingWithdrawal = safeBoolean(
    pickTupleValue(raw, 6, "hasProcessingWithdrawal")
  );

  const allocatedInDbSun = safeSunString(
    pickTupleValue(raw, 7, "allocatedInDbSun"),
    "0"
  );
  const allocatedInDbCount = safeNumber(
    pickTupleValue(raw, 8, "allocatedInDbCount")
  );

  return {
    availableOnChainSun,
    availableOnChainTrx: sunToTrxString(availableOnChainSun),
    availableOnChainCount,

    allocatedInDbSun,
    allocatedInDbTrx: sunToTrxString(allocatedInDbSun),
    allocatedInDbCount,

    pendingBackendSyncSun,
    pendingBackendSyncTrx: sunToTrxString(pendingBackendSyncSun),
    pendingBackendSyncCount,

    requestedForProcessingSun,
    requestedForProcessingTrx: sunToTrxString(requestedForProcessingSun),
    requestedForProcessingCount,

    hasProcessingWithdrawal
  };
}

function buildFallbackWithdrawalQueue(stats: AmbassadorStats): AmbassadorWithdrawalQueue {
  return {
    availableOnChainSun: stats.claimableRewardsSun,
    availableOnChainTrx: stats.claimableRewardsTrx,
    availableOnChainCount: 0,

    allocatedInDbSun: "0",
    allocatedInDbTrx: "0",
    allocatedInDbCount: 0,

    pendingBackendSyncSun: "0",
    pendingBackendSyncTrx: "0",
    pendingBackendSyncCount: 0,

    requestedForProcessingSun: "0",
    requestedForProcessingTrx: "0",
    requestedForProcessingCount: 0,

    hasProcessingWithdrawal: false
  };
}

export async function readAmbassadorIdentity(wallet?: string): Promise<AmbassadorIdentity> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();

  const [coreRaw, profileRaw] = await Promise.all([
    contract.getDashboardCore(resolvedWallet).call(),
    contract.getDashboardProfile(resolvedWallet).call()
  ]);

  return mapIdentity(resolvedWallet, coreRaw, profileRaw);
}

export async function readAmbassadorStats(wallet?: string): Promise<AmbassadorStats> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const raw = await contract.getDashboardStats(resolvedWallet).call();

  return mapStats(raw);
}

export async function readAmbassadorLevelProgress(
  wallet?: string
): Promise<AmbassadorLevelProgress> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const raw = await contract.getAmbassadorLevelProgress(resolvedWallet).call();

  return mapProgress(raw);
}

export async function readAmbassadorWithdrawalQueue(
  wallet?: string,
  statsOverride?: AmbassadorStats
): Promise<AmbassadorWithdrawalQueue> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const stats = statsOverride ?? (await readAmbassadorStats(resolvedWallet));

  if (typeof contract.getAmbassadorWithdrawalQueue === "function") {
    const raw = await contract.getAmbassadorWithdrawalQueue(resolvedWallet).call();
    return mapWithdrawalQueue(raw, stats);
  }

  if (typeof contract.getDashboardWithdrawalQueue === "function") {
    const raw = await contract.getDashboardWithdrawalQueue(resolvedWallet).call();
    return mapWithdrawalQueue(raw, stats);
  }

  return buildFallbackWithdrawalQueue(stats);
}

export async function withdrawRewards(): Promise<WithdrawResult> {
  const contract = await getControllerContractInstance();
  const txid = await contract.withdrawRewards().send();

  return {
    txid: assertNonEmpty(
      typeof txid === "string"
        ? txid
        : txid?.txid || txid?.transaction?.txID || txid?.txID || "",
      "txid"
    )
  };
}

export async function readAmbassadorDashboard(wallet?: string): Promise<AmbassadorDashboard> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const [identity, stats, progress] = await Promise.all([
    readAmbassadorIdentity(resolvedWallet),
    readAmbassadorStats(resolvedWallet),
    readAmbassadorLevelProgress(resolvedWallet)
  ]);

  const withdrawalQueue = await readAmbassadorWithdrawalQueue(resolvedWallet, stats);

  return {
    identity,
    stats,
    progress,
    withdrawalQueue
  };
}
