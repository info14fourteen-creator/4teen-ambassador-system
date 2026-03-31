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

function safeString(value: unknown, fallback = "0"): string {
  if (value == null) {
    return fallback;
  }

  return String(value);
}

function safeSunString(value: unknown, fallback = "0"): string {
  const raw = safeString(value, fallback).trim();

  if (!raw) {
    return fallback;
  }

  if (/^\d+$/.test(raw)) {
    return raw;
  }

  if (/^-?\d+$/.test(raw)) {
    return raw.startsWith("-") ? fallback : raw;
  }

  return fallback;
}

function safeNumber(value: unknown, fallback = 0): number {
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

  return fallback;
}

function safeBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value === "bigint") {
    return value !== 0n;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();

    if (!normalized) return false;
    if (normalized === "true") return true;
    if (normalized === "false") return false;
    if (normalized === "1") return true;
    if (normalized === "0") return false;
  }

  return Boolean(value);
}

function pickTupleValue(source: any, index: number, ...keys: string[]): any {
  if (Array.isArray(source)) {
    if (source[index] !== undefined) {
      return source[index];
    }
  }

  if (source && typeof source === "object") {
    for (const key of keys) {
      if (key && key in source) {
        return source[key];
      }
    }

    const numericKey = String(index);
    if (numericKey in source) {
      return source[numericKey];
    }

    const values = Object.values(source);
    if (values[index] !== undefined) {
      return values[index];
    }
  }

  return undefined;
}

function pickFirstDefined(source: any, candidates: Array<{ index: number; keys: string[] }>): any {
  for (const candidate of candidates) {
    const value = pickTupleValue(source, candidate.index, ...candidate.keys);
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return undefined;
}

export function sunToTrxString(value: unknown): string {
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

function normalizeHex32(value: unknown): string {
  const raw = safeString(value, ZERO_BYTES32).trim().toLowerCase();

  if (!raw) {
    return ZERO_BYTES32;
  }

  if (/^0x[0-9a-f]{64}$/.test(raw)) {
    return raw;
  }

  return ZERO_BYTES32;
}

function normalizeMetaHash(value: unknown): string {
  const raw = normalizeHex32(value);
  return raw === ZERO_BYTES32 ? "—" : raw;
}

function normalizeSlugHash(value: unknown): string {
  const raw = normalizeHex32(value);
  return raw || ZERO_BYTES32;
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
  const exists = safeBoolean(
    pickFirstDefined(coreRaw, [
      { index: 0, keys: ["exists"] }
    ])
  );

  const active = safeBoolean(
    pickFirstDefined(coreRaw, [
      { index: 1, keys: ["active"] }
    ])
  );

  const effectiveLevel = safeNumber(
    pickFirstDefined(coreRaw, [
      { index: 2, keys: ["effectiveLevel", "level"] }
    ])
  );

  const rewardPercent = safeNumber(
    pickFirstDefined(coreRaw, [
      { index: 3, keys: ["rewardPercent"] }
    ])
  );

  const createdAt = safeNumber(
    pickFirstDefined(coreRaw, [
      { index: 4, keys: ["createdAt"] }
    ])
  );

  const selfRegistered = safeBoolean(
    pickFirstDefined(profileRaw, [
      { index: 0, keys: ["selfRegistered"] }
    ])
  );

  const manualAssigned = safeBoolean(
    pickFirstDefined(profileRaw, [
      { index: 1, keys: ["manualAssigned"] }
    ])
  );

  const overrideEnabled = safeBoolean(
    pickFirstDefined(profileRaw, [
      { index: 2, keys: ["overrideEnabled"] }
    ])
  );

  const currentLevel = safeNumber(
    pickFirstDefined(profileRaw, [
      { index: 3, keys: ["currentLevel"] }
    ])
  );

  const overrideLevel = safeNumber(
    pickFirstDefined(profileRaw, [
      { index: 4, keys: ["overrideLevel"] }
    ])
  );

  const slugHash = normalizeSlugHash(
    pickFirstDefined(profileRaw, [
      { index: 5, keys: ["slugHash"] }
    ])
  );

  const metaHash = normalizeMetaHash(
    pickFirstDefined(profileRaw, [
      { index: 6, keys: ["metaHash"] }
    ])
  );

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
  const totalBuyers = safeNumber(
    pickFirstDefined(statsRaw, [
      { index: 0, keys: ["totalBuyers", "buyersCount"] }
    ])
  );

  const trackedVolumeSun = safeSunString(
    pickFirstDefined(statsRaw, [
      { index: 1, keys: ["trackedVolumeSun", "totalVolumeSun"] }
    ]),
    "0"
  );

  const lifetimeRewardsSun = safeSunString(
    pickFirstDefined(statsRaw, [
      { index: 2, keys: ["lifetimeRewardsSun", "totalRewardsAccruedSun"] }
    ]),
    "0"
  );

  const withdrawnRewardsSun = safeSunString(
    pickFirstDefined(statsRaw, [
      { index: 3, keys: ["withdrawnRewardsSun", "totalRewardsClaimedSun"] }
    ]),
    "0"
  );

  const claimableRewardsSun = safeSunString(
    pickFirstDefined(statsRaw, [
      { index: 4, keys: ["claimableRewardsSun", "availableOnChainSun"] }
    ]),
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

function mapProgress(progressRaw: any, identity?: AmbassadorIdentity): AmbassadorLevelProgress {
  const currentLevel = safeNumber(
    pickFirstDefined(progressRaw, [
      { index: 0, keys: ["currentLevel", "level"] }
    ]),
    identity?.currentLevel ?? identity?.effectiveLevel ?? 0
  );

  const buyersCount = safeNumber(
    pickFirstDefined(progressRaw, [
      { index: 1, keys: ["buyersCount", "totalBuyers"] }
    ]),
    0
  );

  const nextThreshold = safeNumber(
    pickFirstDefined(progressRaw, [
      { index: 2, keys: ["nextThreshold"] }
    ]),
    0
  );

  const remainingToNextLevel = safeNumber(
    pickFirstDefined(progressRaw, [
      { index: 3, keys: ["remainingToNextLevel"] }
    ]),
    0
  );

  return {
    currentLevel,
    buyersCount,
    nextThreshold,
    remainingToNextLevel
  };
}

function mapWithdrawalQueue(raw: any, stats: AmbassadorStats): AmbassadorWithdrawalQueue {
  const availableOnChainSun = safeSunString(
    pickFirstDefined(raw, [
      { index: 0, keys: ["availableOnChainSun", "claimableRewardsSun"] }
    ]),
    stats.claimableRewardsSun
  );

  const pendingBackendSyncSun = safeSunString(
    pickFirstDefined(raw, [
      { index: 1, keys: ["pendingBackendSyncSun"] }
    ]),
    "0"
  );

  const requestedForProcessingSun = safeSunString(
    pickFirstDefined(raw, [
      { index: 2, keys: ["requestedForProcessingSun"] }
    ]),
    "0"
  );

  const availableOnChainCount = safeNumber(
    pickFirstDefined(raw, [
      { index: 3, keys: ["availableOnChainCount"] }
    ]),
    0
  );

  const pendingBackendSyncCount = safeNumber(
    pickFirstDefined(raw, [
      { index: 4, keys: ["pendingBackendSyncCount"] }
    ]),
    0
  );

  const requestedForProcessingCount = safeNumber(
    pickFirstDefined(raw, [
      { index: 5, keys: ["requestedForProcessingCount"] }
    ]),
    0
  );

  const hasProcessingWithdrawal = safeBoolean(
    pickFirstDefined(raw, [
      { index: 6, keys: ["hasProcessingWithdrawal"] }
    ])
  );

  const allocatedInDbSun = safeSunString(
    pickFirstDefined(raw, [
      { index: 7, keys: ["allocatedInDbSun"] }
    ]),
    "0"
  );

  const allocatedInDbCount = safeNumber(
    pickFirstDefined(raw, [
      { index: 8, keys: ["allocatedInDbCount"] }
    ]),
    0
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
    availableOnChainCount: stats.claimableRewardsSun !== "0" ? 1 : 0,

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
  const identity = await readAmbassadorIdentity(resolvedWallet);
  const raw = await contract.getAmbassadorLevelProgress(resolvedWallet).call();

  return mapProgress(raw, identity);
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

  const contract = await getControllerContractInstance();

  const [coreRaw, profileRaw, statsRaw, progressRaw] = await Promise.all([
    contract.getDashboardCore(resolvedWallet).call(),
    contract.getDashboardProfile(resolvedWallet).call(),
    contract.getDashboardStats(resolvedWallet).call(),
    contract.getAmbassadorLevelProgress(resolvedWallet).call()
  ]);

  const identity = mapIdentity(resolvedWallet, coreRaw, profileRaw);
  const stats = mapStats(statsRaw);
  const progress = mapProgress(progressRaw, identity);

  let withdrawalQueue: AmbassadorWithdrawalQueue;

  if (typeof contract.getAmbassadorWithdrawalQueue === "function") {
    const raw = await contract.getAmbassadorWithdrawalQueue(resolvedWallet).call();
    withdrawalQueue = mapWithdrawalQueue(raw, stats);
  } else if (typeof contract.getDashboardWithdrawalQueue === "function") {
    const raw = await contract.getDashboardWithdrawalQueue(resolvedWallet).call();
    withdrawalQueue = mapWithdrawalQueue(raw, stats);
  } else {
    withdrawalQueue = buildFallbackWithdrawalQueue(stats);
  }

  return {
    identity,
    stats,
    progress,
    withdrawalQueue
  };
}
