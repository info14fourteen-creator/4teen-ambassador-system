import { FOURTEEN_CONTROLLER_CONTRACT } from "../../../../../shared/config/contracts";

declare global {
  interface Window {
    tronWeb?: any;
    tronLink?: any;
  }
}

export interface AmbassadorIdentity {
  wallet: string;
  exists: boolean;
  active: boolean;
  selfRegistered: boolean;
  manualAssigned: boolean;
  overrideEnabled: boolean;
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
  totalVolumeSun: string;
  totalVolumeTrx: string;
  totalRewardsAccruedSun: string;
  totalRewardsAccruedTrx: string;
  totalRewardsClaimedSun: string;
  totalRewardsClaimedTrx: string;
  claimableRewardsSun: string;
  claimableRewardsTrx: string;
}

export interface RewardSummary {
  availableSun: string;
  availableTrx: string;
  withdrawnSun: string;
  withdrawnTrx: string;
  lifetimeSun: string;
  lifetimeTrx: string;
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
  pendingBackendSyncSun: string;
  pendingBackendSyncTrx: string;
  requestedForProcessingSun: string;
  requestedForProcessingTrx: string;
  availableOnChainCount: number;
  pendingBackendSyncCount: number;
  requestedForProcessingCount: number;
  hasProcessingWithdrawal: boolean;
}

export interface AmbassadorDashboard {
  identity: AmbassadorIdentity;
  stats: AmbassadorStats;
  rewards: RewardSummary;
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

function safeString(value: any): string {
  if (value == null) return "0";
  return String(value);
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

function sunToTrxString(value: any): string {
  const raw = safeString(value);

  if (!raw || raw === "0") {
    return "0";
  }

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
  const slugHash = safeString(pickTupleValue(profileRaw, 5, "slugHash"));
  const metaHash = safeString(pickTupleValue(profileRaw, 6, "metaHash"));

  return {
    wallet,
    exists,
    active,
    selfRegistered,
    manualAssigned,
    overrideEnabled,
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
  const totalVolumeSun = safeString(pickTupleValue(statsRaw, 1, "totalVolumeSun"));
  const totalRewardsAccruedSun = safeString(
    pickTupleValue(statsRaw, 2, "totalRewardsAccruedSun")
  );
  const totalRewardsClaimedSun = safeString(
    pickTupleValue(statsRaw, 3, "totalRewardsClaimedSun")
  );
  const claimableRewardsSun = safeString(
    pickTupleValue(statsRaw, 4, "claimableRewardsSun")
  );

  return {
    totalBuyers,
    totalVolumeSun,
    totalVolumeTrx: sunToTrxString(totalVolumeSun),
    totalRewardsAccruedSun,
    totalRewardsAccruedTrx: sunToTrxString(totalRewardsAccruedSun),
    totalRewardsClaimedSun,
    totalRewardsClaimedTrx: sunToTrxString(totalRewardsClaimedSun),
    claimableRewardsSun,
    claimableRewardsTrx: sunToTrxString(claimableRewardsSun)
  };
}

function mapRewards(payoutRaw: any): RewardSummary {
  const availableSun = safeString(pickTupleValue(payoutRaw, 0, "claimableRewardsSun"));
  const lifetimeSun = safeString(pickTupleValue(payoutRaw, 1, "totalRewardsAccruedSun"));
  const withdrawnSun = safeString(pickTupleValue(payoutRaw, 2, "totalRewardsClaimedSun"));

  return {
    availableSun,
    availableTrx: sunToTrxString(availableSun),
    withdrawnSun,
    withdrawnTrx: sunToTrxString(withdrawnSun),
    lifetimeSun,
    lifetimeTrx: sunToTrxString(lifetimeSun)
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

function mapWithdrawalQueue(raw: any): AmbassadorWithdrawalQueue {
  const availableOnChainSun = safeString(
    pickTupleValue(raw, 0, "availableOnChainSun")
  );
  const pendingBackendSyncSun = safeString(
    pickTupleValue(raw, 1, "pendingBackendSyncSun")
  );
  const requestedForProcessingSun = safeString(
    pickTupleValue(raw, 2, "requestedForProcessingSun")
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

  return {
    availableOnChainSun,
    availableOnChainTrx: sunToTrxString(availableOnChainSun),
    pendingBackendSyncSun,
    pendingBackendSyncTrx: sunToTrxString(pendingBackendSyncSun),
    requestedForProcessingSun,
    requestedForProcessingTrx: sunToTrxString(requestedForProcessingSun),
    availableOnChainCount,
    pendingBackendSyncCount,
    requestedForProcessingCount,
    hasProcessingWithdrawal
  };
}

function buildFallbackWithdrawalQueue(rewards: RewardSummary): AmbassadorWithdrawalQueue {
  return {
    availableOnChainSun: rewards.availableSun,
    availableOnChainTrx: rewards.availableTrx,
    pendingBackendSyncSun: "0",
    pendingBackendSyncTrx: "0",
    requestedForProcessingSun: "0",
    requestedForProcessingTrx: "0",
    availableOnChainCount: 0,
    pendingBackendSyncCount: 0,
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

export async function readRewardSummary(wallet?: string): Promise<RewardSummary> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const raw = await contract.getAmbassadorPayoutData(resolvedWallet).call();

  return mapRewards(raw);
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
  wallet?: string
): Promise<AmbassadorWithdrawalQueue> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();

  if (typeof contract.getAmbassadorWithdrawalQueue === "function") {
    const raw = await contract.getAmbassadorWithdrawalQueue(resolvedWallet).call();
    return mapWithdrawalQueue(raw);
  }

  if (typeof contract.getDashboardWithdrawalQueue === "function") {
    const raw = await contract.getDashboardWithdrawalQueue(resolvedWallet).call();
    return mapWithdrawalQueue(raw);
  }

  const rewards = await readRewardSummary(resolvedWallet);
  return buildFallbackWithdrawalQueue(rewards);
}

export async function withdrawRewards(): Promise<WithdrawResult> {
  const contract = await getControllerContractInstance();
  const txid = await contract.withdrawRewards().send();

  return {
    txid: assertNonEmpty(txid, "txid")
  };
}

export async function readAmbassadorDashboard(wallet?: string): Promise<AmbassadorDashboard> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const [identity, stats, rewards, progress, withdrawalQueue] = await Promise.all([
    readAmbassadorIdentity(resolvedWallet),
    readAmbassadorStats(resolvedWallet),
    readRewardSummary(resolvedWallet),
    readAmbassadorLevelProgress(resolvedWallet),
    readAmbassadorWithdrawalQueue(resolvedWallet)
  ]);

  return {
    identity,
    stats,
    rewards,
    progress,
    withdrawalQueue
  };
}
