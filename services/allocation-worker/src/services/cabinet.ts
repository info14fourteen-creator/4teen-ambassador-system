import { getAmbassadorRegistryRecordByWallet } from "../db/ambassadors";
import type {
  CabinetStatsRecord,
  PurchaseProcessingStatus,
  PurchaseStore
} from "../db/purchases";
import {
  getAllocationRetryReadyAt,
  isPurchaseReadyForAllocationRetry,
  isRateLimitedAllocationFailure
} from "../db/purchases";

export interface CabinetReplayResultItem {
  purchaseId: string;
  ok: boolean;
  skipped?: boolean;
  error?: string;
  result?: unknown;
}

export interface CabinetReplayPendingResult {
  wallet: string;
  totalFound: number;
  attempted: number;
  succeeded: number;
  failed: number;
  skipped: number;
  items: CabinetReplayResultItem[];
}

export interface CabinetServiceDependencies {
  store: PurchaseStore;
  tronWeb: any;
  controllerContractAddress: string;
  processor: {
    replayFailedAllocation: (
      purchaseId: string,
      feeLimitSun?: number,
      now?: number
    ) => Promise<unknown>;
  };
}

export interface CabinetProfileIdentity {
  active: boolean;
  level: number;
  levelLabel: string;
  rewardPercent: number;
  createdAt: number;
  slugHash: string;
  metaHash: string | null;
}

export interface CabinetProfileStats {
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

export interface CabinetProfileWithdrawalQueue {
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

export interface CabinetProfileProgress {
  currentLevel: number;
  buyersCount: number;
  nextThreshold: number;
  remainingToNextLevel: number;
}

export interface CabinetProfileRegisteredResult {
  registered: true;
  wallet: string;
  slug: string;
  status: string;
  referralLink: string;
  identity: CabinetProfileIdentity;
  stats: CabinetProfileStats;
  withdrawalQueue: CabinetProfileWithdrawalQueue;
  progress: CabinetProfileProgress;
}

export interface CabinetProfileNotRegisteredResult {
  registered: false;
  wallet: string;
}

export type CabinetProfileResult =
  | CabinetProfileRegisteredResult
  | CabinetProfileNotRegisteredResult;

const DEFAULT_PENDING_STATUSES: PurchaseProcessingStatus[] = [
  "verified",
  "deferred",
  "allocation_failed_retryable"
];

const ZERO_BYTES32 =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
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
  return Boolean(value);
}

function toErrorMessage(error: unknown): string {
  if (typeof error === "string" && error.trim()) {
    return error.trim();
  }

  if (
    error &&
    typeof error === "object" &&
    "message" in error &&
    typeof (error as { message?: unknown }).message === "string"
  ) {
    const message = (error as { message: string }).message.trim();
    if (message) {
      return message;
    }
  }

  return "Unknown error";
}

function sunToTrxString(value: string | number | bigint | null | undefined): string {
  const raw = String(value ?? "0").trim();

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

function levelToLabel(level: number): string {
  if (level === 0) return "Bronze";
  if (level === 1) return "Silver";
  if (level === 2) return "Gold";
  if (level === 3) return "Platinum";
  return `Unknown (${level})`;
}

function buildReferralLink(slug: string): string {
  return `https://4teen.me/?r=${encodeURIComponent(slug)}`;
}

function normalizeHex32(value: unknown): string {
  const raw = String(value ?? "").trim().toLowerCase();
  return raw || ZERO_BYTES32;
}

function normalizeMetaHash(value: unknown): string | null {
  const raw = String(value ?? "").trim().toLowerCase();

  if (!raw || raw === ZERO_BYTES32) {
    return null;
  }

  return raw;
}

function pickTupleValue(source: any, index: number, key?: string): unknown {
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

function mapStats(input: {
  onChainStats: {
    totalBuyers: string;
    trackedVolumeSun: string;
    claimableRewardsSun: string;
    lifetimeRewardsSun: string;
    withdrawnRewardsSun: string;
  };
  dbStats: CabinetStatsRecord;
}): {
  stats: CabinetProfileStats;
  withdrawalQueue: CabinetProfileWithdrawalQueue;
} {
  const { onChainStats, dbStats } = input;

  return {
    stats: {
      totalBuyers: safeNumber(onChainStats.totalBuyers),
      trackedVolumeSun: onChainStats.trackedVolumeSun,
      trackedVolumeTrx: sunToTrxString(onChainStats.trackedVolumeSun),
      claimableRewardsSun: onChainStats.claimableRewardsSun,
      claimableRewardsTrx: sunToTrxString(onChainStats.claimableRewardsSun),
      lifetimeRewardsSun: onChainStats.lifetimeRewardsSun,
      lifetimeRewardsTrx: sunToTrxString(onChainStats.lifetimeRewardsSun),
      withdrawnRewardsSun: onChainStats.withdrawnRewardsSun,
      withdrawnRewardsTrx: sunToTrxString(onChainStats.withdrawnRewardsSun)
    },
    withdrawalQueue: {
      /**
       * Real withdrawable now.
       * This must come only from blockchain contract state.
       */
      availableOnChainSun: onChainStats.claimableRewardsSun,
      availableOnChainTrx: sunToTrxString(onChainStats.claimableRewardsSun),
      availableOnChainCount: dbStats.availableOnChainCount,

      /**
       * Backend-derived informational bucket.
       * Allocated in DB does NOT mean currently withdrawable on-chain.
       */
      allocatedInDbSun: dbStats.allocatedInDbSun,
      allocatedInDbTrx: sunToTrxString(dbStats.allocatedInDbSun),
      allocatedInDbCount: dbStats.allocatedInDbCount,

      /**
       * Verified / deferred / retryable purchases that still need backend sync.
       */
      pendingBackendSyncSun: dbStats.pendingBackendSyncSun,
      pendingBackendSyncTrx: sunToTrxString(dbStats.pendingBackendSyncSun),
      pendingBackendSyncCount: dbStats.pendingBackendSyncCount,

      /**
       * Queue already included into active withdrawal preparation flow.
       */
      requestedForProcessingSun: dbStats.requestedForProcessingSun,
      requestedForProcessingTrx: sunToTrxString(dbStats.requestedForProcessingSun),
      requestedForProcessingCount: dbStats.requestedForProcessingCount,

      hasProcessingWithdrawal: dbStats.hasProcessingWithdrawal
    }
  };
}

function extractReplayStatus(result: unknown): string {
  if (
    result &&
    typeof result === "object" &&
    "status" in result &&
    typeof (result as { status?: unknown }).status === "string"
  ) {
    return String((result as { status: string }).status).trim().toLowerCase();
  }

  return "";
}

function extractReplayReason(result: unknown): string | null {
  if (
    result &&
    typeof result === "object" &&
    "reason" in result &&
    typeof (result as { reason?: unknown }).reason === "string"
  ) {
    const reason = String((result as { reason: string }).reason).trim();
    return reason || null;
  }

  if (
    result &&
    typeof result === "object" &&
    "errorMessage" in result &&
    typeof (result as { errorMessage?: unknown }).errorMessage === "string"
  ) {
    const errorMessage = String(
      (result as { errorMessage: string }).errorMessage
    ).trim();
    return errorMessage || null;
  }

  return null;
}

function toSunString(value: unknown): string {
  if (typeof value === "bigint") {
    return value.toString();
  }

  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return Math.floor(value).toString();
  }

  if (typeof value === "string" && /^\d+$/.test(value.trim())) {
    return value.trim();
  }

  return "0";
}

export class CabinetService {
  private readonly store: PurchaseStore;
  private readonly tronWeb: any;
  private readonly controllerContractAddress: string;
  private readonly processor: CabinetServiceDependencies["processor"];
  private contractInstance: any | null = null;

  constructor(deps: CabinetServiceDependencies) {
    if (!deps?.store) {
      throw new Error("store is required");
    }

    if (!deps?.tronWeb) {
      throw new Error("tronWeb is required");
    }

    if (!deps?.processor) {
      throw new Error("processor is required");
    }

    this.store = deps.store;
    this.tronWeb = deps.tronWeb;
    this.processor = deps.processor;
    this.controllerContractAddress = assertNonEmpty(
      deps.controllerContractAddress,
      "controllerContractAddress"
    );
  }

  private async contract(): Promise<any> {
    if (!this.contractInstance) {
      this.contractInstance = await this.tronWeb.contract().at(this.controllerContractAddress);
    }

    return this.contractInstance;
  }

  private async readOnChainDashboard(wallet: string): Promise<{
    identity: CabinetProfileIdentity;
    progress: CabinetProfileProgress;
    stats: {
      totalBuyers: string;
      trackedVolumeSun: string;
      claimableRewardsSun: string;
      lifetimeRewardsSun: string;
      withdrawnRewardsSun: string;
    };
  }> {
    const contract = await this.contract();

    const [coreRaw, profileRaw, progressRaw, statsRaw] = await Promise.all([
      contract.getDashboardCore(wallet).call(),
      contract.getDashboardProfile(wallet).call(),
      contract.getAmbassadorLevelProgress(wallet).call(),
      contract.getDashboardStats(wallet).call()
    ]);

    const active = safeBoolean(pickTupleValue(coreRaw, 1, "active"));
    const effectiveLevel = safeNumber(pickTupleValue(coreRaw, 2, "effectiveLevel"));
    const rewardPercent = safeNumber(pickTupleValue(coreRaw, 3, "rewardPercent"));
    const createdAt = safeNumber(pickTupleValue(coreRaw, 4, "createdAt"));

    const currentLevel = safeNumber(pickTupleValue(profileRaw, 3, "currentLevel"));
    const slugHash = normalizeHex32(pickTupleValue(profileRaw, 5, "slugHash"));
    const metaHash = normalizeMetaHash(pickTupleValue(profileRaw, 6, "metaHash"));

    const buyersCount = safeNumber(pickTupleValue(progressRaw, 1, "buyersCount"));
    const nextThreshold = safeNumber(pickTupleValue(progressRaw, 2, "nextThreshold"));
    const remainingToNextLevel = safeNumber(
      pickTupleValue(progressRaw, 3, "remainingToNextLevel")
    );

    const totalBuyers = toSunString(pickTupleValue(statsRaw, 0, "totalBuyers"));
    const trackedVolumeSun = toSunString(pickTupleValue(statsRaw, 1, "totalVolumeSun"));
    const lifetimeRewardsSun = toSunString(
      pickTupleValue(statsRaw, 2, "totalRewardsAccruedSun")
    );
    const withdrawnRewardsSun = toSunString(
      pickTupleValue(statsRaw, 3, "totalRewardsClaimedSun")
    );
    const claimableRewardsSun = toSunString(
      pickTupleValue(statsRaw, 4, "claimableRewardsSun")
    );

    return {
      identity: {
        active,
        level: effectiveLevel,
        levelLabel: levelToLabel(effectiveLevel),
        rewardPercent,
        createdAt,
        slugHash,
        metaHash
      },
      progress: {
        currentLevel,
        buyersCount,
        nextThreshold,
        remainingToNextLevel
      },
      stats: {
        totalBuyers,
        trackedVolumeSun,
        claimableRewardsSun,
        lifetimeRewardsSun,
        withdrawnRewardsSun
      }
    };
  }

  async getProfileByWallet(wallet: string): Promise<CabinetProfileResult> {
    const normalizedWallet = assertNonEmpty(wallet, "wallet");
    const record = await getAmbassadorRegistryRecordByWallet(normalizedWallet);

    if (!record) {
      return {
        registered: false,
        wallet: normalizedWallet
      };
    }

    const registryWallet = assertNonEmpty(record.privateIdentity.wallet, "registryWallet");

    const [dbStatsRecord, onChain] = await Promise.all([
      this.store.getCabinetStatsByAmbassadorWallet(registryWallet),
      this.readOnChainDashboard(registryWallet)
    ]);

    const mapped = mapStats({
      onChainStats: onChain.stats,
      dbStats: dbStatsRecord
    });

    return {
      registered: true,
      wallet: registryWallet,
      slug: record.publicProfile.slug,
      status: record.publicProfile.status,
      referralLink: buildReferralLink(record.publicProfile.slug),
      identity: onChain.identity,
      stats: mapped.stats,
      withdrawalQueue: mapped.withdrawalQueue,
      progress: onChain.progress
    };
  }

  async replayPendingByWallet(
    wallet: string,
    now: number = Date.now(),
    feeLimitSun?: number
  ): Promise<CabinetReplayPendingResult> {
    const normalizedWallet = assertNonEmpty(wallet, "wallet");
    const record = await getAmbassadorRegistryRecordByWallet(normalizedWallet);

    if (!record) {
      throw new Error("Ambassador not found for wallet");
    }

    const registryWallet = assertNonEmpty(record.privateIdentity.wallet, "registryWallet");

    const pending = await this.store.listPendingByAmbassador({
      ambassadorWallet: registryWallet,
      statuses: DEFAULT_PENDING_STATUSES
    });

    const items: CabinetReplayResultItem[] = [];

    for (const purchase of pending) {
      if (!isPurchaseReadyForAllocationRetry(purchase, now)) {
        const retryAt = getAllocationRetryReadyAt(purchase);
        const retryInMs = Math.max(0, retryAt - now);

        items.push({
          purchaseId: purchase.purchaseId,
          ok: true,
          skipped: true,
          error: isRateLimitedAllocationFailure(purchase)
            ? `Cooldown active after rate limit. Retry in ${retryInMs}ms`
            : `Cooldown active. Retry in ${retryInMs}ms`
        });
        continue;
      }

      try {
        const result = await this.processor.replayFailedAllocation(
          purchase.purchaseId,
          feeLimitSun,
          now
        );

        const replayStatus = extractReplayStatus(result);
        const isAllocated = replayStatus === "allocated";
        const isSkipped = replayStatus === "skipped";
        const replayReason =
          extractReplayReason(result) ??
          (isSkipped ? "Replay skipped" : "Allocation failed");

        items.push({
          purchaseId: purchase.purchaseId,
          ok: isAllocated,
          skipped: isSkipped,
          error: isAllocated ? undefined : replayReason,
          result
        });
      } catch (error) {
        items.push({
          purchaseId: purchase.purchaseId,
          ok: false,
          error: toErrorMessage(error)
        });
      }
    }

    const succeeded = items.filter((item) => item.ok && !item.skipped).length;
    const skipped = items.filter((item) => item.skipped).length;
    const failed = items.filter((item) => !item.ok && !item.skipped).length;

    return {
      wallet: registryWallet,
      totalFound: pending.length,
      attempted: succeeded + failed,
      succeeded,
      failed,
      skipped,
      items
    };
  }
}

export function createCabinetService(deps: CabinetServiceDependencies): CabinetService {
  return new CabinetService(deps);
}
