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
  pendingBackendSyncSun: string;
  pendingBackendSyncTrx: string;
  requestedForProcessingSun: string;
  requestedForProcessingTrx: string;
  availableOnChainCount: number;
  pendingBackendSyncCount: number;
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
  return raw || "0x0000000000000000000000000000000000000000000000000000000000000000";
}

function normalizeMetaHash(value: unknown): string | null {
  const raw = String(value ?? "").trim().toLowerCase();

  if (!raw) {
    return null;
  }

  if (raw === "0x0000000000000000000000000000000000000000000000000000000000000000") {
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

function mapStats(stats: CabinetStatsRecord): {
  stats: CabinetProfileStats;
  withdrawalQueue: CabinetProfileWithdrawalQueue;
} {
  return {
    stats: {
      totalBuyers: stats.totalBuyers,
      trackedVolumeSun: stats.trackedVolumeSun,
      trackedVolumeTrx: sunToTrxString(stats.trackedVolumeSun),
      claimableRewardsSun: stats.claimableRewardsSun,
      claimableRewardsTrx: sunToTrxString(stats.claimableRewardsSun),
      lifetimeRewardsSun: stats.lifetimeRewardsSun,
      lifetimeRewardsTrx: sunToTrxString(stats.lifetimeRewardsSun),
      withdrawnRewardsSun: stats.withdrawnRewardsSun,
      withdrawnRewardsTrx: sunToTrxString(stats.withdrawnRewardsSun)
    },
    withdrawalQueue: {
      availableOnChainSun: stats.availableOnChainSun,
      availableOnChainTrx: sunToTrxString(stats.availableOnChainSun),
      pendingBackendSyncSun: stats.pendingBackendSyncSun,
      pendingBackendSyncTrx: sunToTrxString(stats.pendingBackendSyncSun),
      requestedForProcessingSun: stats.requestedForProcessingSun,
      requestedForProcessingTrx: sunToTrxString(stats.requestedForProcessingSun),
      availableOnChainCount: stats.availableOnChainCount,
      pendingBackendSyncCount: stats.pendingBackendSyncCount,
      requestedForProcessingCount: stats.requestedForProcessingCount,
      hasProcessingWithdrawal: stats.hasProcessingWithdrawal
    }
  };
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
  }> {
    const contract = await this.contract();

    const [coreRaw, profileRaw, progressRaw] = await Promise.all([
      contract.getDashboardCore(wallet).call(),
      contract.getDashboardProfile(wallet).call(),
      contract.getAmbassadorLevelProgress(wallet).call()
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
    const statsRecord = await this.store.getCabinetStatsByAmbassadorWallet(registryWallet);
    const mapped = mapStats(statsRecord);
    const onChain = await this.readOnChainDashboard(registryWallet);

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

        items.push({
          purchaseId: purchase.purchaseId,
          ok: true,
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
    const failed = items.filter((item) => !item.ok).length;

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
