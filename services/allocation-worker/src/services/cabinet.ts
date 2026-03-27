import { getAmbassadorRegistryRecordByWallet } from "../db/ambassadors";
import type { CabinetStatsRecord, PurchaseStore } from "../db/purchases";

export interface CabinetServiceDependencies {
  store: PurchaseStore;
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

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
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

/**
 * Строго по контракту:
 * Bronze   < 10 buyers
 * Silver   >= 10 buyers
 * Gold     >= 100 buyers
 * Platinum >= 1000 buyers
 */
function inferLevel(totalBuyers: number): number {
  if (totalBuyers >= 1000) return 3;
  if (totalBuyers >= 100) return 2;
  if (totalBuyers >= 10) return 1;
  return 0;
}

/**
 * Строго по контракту _getRewardPercentByLevel:
 * Bronze   = 10
 * Silver   = 25
 * Gold     = 50
 * Platinum = 75
 *
 * Это процент от ownerShareSun, а не от полной покупки.
 */
function inferRewardPercent(level: number): number {
  if (level === 3) return 75;
  if (level === 2) return 50;
  if (level === 1) return 25;
  return 10;
}

function buildProgress(totalBuyers: number): CabinetProfileProgress {
  const currentLevel = inferLevel(totalBuyers);

  if (currentLevel === 0) {
    return {
      currentLevel,
      buyersCount: totalBuyers,
      nextThreshold: 10,
      remainingToNextLevel: Math.max(0, 10 - totalBuyers)
    };
  }

  if (currentLevel === 1) {
    return {
      currentLevel,
      buyersCount: totalBuyers,
      nextThreshold: 100,
      remainingToNextLevel: Math.max(0, 100 - totalBuyers)
    };
  }

  if (currentLevel === 2) {
    return {
      currentLevel,
      buyersCount: totalBuyers,
      nextThreshold: 1000,
      remainingToNextLevel: Math.max(0, 1000 - totalBuyers)
    };
  }

  return {
    currentLevel: 3,
    buyersCount: totalBuyers,
    nextThreshold: 1000,
    remainingToNextLevel: 0
  };
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

  constructor(deps: CabinetServiceDependencies) {
    if (!deps?.store) {
      throw new Error("store is required");
    }

    this.store = deps.store;
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

    const registryWallet = record.privateIdentity.wallet;
    const statsRecord = await this.store.getCabinetStatsByAmbassadorWallet(registryWallet);
    const mapped = mapStats(statsRecord);

    const active = record.publicProfile.status === "active";
    const level = inferLevel(statsRecord.totalBuyers);
    const rewardPercent = inferRewardPercent(level);
    const progress = buildProgress(statsRecord.totalBuyers);

    return {
      registered: true,
      wallet: registryWallet,
      slug: record.publicProfile.slug,
      status: record.publicProfile.status,
      referralLink: buildReferralLink(record.publicProfile.slug),
      identity: {
        active,
        level,
        levelLabel: levelToLabel(level),
        rewardPercent,
        createdAt: record.publicProfile.createdAt,
        slugHash: record.publicProfile.slugHash,
        metaHash: null
      },
      stats: mapped.stats,
      withdrawalQueue: mapped.withdrawalQueue,
      progress
    };
  }
}

export function createCabinetService(deps: CabinetServiceDependencies): CabinetService {
  return new CabinetService(deps);
}
