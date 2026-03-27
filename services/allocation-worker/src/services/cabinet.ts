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

export interface CabinetProfileRegisteredResult {
  registered: true;
  wallet: string;
  slug: string;
  status: string;
  referralLink: string;
  identity: CabinetProfileIdentity;
  stats: CabinetProfileStats;
  withdrawalQueue: CabinetProfileWithdrawalQueue;
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
  return `?r=${encodeURIComponent(slug)}`;
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
    const level = 0;
    const rewardPercent = 0;

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
      withdrawalQueue: mapped.withdrawalQueue
    };
  }
}

export function createCabinetService(deps: CabinetServiceDependencies): CabinetService {
  return new CabinetService(deps);
}
