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

function normalizeSunString(value: string | number | bigint | null | undefined): string {
  const raw = String(value ?? "0").trim();

  if (!raw || raw === "0") {
    return "0";
  }

  const negative = raw.startsWith("-");
  const digits = negative ? raw.slice(1) : raw;

  if (!/^\d+$/.test(digits)) {
    return "0";
  }

  const normalizedDigits = digits.replace(/^0+/, "") || "0";
  return negative ? `-${normalizedDigits}` : normalizedDigits;
}

function sunToTrxString(value: string | number | bigint | null | undefined): string {
  const raw = normalizeSunString(value);

  if (raw === "0") {
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

function inferLevel(totalBuyers: number): number {
  if (totalBuyers >= 100) return 3;
  if (totalBuyers >= 25) return 2;
  if (totalBuyers >= 5) return 1;
  return 0;
}

function inferRewardPercent(level: number): number {
  if (level === 3) return 12;
  if (level === 2) return 10;
  if (level === 1) return 8;
  return 7;
}

function mapStats(stats: CabinetStatsRecord): {
  stats: CabinetProfileStats;
  withdrawalQueue: CabinetProfileWithdrawalQueue;
} {
  const trackedVolumeSun = normalizeSunString(stats.trackedVolumeSun);
  const claimableRewardsSun = normalizeSunString(stats.claimableRewardsSun);
  const lifetimeRewardsSun = normalizeSunString(stats.lifetimeRewardsSun);
  const withdrawnRewardsSun = normalizeSunString(stats.withdrawnRewardsSun);

  const availableOnChainSun = normalizeSunString(stats.availableOnChainSun);
  const pendingBackendSyncSun = normalizeSunString(stats.pendingBackendSyncSun);
  const requestedForProcessingSun = normalizeSunString(stats.requestedForProcessingSun);

  return {
    stats: {
      totalBuyers: Number(stats.totalBuyers || 0),
      trackedVolumeSun,
      trackedVolumeTrx: sunToTrxString(trackedVolumeSun),
      claimableRewardsSun,
      claimableRewardsTrx: sunToTrxString(claimableRewardsSun),
      lifetimeRewardsSun,
      lifetimeRewardsTrx: sunToTrxString(lifetimeRewardsSun),
      withdrawnRewardsSun,
      withdrawnRewardsTrx: sunToTrxString(withdrawnRewardsSun)
    },
    withdrawalQueue: {
      availableOnChainSun,
      availableOnChainTrx: sunToTrxString(availableOnChainSun),
      pendingBackendSyncSun,
      pendingBackendSyncTrx: sunToTrxString(pendingBackendSyncSun),
      requestedForProcessingSun,
      requestedForProcessingTrx: sunToTrxString(requestedForProcessingSun),
      availableOnChainCount: Number(stats.availableOnChainCount || 0),
      pendingBackendSyncCount: Number(stats.pendingBackendSyncCount || 0),
      requestedForProcessingCount: Number(stats.requestedForProcessingCount || 0),
      hasProcessingWithdrawal: Boolean(stats.hasProcessingWithdrawal)
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
    const level = inferLevel(mapped.stats.totalBuyers);
    const rewardPercent = inferRewardPercent(level);

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
