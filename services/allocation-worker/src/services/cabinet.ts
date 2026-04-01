import { getAmbassadorRegistryRecordByWallet } from "../db/ambassadors";
import {
  getDashboardSnapshotByWallet,
  markDashboardSnapshotSyncFailed,
  upsertDashboardSnapshot
} from "../db/dashboardSnapshots";
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

  /**
   * Backward-compatible aliases for older UI consumers.
   */
  totalVolumeSun: string;
  totalVolumeTrx: string;
  totalRewardsAccruedSun: string;
  totalRewardsAccruedTrx: string;
  totalRewardsClaimedSun: string;
  totalRewardsClaimedTrx: string;
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

function buildReferralLink(slug: string): string {
  return `https://4teen.me/?r=${encodeURIComponent(slug)}`;
}

function normalizeHex32(value: unknown): string {
  const raw = String(value ?? "").trim().toLowerCase();

  if (!raw) {
    return ZERO_BYTES32;
  }

  if (/^0x[0-9a-f]{64}$/.test(raw)) {
    return raw;
  }

  return ZERO_BYTES32;
}

function normalizeMetaHash(value: unknown): string | null {
  const raw = normalizeHex32(value);

  if (!raw || raw === ZERO_BYTES32) {
    return null;
  }

  return raw;
}

function normalizeSlugHash(value: unknown): string {
  const raw = normalizeHex32(value);
  return raw || ZERO_BYTES32;
}

function pickTupleValue(source: any, index: number, key?: string): unknown {
  if (Array.isArray(source) && source[index] !== undefined) {
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
    if (values[index] !== undefined) {
      return values[index];
    }
  }

  return undefined;
}

function pickFirstDefined(
  source: any,
  candidates: Array<{ index: number; keys?: string[] }>
): unknown {
  for (const candidate of candidates) {
    const keys = candidate.keys ?? [];
    for (const key of keys) {
      const value = pickTupleValue(source, candidate.index, key);
      if (value !== undefined && value !== null && value !== "") {
        return value;
      }
    }

    const fallbackValue = pickTupleValue(source, candidate.index);
    if (fallbackValue !== undefined && fallbackValue !== null && fallbackValue !== "") {
      return fallbackValue;
    }
  }

  return undefined;
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

  const trackedVolumeSun = onChainStats.trackedVolumeSun;
  const claimableRewardsSun = onChainStats.claimableRewardsSun;
  const lifetimeRewardsSun = onChainStats.lifetimeRewardsSun;
  const withdrawnRewardsSun = onChainStats.withdrawnRewardsSun;

  return {
    stats: {
      totalBuyers: safeNumber(onChainStats.totalBuyers),
      trackedVolumeSun,
      trackedVolumeTrx: sunToTrxString(trackedVolumeSun),
      claimableRewardsSun,
      claimableRewardsTrx: sunToTrxString(claimableRewardsSun),
      lifetimeRewardsSun,
      lifetimeRewardsTrx: sunToTrxString(lifetimeRewardsSun),
      withdrawnRewardsSun,
      withdrawnRewardsTrx: sunToTrxString(withdrawnRewardsSun),
      totalVolumeSun: trackedVolumeSun,
      totalVolumeTrx: sunToTrxString(trackedVolumeSun),
      totalRewardsAccruedSun: lifetimeRewardsSun,
      totalRewardsAccruedTrx: sunToTrxString(lifetimeRewardsSun),
      totalRewardsClaimedSun: withdrawnRewardsSun,
      totalRewardsClaimedTrx: sunToTrxString(withdrawnRewardsSun)
    },
    withdrawalQueue: {
      availableOnChainSun: claimableRewardsSun,
      availableOnChainTrx: sunToTrxString(claimableRewardsSun),
      availableOnChainCount: dbStats.availableOnChainCount,

      allocatedInDbSun: dbStats.allocatedInDbSun,
      allocatedInDbTrx: sunToTrxString(dbStats.allocatedInDbSun),
      allocatedInDbCount: dbStats.allocatedInDbCount,

      pendingBackendSyncSun: dbStats.pendingBackendSyncSun,
      pendingBackendSyncTrx: sunToTrxString(dbStats.pendingBackendSyncSun),
      pendingBackendSyncCount: dbStats.pendingBackendSyncCount,

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

function logJson(level: "info" | "warn" | "error", payload: Record<string, unknown>): void {
  const line = JSON.stringify({ level, scope: "cabinet", ...payload });

  if (level === "error") {
    console.error(line);
    return;
  }

  if (level === "warn") {
    console.warn(line);
    return;
  }

  console.log(line);
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

  private async readContractTuple(
    contract: any,
    methodName: string,
    wallet: string
  ): Promise<any> {
    const method = contract?.[methodName];

    if (typeof method !== "function") {
      throw new Error(`Controller contract method is missing: ${methodName}`);
    }

    try {
      return await method(wallet).call();
    } catch (error) {
      throw new Error(
        `${methodName}(${wallet}) failed: ${toErrorMessage(error)}`
      );
    }
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
    debug: {
      coreRaw: any;
      profileRaw: any;
      progressRaw: any;
      statsRaw: any;
    };
  }> {
    const contract = await this.contract();

    const coreRaw = await this.readContractTuple(contract, "getDashboardCore", wallet);
    const profileRaw = await this.readContractTuple(contract, "getDashboardProfile", wallet);
    const progressRaw = await this.readContractTuple(
      contract,
      "getAmbassadorLevelProgress",
      wallet
    );
    const statsRaw = await this.readContractTuple(contract, "getDashboardStats", wallet);

    const exists = safeBoolean(
      pickFirstDefined(coreRaw, [{ index: 0, keys: ["exists"] }])
    );

    const active = safeBoolean(
      pickFirstDefined(coreRaw, [{ index: 1, keys: ["active"] }])
    );

    const effectiveLevel = safeNumber(
      pickFirstDefined(coreRaw, [{ index: 2, keys: ["effectiveLevel", "level"] }]),
      0
    );

    const rewardPercent = safeNumber(
      pickFirstDefined(coreRaw, [{ index: 3, keys: ["rewardPercent"] }]),
      0
    );

    const createdAt = safeNumber(
      pickFirstDefined(coreRaw, [{ index: 4, keys: ["createdAt"] }]),
      0
    );

    const selfRegistered = safeBoolean(
      pickFirstDefined(profileRaw, [{ index: 0, keys: ["selfRegistered"] }])
    );

    const manualAssigned = safeBoolean(
      pickFirstDefined(profileRaw, [{ index: 1, keys: ["manualAssigned"] }])
    );

    const overrideEnabled = safeBoolean(
      pickFirstDefined(profileRaw, [{ index: 2, keys: ["overrideEnabled"] }])
    );

    const currentLevel = safeNumber(
      pickFirstDefined(profileRaw, [{ index: 3, keys: ["currentLevel"] }]),
      0
    );

    const overrideLevel = safeNumber(
      pickFirstDefined(profileRaw, [{ index: 4, keys: ["overrideLevel"] }]),
      0
    );

    const slugHash = normalizeSlugHash(
      pickFirstDefined(profileRaw, [{ index: 5, keys: ["slugHash"] }])
    );

    const metaHash = normalizeMetaHash(
      pickFirstDefined(profileRaw, [{ index: 6, keys: ["metaHash"] }])
    );

    const progressCurrentLevel = safeNumber(
      pickFirstDefined(progressRaw, [{ index: 0, keys: ["currentLevel", "level"] }]),
      currentLevel
    );

    const buyersCount = safeNumber(
      pickFirstDefined(progressRaw, [{ index: 1, keys: ["buyersCount", "totalBuyers"] }]),
      0
    );

    const nextThreshold = safeNumber(
      pickFirstDefined(progressRaw, [{ index: 2, keys: ["nextThreshold"] }]),
      0
    );

    const remainingToNextLevel = safeNumber(
      pickFirstDefined(progressRaw, [{ index: 3, keys: ["remainingToNextLevel"] }]),
      0
    );

    const totalBuyers = toSunString(
      pickFirstDefined(statsRaw, [{ index: 0, keys: ["totalBuyers"] }])
    );

    const trackedVolumeSun = toSunString(
      pickFirstDefined(statsRaw, [
        { index: 1, keys: ["trackedVolumeSun", "totalVolumeSun"] }
      ])
    );

    const lifetimeRewardsSun = toSunString(
      pickFirstDefined(statsRaw, [
        { index: 2, keys: ["lifetimeRewardsSun", "totalRewardsAccruedSun"] }
      ])
    );

    const withdrawnRewardsSun = toSunString(
      pickFirstDefined(statsRaw, [
        { index: 3, keys: ["withdrawnRewardsSun", "totalRewardsClaimedSun"] }
      ])
    );

    const claimableRewardsSun = toSunString(
      pickFirstDefined(statsRaw, [
        { index: 4, keys: ["claimableRewardsSun", "availableOnChainSun"] }
      ])
    );

    return {
      identity: {
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
      },
      progress: {
        currentLevel: progressCurrentLevel,
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
      },
      debug: {
        coreRaw,
        profileRaw,
        progressRaw,
        statsRaw
      }
    };
  }

  private async storeDashboardSnapshot(input: {
    wallet: string;
    slug: string;
    status: string;
    onChain: {
      identity: CabinetProfileIdentity;
      progress: CabinetProfileProgress;
      stats: {
        totalBuyers: string;
        trackedVolumeSun: string;
        claimableRewardsSun: string;
        lifetimeRewardsSun: string;
        withdrawnRewardsSun: string;
      };
      debug: {
        coreRaw: any;
        profileRaw: any;
        progressRaw: any;
        statsRaw: any;
      };
    };
  }): Promise<void> {
    await upsertDashboardSnapshot({
      wallet: input.wallet,
      slug: input.slug,
      registryStatus: input.status,

      existsOnChain: input.onChain.identity.exists,
      activeOnChain: input.onChain.identity.active,
      selfRegistered: input.onChain.identity.selfRegistered,
      manualAssigned: input.onChain.identity.manualAssigned,
      overrideEnabled: input.onChain.identity.overrideEnabled,

      level: input.onChain.identity.level,
      effectiveLevel: input.onChain.identity.effectiveLevel,
      currentLevel: input.onChain.identity.currentLevel,
      overrideLevel: input.onChain.identity.overrideLevel,
      rewardPercent: input.onChain.identity.rewardPercent,

      createdAtOnChain: input.onChain.identity.createdAt,
      slugHash: input.onChain.identity.slugHash,
      metaHash: input.onChain.identity.metaHash,

      totalBuyers: safeNumber(input.onChain.stats.totalBuyers, 0),
      trackedVolumeSun: input.onChain.stats.trackedVolumeSun,
      claimableRewardsSun: input.onChain.stats.claimableRewardsSun,
      lifetimeRewardsSun: input.onChain.stats.lifetimeRewardsSun,
      withdrawnRewardsSun: input.onChain.stats.withdrawnRewardsSun,

      nextThreshold: input.onChain.progress.nextThreshold,
      remainingToNextLevel: input.onChain.progress.remainingToNextLevel,

      rawCoreJson: input.onChain.debug.coreRaw,
      rawProfileJson: input.onChain.debug.profileRaw,
      rawProgressJson: input.onChain.debug.progressRaw,
      rawStatsJson: input.onChain.debug.statsRaw,

      syncStatus: "success",
      syncError: null,
      lastSyncedAt: Date.now()
    });
  }

  private async markSnapshotFailure(input: {
    wallet: string;
    slug: string;
    status: string;
    error: unknown;
  }): Promise<void> {
    try {
      await markDashboardSnapshotSyncFailed({
        wallet: input.wallet,
        slug: input.slug,
        registryStatus: input.status,
        syncError: toErrorMessage(input.error),
        syncStatus: "failed",
        lastSyncedAt: Date.now()
      });
    } catch (snapshotError) {
      logJson("warn", {
        stage: "snapshot-failure-write-failed",
        wallet: input.wallet,
        slug: input.slug,
        status: input.status,
        error: toErrorMessage(snapshotError)
      });
    }
  }

  private buildProfileFromSnapshot(input: {
    wallet: string;
    slug: string;
    status: string;
    dbStatsRecord: CabinetStatsRecord;
    snapshot: Awaited<ReturnType<typeof getDashboardSnapshotByWallet>>;
  }): CabinetProfileRegisteredResult {
    const { wallet, slug, status, dbStatsRecord, snapshot } = input;

    const onChainStats = {
      totalBuyers: String(snapshot?.totalBuyers ?? 0),
      trackedVolumeSun: snapshot?.trackedVolumeSun ?? "0",
      claimableRewardsSun: snapshot?.claimableRewardsSun ?? "0",
      lifetimeRewardsSun: snapshot?.lifetimeRewardsSun ?? "0",
      withdrawnRewardsSun: snapshot?.withdrawnRewardsSun ?? "0"
    };

    const mapped = mapStats({
      onChainStats,
      dbStats: dbStatsRecord
    });

    return {
      registered: true,
      wallet,
      slug,
      status,
      referralLink: buildReferralLink(slug),
      identity: {
        wallet,
        exists: safeBoolean(snapshot?.existsOnChain, true),
        active: status === "active" ? safeBoolean(snapshot?.activeOnChain, false) : false,
        selfRegistered: safeBoolean(snapshot?.selfRegistered, false),
        manualAssigned: safeBoolean(snapshot?.manualAssigned, false),
        overrideEnabled: safeBoolean(snapshot?.overrideEnabled, false),
        level: safeNumber(snapshot?.level, 0),
        effectiveLevel: safeNumber(snapshot?.effectiveLevel, 0),
        currentLevel: safeNumber(snapshot?.currentLevel, 0),
        overrideLevel: safeNumber(snapshot?.overrideLevel, 0),
        rewardPercent: safeNumber(snapshot?.rewardPercent, 0),
        createdAt: safeNumber(snapshot?.createdAtOnChain, 0),
        slugHash: normalizeSlugHash(snapshot?.slugHash),
        metaHash: normalizeMetaHash(snapshot?.metaHash)
      },
      stats: mapped.stats,
      withdrawalQueue: mapped.withdrawalQueue,
      progress: {
        currentLevel: safeNumber(snapshot?.currentLevel, 0),
        buyersCount: safeNumber(snapshot?.totalBuyers, 0),
        nextThreshold: safeNumber(snapshot?.nextThreshold, 0),
        remainingToNextLevel: safeNumber(snapshot?.remainingToNextLevel, 0)
      }
    };
  }

  private buildFallbackProfile(
    wallet: string,
    slug: string,
    status: string,
    dbStatsRecord: CabinetStatsRecord
  ): CabinetProfileRegisteredResult {
    const mapped = mapStats({
      onChainStats: {
        totalBuyers: String(dbStatsRecord.totalBuyers || 0),
        trackedVolumeSun: dbStatsRecord.trackedVolumeSun || "0",
        claimableRewardsSun: "0",
        lifetimeRewardsSun: dbStatsRecord.lifetimeRewardsSun || "0",
        withdrawnRewardsSun: dbStatsRecord.withdrawnRewardsSun || "0"
      },
      dbStats: dbStatsRecord
    });

    return {
      registered: true,
      wallet,
      slug,
      status,
      referralLink: buildReferralLink(slug),
      identity: {
        wallet,
        exists: true,
        active: status === "active",
        selfRegistered: false,
        manualAssigned: false,
        overrideEnabled: false,
        level: 0,
        effectiveLevel: 0,
        currentLevel: 0,
        overrideLevel: 0,
        rewardPercent: 0,
        createdAt: 0,
        slugHash: ZERO_BYTES32,
        metaHash: null
      },
      stats: mapped.stats,
      withdrawalQueue: mapped.withdrawalQueue,
      progress: {
        currentLevel: 0,
        buyersCount: dbStatsRecord.totalBuyers || 0,
        nextThreshold: 0,
        remainingToNextLevel: 0
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
    const slug = record.publicProfile.slug;
    const status = record.publicProfile.status;
    const dbStatsRecord = await this.store.getCabinetStatsByAmbassadorWallet(registryWallet);

    try {
      const onChain = await this.readOnChainDashboard(registryWallet);

      await this.storeDashboardSnapshot({
        wallet: registryWallet,
        slug,
        status,
        onChain
      });

      logJson("info", {
        stage: "onchain-dashboard-read-success",
        wallet: registryWallet,
        slug,
        status,
        controllerContractAddress: this.controllerContractAddress,
        identity: {
          exists: onChain.identity.exists,
          active: onChain.identity.active,
          effectiveLevel: onChain.identity.effectiveLevel,
          currentLevel: onChain.identity.currentLevel,
          rewardPercent: onChain.identity.rewardPercent,
          createdAt: onChain.identity.createdAt,
          slugHash: onChain.identity.slugHash,
          metaHash: onChain.identity.metaHash
        },
        progress: onChain.progress,
        stats: onChain.stats
      });

      const mapped = mapStats({
        onChainStats: onChain.stats,
        dbStats: dbStatsRecord
      });

      return {
        registered: true,
        wallet: registryWallet,
        slug,
        status,
        referralLink: buildReferralLink(slug),
        identity: {
          ...onChain.identity,
          active: status === "active" ? onChain.identity.active : false
        },
        stats: mapped.stats,
        withdrawalQueue: mapped.withdrawalQueue,
        progress: onChain.progress
      };
    } catch (error) {
      logJson("error", {
        stage: "onchain-dashboard-read-failed",
        wallet: registryWallet,
        slug,
        status,
        controllerContractAddress: this.controllerContractAddress,
        error: toErrorMessage(error)
      });

      await this.markSnapshotFailure({
        wallet: registryWallet,
        slug,
        status,
        error
      });

      const snapshot = await getDashboardSnapshotByWallet(registryWallet);

      if (snapshot) {
        logJson("warn", {
          stage: "using-dashboard-snapshot",
          wallet: registryWallet,
          slug,
          status,
          lastSyncedAt: snapshot.lastSyncedAt,
          syncStatus: snapshot.syncStatus,
          syncError: snapshot.syncError
        });

        return this.buildProfileFromSnapshot({
          wallet: registryWallet,
          slug,
          status,
          dbStatsRecord,
          snapshot
        });
      }

      logJson("warn", {
        stage: "using-db-fallback-without-snapshot",
        wallet: registryWallet,
        slug,
        status
      });

      return this.buildFallbackProfile(registryWallet, slug, status, dbStatsRecord);
    }
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
