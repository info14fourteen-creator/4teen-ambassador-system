import {
  markDashboardSnapshotSyncFailed,
  upsertDashboardSnapshot,
  type AmbassadorDashboardSnapshotRecord
} from "../db/dashboardSnapshots";

export interface DashboardRefreshDependencies {
  tronWeb: any;
  controllerContractAddress: string;
  stepDelayMs?: number;
  minRefreshIntervalMs?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface RefreshWalletDashboardInput {
  wallet: string;
  slug: string;
  status: string;
  force?: boolean;
}

const ZERO_BYTES32 =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

const DEFAULT_STEP_DELAY_MS = 1200;
const DEFAULT_MIN_REFRESH_INTERVAL_MS = 25000;

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
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
    const message = String((error as { message: string }).message || "").trim();
    if (message) {
      return message;
    }
  }

  return "Unknown error";
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
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

export class DashboardRefreshService {
  private readonly tronWeb: any;
  private readonly controllerContractAddress: string;
  private readonly stepDelayMs: number;
  private readonly minRefreshIntervalMs: number;
  private readonly logger: Pick<Console, "info" | "warn" | "error">;
  private contractInstance: any | null = null;
  private readonly inflightByWallet = new Map<string, Promise<AmbassadorDashboardSnapshotRecord | null>>();
  private readonly lastAttemptAtByWallet = new Map<string, number>();

  constructor(deps: DashboardRefreshDependencies) {
    if (!deps?.tronWeb) {
      throw new Error("tronWeb is required");
    }

    this.tronWeb = deps.tronWeb;
    this.controllerContractAddress = assertNonEmpty(
      deps.controllerContractAddress,
      "controllerContractAddress"
    );
    this.stepDelayMs = Math.max(0, Number(deps.stepDelayMs ?? DEFAULT_STEP_DELAY_MS));
    this.minRefreshIntervalMs = Math.max(
      0,
      Number(deps.minRefreshIntervalMs ?? DEFAULT_MIN_REFRESH_INTERVAL_MS)
    );
    this.logger = deps.logger ?? console;
  }

  private logInfo(payload: Record<string, unknown>): void {
    this.logger.info?.(JSON.stringify({ level: "info", scope: "dashboard-refresh", ...payload }));
  }

  private logWarn(payload: Record<string, unknown>): void {
    this.logger.warn?.(JSON.stringify({ level: "warn", scope: "dashboard-refresh", ...payload }));
  }

  private logError(payload: Record<string, unknown>): void {
    this.logger.error?.(JSON.stringify({ level: "error", scope: "dashboard-refresh", ...payload }));
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

    return await method(wallet).call();
  }

  private async writeStepSnapshot(
    wallet: string,
    slug: string,
    status: string,
    patch: Record<string, unknown>
  ): Promise<AmbassadorDashboardSnapshotRecord> {
    return upsertDashboardSnapshot({
      wallet,
      slug,
      registryStatus: status,
      syncStatus: "partial",
      syncError: null,
      lastSyncedAt: Date.now(),
      ...patch
    });
  }

  async refreshWalletDashboard(
    input: RefreshWalletDashboardInput
  ): Promise<AmbassadorDashboardSnapshotRecord | null> {
    const wallet = assertNonEmpty(input.wallet, "wallet");
    const slug = assertNonEmpty(input.slug, "slug");
    const status = assertNonEmpty(input.status, "status");
    const now = Date.now();

    const existingInflight = this.inflightByWallet.get(wallet);

    if (existingInflight) {
      this.logInfo({
        stage: "refresh-ignored-inflight",
        wallet,
        slug,
        status
      });
      return existingInflight;
    }

    const lastAttemptAt = this.lastAttemptAtByWallet.get(wallet) ?? 0;

    if (!input.force && this.minRefreshIntervalMs > 0 && now - lastAttemptAt < this.minRefreshIntervalMs) {
      this.logInfo({
        stage: "refresh-ignored-rate-window",
        wallet,
        slug,
        status,
        retryAfterMs: this.minRefreshIntervalMs - (now - lastAttemptAt)
      });
      return null;
    }

    this.lastAttemptAtByWallet.set(wallet, now);

    const job = this.runRefresh(wallet, slug, status)
      .catch(async (error) => {
        const message = toErrorMessage(error);

        this.logError({
          stage: "refresh-failed",
          wallet,
          slug,
          status,
          error: message
        });

        await markDashboardSnapshotSyncFailed({
          wallet,
          slug,
          registryStatus: status,
          syncStatus: "failed",
          syncError: message,
          lastSyncedAt: Date.now()
        });

        return null;
      })
      .finally(() => {
        this.inflightByWallet.delete(wallet);
      });

    this.inflightByWallet.set(wallet, job);
    return job;
  }

  refreshWalletDashboardInBackground(input: RefreshWalletDashboardInput): void {
    void this.refreshWalletDashboard(input);
  }

  private async runRefresh(
    wallet: string,
    slug: string,
    status: string
  ): Promise<AmbassadorDashboardSnapshotRecord> {
    const contract = await this.contract();

    this.logInfo({
      stage: "refresh-started",
      wallet,
      slug,
      status
    });

    const coreRaw = await this.readContractTuple(contract, "getDashboardCore", wallet);

    let snapshot = await this.writeStepSnapshot(wallet, slug, status, {
      existsOnChain: safeBoolean(
        pickFirstDefined(coreRaw, [{ index: 0, keys: ["exists"] }])
      ),
      activeOnChain: safeBoolean(
        pickFirstDefined(coreRaw, [{ index: 1, keys: ["active"] }])
      ),
      level: safeNumber(
        pickFirstDefined(coreRaw, [{ index: 2, keys: ["effectiveLevel", "level"] }]),
        0
      ),
      effectiveLevel: safeNumber(
        pickFirstDefined(coreRaw, [{ index: 2, keys: ["effectiveLevel", "level"] }]),
        0
      ),
      rewardPercent: safeNumber(
        pickFirstDefined(coreRaw, [{ index: 3, keys: ["rewardPercent"] }]),
        0
      ),
      createdAtOnChain: safeNumber(
        pickFirstDefined(coreRaw, [{ index: 4, keys: ["createdAt"] }]),
        0
      ),
      rawCoreJson: coreRaw
    });

    this.logInfo({
      stage: "refresh-step-core-finished",
      wallet,
      slug,
      status
    });

    if (this.stepDelayMs > 0) {
      await delay(this.stepDelayMs);
    }

    const profileRaw = await this.readContractTuple(contract, "getDashboardProfile", wallet);

    snapshot = await this.writeStepSnapshot(wallet, slug, status, {
      selfRegistered: safeBoolean(
        pickFirstDefined(profileRaw, [{ index: 0, keys: ["selfRegistered"] }])
      ),
      manualAssigned: safeBoolean(
        pickFirstDefined(profileRaw, [{ index: 1, keys: ["manualAssigned"] }])
      ),
      overrideEnabled: safeBoolean(
        pickFirstDefined(profileRaw, [{ index: 2, keys: ["overrideEnabled"] }])
      ),
      currentLevel: safeNumber(
        pickFirstDefined(profileRaw, [{ index: 3, keys: ["currentLevel"] }]),
        0
      ),
      overrideLevel: safeNumber(
        pickFirstDefined(profileRaw, [{ index: 4, keys: ["overrideLevel"] }]),
        0
      ),
      slugHash: normalizeSlugHash(
        pickFirstDefined(profileRaw, [{ index: 5, keys: ["slugHash"] }])
      ),
      metaHash: normalizeMetaHash(
        pickFirstDefined(profileRaw, [{ index: 6, keys: ["metaHash"] }])
      ),
      rawProfileJson: profileRaw
    });

    this.logInfo({
      stage: "refresh-step-profile-finished",
      wallet,
      slug,
      status
    });

    if (this.stepDelayMs > 0) {
      await delay(this.stepDelayMs);
    }

    const progressRaw = await this.readContractTuple(
      contract,
      "getAmbassadorLevelProgress",
      wallet
    );

    snapshot = await this.writeStepSnapshot(wallet, slug, status, {
      currentLevel: safeNumber(
        pickFirstDefined(progressRaw, [{ index: 0, keys: ["currentLevel", "level"] }]),
        snapshot.currentLevel
      ),
      totalBuyers: safeNumber(
        pickFirstDefined(progressRaw, [{ index: 1, keys: ["buyersCount", "totalBuyers"] }]),
        snapshot.totalBuyers
      ),
      nextThreshold: safeNumber(
        pickFirstDefined(progressRaw, [{ index: 2, keys: ["nextThreshold"] }]),
        0
      ),
      remainingToNextLevel: safeNumber(
        pickFirstDefined(progressRaw, [{ index: 3, keys: ["remainingToNextLevel"] }]),
        0
      ),
      rawProgressJson: progressRaw
    });

    this.logInfo({
      stage: "refresh-step-progress-finished",
      wallet,
      slug,
      status
    });

    if (this.stepDelayMs > 0) {
      await delay(this.stepDelayMs);
    }

    const statsRaw = await this.readContractTuple(contract, "getDashboardStats", wallet);

    snapshot = await upsertDashboardSnapshot({
      wallet,
      slug,
      registryStatus: status,

      totalBuyers: safeNumber(
        pickFirstDefined(statsRaw, [{ index: 0, keys: ["totalBuyers"] }]),
        snapshot.totalBuyers
      ),
      trackedVolumeSun: toSunString(
        pickFirstDefined(statsRaw, [
          { index: 1, keys: ["trackedVolumeSun", "totalVolumeSun"] }
        ])
      ),
      lifetimeRewardsSun: toSunString(
        pickFirstDefined(statsRaw, [
          { index: 2, keys: ["lifetimeRewardsSun", "totalRewardsAccruedSun"] }
        ])
      ),
      withdrawnRewardsSun: toSunString(
        pickFirstDefined(statsRaw, [
          { index: 3, keys: ["withdrawnRewardsSun", "totalRewardsClaimedSun"] }
        ])
      ),
      claimableRewardsSun: toSunString(
        pickFirstDefined(statsRaw, [
          { index: 4, keys: ["claimableRewardsSun", "availableOnChainSun"] }
        ])
      ),
      rawStatsJson: statsRaw,

      syncStatus: "success",
      syncError: null,
      lastSyncedAt: Date.now()
    });

    this.logInfo({
      stage: "refresh-finished",
      wallet,
      slug,
      status,
      totalBuyers: snapshot.totalBuyers,
      trackedVolumeSun: snapshot.trackedVolumeSun,
      claimableRewardsSun: snapshot.claimableRewardsSun,
      lifetimeRewardsSun: snapshot.lifetimeRewardsSun,
      withdrawnRewardsSun: snapshot.withdrawnRewardsSun
    });

    return snapshot;
  }
}

export function createDashboardRefreshService(
  deps: DashboardRefreshDependencies
): DashboardRefreshService {
  return new DashboardRefreshService(deps);
}
