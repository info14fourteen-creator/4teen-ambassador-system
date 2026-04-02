import { query } from "./postgres";

export type DashboardSnapshotSyncStatus =
  | "success"
  | "partial"
  | "failed";

export interface AmbassadorDashboardSnapshotRecord {
  wallet: string;
  slug: string | null;
  registryStatus: string | null;

  existsOnChain: boolean;
  activeOnChain: boolean;
  selfRegistered: boolean;
  manualAssigned: boolean;
  overrideEnabled: boolean;

  level: number;
  effectiveLevel: number;
  currentLevel: number;
  overrideLevel: number;
  rewardPercent: number;

  createdAtOnChain: number | null;
  slugHash: string | null;
  metaHash: string | null;

  totalBuyers: number;
  trackedVolumeSun: string;
  claimableRewardsSun: string;
  lifetimeRewardsSun: string;
  withdrawnRewardsSun: string;

  nextThreshold: number;
  remainingToNextLevel: number;

  rawCoreJson: string | null;
  rawProfileJson: string | null;
  rawProgressJson: string | null;
  rawStatsJson: string | null;

  syncStatus: DashboardSnapshotSyncStatus;
  syncError: string | null;
  lastSyncedAt: number;
  createdAt: number;
  updatedAt: number;
}

export interface UpsertAmbassadorDashboardSnapshotInput {
  wallet: string;
  slug?: string | null;
  registryStatus?: string | null;

  existsOnChain?: boolean;
  activeOnChain?: boolean;
  selfRegistered?: boolean;
  manualAssigned?: boolean;
  overrideEnabled?: boolean;

  level?: number;
  effectiveLevel?: number;
  currentLevel?: number;
  overrideLevel?: number;
  rewardPercent?: number;

  createdAtOnChain?: number | null;
  slugHash?: string | null;
  metaHash?: string | null;

  totalBuyers?: number;
  trackedVolumeSun?: string;
  claimableRewardsSun?: string;
  lifetimeRewardsSun?: string;
  withdrawnRewardsSun?: string;

  nextThreshold?: number;
  remainingToNextLevel?: number;

  rawCoreJson?: string | null;
  rawProfileJson?: string | null;
  rawProgressJson?: string | null;
  rawStatsJson?: string | null;

  syncStatus?: DashboardSnapshotSyncStatus;
  syncError?: string | null;
  lastSyncedAt?: number;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeOptionalString(value: unknown): string | null {
  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized || null;
}

function normalizeBoolean(value: unknown, fallback = false): boolean {
  if (value == null) {
    return fallback;
  }

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

    if (!normalized) return fallback;
    if (normalized === "true") return true;
    if (normalized === "false") return false;
    if (normalized === "1") return true;
    if (normalized === "0") return false;
  }

  return Boolean(value);
}

function normalizeInteger(
  value: unknown,
  fieldName: string,
  fallback = 0
): number {
  if (value == null || value === "") {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed)) {
    throw new Error(`${fieldName} must be a finite number`);
  }

  return Math.floor(parsed);
}

function normalizeNonNegativeInteger(
  value: unknown,
  fieldName: string,
  fallback = 0
): number {
  const parsed = normalizeInteger(value, fieldName, fallback);

  if (parsed < 0) {
    throw new Error(`${fieldName} must be a non-negative integer`);
  }

  return parsed;
}

function normalizeTimestamp(
  value: unknown,
  fieldName: string
): number | null {
  if (value == null || value === "") {
    return null;
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${fieldName} must be a non-negative timestamp`);
  }

  return Math.floor(parsed);
}

function normalizeSunAmount(value: unknown, fieldName: string): string {
  if (value == null || value === "") {
    return "0";
  }

  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${fieldName} must be a non-negative integer string`);
  }

  return normalized;
}

function normalizeSyncStatus(value: unknown): DashboardSnapshotSyncStatus {
  const normalized = String(value || "").trim().toLowerCase();

  if (normalized === "success") {
    return "success";
  }

  if (normalized === "partial") {
    return "partial";
  }

  if (normalized === "failed") {
    return "failed";
  }

  return "success";
}

function safeJsonStringify(value: unknown): string | null {
  if (value == null) {
    return null;
  }

  if (typeof value === "string") {
    const normalized = value.trim();
    return normalized || null;
  }

  try {
    return JSON.stringify(value);
  } catch {
    return null;
  }
}

function rowToSnapshotRecord(row: any): AmbassadorDashboardSnapshotRecord {
  return {
    wallet: String(row.wallet),
    slug: normalizeOptionalString(row.slug),
    registryStatus: normalizeOptionalString(row.registry_status),

    existsOnChain: normalizeBoolean(row.exists_on_chain, false),
    activeOnChain: normalizeBoolean(row.active_on_chain, false),
    selfRegistered: normalizeBoolean(row.self_registered, false),
    manualAssigned: normalizeBoolean(row.manual_assigned, false),
    overrideEnabled: normalizeBoolean(row.override_enabled, false),

    level: normalizeNonNegativeInteger(row.level, "level"),
    effectiveLevel: normalizeNonNegativeInteger(row.effective_level, "effectiveLevel"),
    currentLevel: normalizeNonNegativeInteger(row.current_level, "currentLevel"),
    overrideLevel: normalizeNonNegativeInteger(row.override_level, "overrideLevel"),
    rewardPercent: normalizeNonNegativeInteger(row.reward_percent, "rewardPercent"),

    createdAtOnChain: normalizeTimestamp(row.created_at_on_chain, "createdAtOnChain"),
    slugHash: normalizeOptionalString(row.slug_hash),
    metaHash: normalizeOptionalString(row.meta_hash),

    totalBuyers: normalizeNonNegativeInteger(row.total_buyers, "totalBuyers"),
    trackedVolumeSun: normalizeSunAmount(row.tracked_volume_sun, "trackedVolumeSun"),
    claimableRewardsSun: normalizeSunAmount(row.claimable_rewards_sun, "claimableRewardsSun"),
    lifetimeRewardsSun: normalizeSunAmount(row.lifetime_rewards_sun, "lifetimeRewardsSun"),
    withdrawnRewardsSun: normalizeSunAmount(row.withdrawn_rewards_sun, "withdrawnRewardsSun"),

    nextThreshold: normalizeNonNegativeInteger(row.next_threshold, "nextThreshold"),
    remainingToNextLevel: normalizeNonNegativeInteger(
      row.remaining_to_next_level,
      "remainingToNextLevel"
    ),

    rawCoreJson: normalizeOptionalString(row.raw_core_json),
    rawProfileJson: normalizeOptionalString(row.raw_profile_json),
    rawProgressJson: normalizeOptionalString(row.raw_progress_json),
    rawStatsJson: normalizeOptionalString(row.raw_stats_json),

    syncStatus: normalizeSyncStatus(row.sync_status),
    syncError: normalizeOptionalString(row.sync_error),
    lastSyncedAt: normalizeNonNegativeInteger(row.last_synced_at, "lastSyncedAt"),
    createdAt: normalizeNonNegativeInteger(row.created_at_ms, "createdAt"),
    updatedAt: normalizeNonNegativeInteger(row.updated_at_ms, "updatedAt")
  };
}

function buildSelectSql(): string {
  return `
    SELECT
      wallet,
      slug,
      registry_status,
      exists_on_chain,
      active_on_chain,
      self_registered,
      manual_assigned,
      override_enabled,
      level,
      effective_level,
      current_level,
      override_level,
      reward_percent,
      created_at_on_chain,
      slug_hash,
      meta_hash,
      total_buyers,
      tracked_volume_sun,
      claimable_rewards_sun,
      lifetime_rewards_sun,
      withdrawn_rewards_sun,
      next_threshold,
      remaining_to_next_level,
      raw_core_json,
      raw_profile_json,
      raw_progress_json,
      raw_stats_json,
      sync_status,
      sync_error,
      last_synced_at,
      FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
      FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms
    FROM ambassador_dashboard_snapshots
  `;
}

export async function initDashboardSnapshotTables(): Promise<void> {
  await query(`
    CREATE TABLE IF NOT EXISTS ambassador_dashboard_snapshots (
      wallet TEXT PRIMARY KEY,
      slug TEXT NULL,
      registry_status TEXT NULL,

      exists_on_chain BOOLEAN NOT NULL DEFAULT FALSE,
      active_on_chain BOOLEAN NOT NULL DEFAULT FALSE,
      self_registered BOOLEAN NOT NULL DEFAULT FALSE,
      manual_assigned BOOLEAN NOT NULL DEFAULT FALSE,
      override_enabled BOOLEAN NOT NULL DEFAULT FALSE,

      level INTEGER NOT NULL DEFAULT 0,
      effective_level INTEGER NOT NULL DEFAULT 0,
      current_level INTEGER NOT NULL DEFAULT 0,
      override_level INTEGER NOT NULL DEFAULT 0,
      reward_percent INTEGER NOT NULL DEFAULT 0,

      created_at_on_chain BIGINT NULL,
      slug_hash TEXT NULL,
      meta_hash TEXT NULL,

      total_buyers INTEGER NOT NULL DEFAULT 0,
      tracked_volume_sun TEXT NOT NULL DEFAULT '0',
      claimable_rewards_sun TEXT NOT NULL DEFAULT '0',
      lifetime_rewards_sun TEXT NOT NULL DEFAULT '0',
      withdrawn_rewards_sun TEXT NOT NULL DEFAULT '0',

      next_threshold INTEGER NOT NULL DEFAULT 0,
      remaining_to_next_level INTEGER NOT NULL DEFAULT 0,

      raw_core_json TEXT NULL,
      raw_profile_json TEXT NULL,
      raw_progress_json TEXT NULL,
      raw_stats_json TEXT NULL,

      sync_status TEXT NOT NULL DEFAULT 'success',
      sync_error TEXT NULL,
      last_synced_at BIGINT NOT NULL DEFAULT 0,

      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_dashboard_snapshots_slug
    ON ambassador_dashboard_snapshots(slug)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_dashboard_snapshots_registry_status
    ON ambassador_dashboard_snapshots(registry_status)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_dashboard_snapshots_last_synced_at
    ON ambassador_dashboard_snapshots(last_synced_at)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_dashboard_snapshots_sync_status
    ON ambassador_dashboard_snapshots(sync_status)
  `);
}

export async function getDashboardSnapshotByWallet(
  wallet: string
): Promise<AmbassadorDashboardSnapshotRecord | null> {
  const normalizedWallet = assertNonEmpty(wallet, "wallet");

  const result = await query(
    `
      ${buildSelectSql()}
      WHERE wallet = $1
      LIMIT 1
    `,
    [normalizedWallet]
  );

  const row = result.rows[0];
  return row ? rowToSnapshotRecord(row) : null;
}

function mergeSnapshotState(
  existing: AmbassadorDashboardSnapshotRecord | null,
  input: UpsertAmbassadorDashboardSnapshotInput
) {
  return {
    wallet: assertNonEmpty(input.wallet, "wallet"),
    slug:
      input.slug !== undefined
        ? normalizeOptionalString(input.slug)
        : existing?.slug ?? null,
    registryStatus:
      input.registryStatus !== undefined
        ? normalizeOptionalString(input.registryStatus)
        : existing?.registryStatus ?? null,

    existsOnChain:
      input.existsOnChain !== undefined
        ? normalizeBoolean(input.existsOnChain, false)
        : existing?.existsOnChain ?? false,
    activeOnChain:
      input.activeOnChain !== undefined
        ? normalizeBoolean(input.activeOnChain, false)
        : existing?.activeOnChain ?? false,
    selfRegistered:
      input.selfRegistered !== undefined
        ? normalizeBoolean(input.selfRegistered, false)
        : existing?.selfRegistered ?? false,
    manualAssigned:
      input.manualAssigned !== undefined
        ? normalizeBoolean(input.manualAssigned, false)
        : existing?.manualAssigned ?? false,
    overrideEnabled:
      input.overrideEnabled !== undefined
        ? normalizeBoolean(input.overrideEnabled, false)
        : existing?.overrideEnabled ?? false,

    level:
      input.level !== undefined
        ? normalizeNonNegativeInteger(input.level, "level")
        : existing?.level ?? 0,
    effectiveLevel:
      input.effectiveLevel !== undefined
        ? normalizeNonNegativeInteger(input.effectiveLevel, "effectiveLevel")
        : existing?.effectiveLevel ?? 0,
    currentLevel:
      input.currentLevel !== undefined
        ? normalizeNonNegativeInteger(input.currentLevel, "currentLevel")
        : existing?.currentLevel ?? 0,
    overrideLevel:
      input.overrideLevel !== undefined
        ? normalizeNonNegativeInteger(input.overrideLevel, "overrideLevel")
        : existing?.overrideLevel ?? 0,
    rewardPercent:
      input.rewardPercent !== undefined
        ? normalizeNonNegativeInteger(input.rewardPercent, "rewardPercent")
        : existing?.rewardPercent ?? 0,

    createdAtOnChain:
      input.createdAtOnChain !== undefined
        ? normalizeTimestamp(input.createdAtOnChain, "createdAtOnChain")
        : existing?.createdAtOnChain ?? null,
    slugHash:
      input.slugHash !== undefined
        ? normalizeOptionalString(input.slugHash)
        : existing?.slugHash ?? null,
    metaHash:
      input.metaHash !== undefined
        ? normalizeOptionalString(input.metaHash)
        : existing?.metaHash ?? null,

    totalBuyers:
      input.totalBuyers !== undefined
        ? normalizeNonNegativeInteger(input.totalBuyers, "totalBuyers")
        : existing?.totalBuyers ?? 0,
    trackedVolumeSun:
      input.trackedVolumeSun !== undefined
        ? normalizeSunAmount(input.trackedVolumeSun, "trackedVolumeSun")
        : existing?.trackedVolumeSun ?? "0",
    claimableRewardsSun:
      input.claimableRewardsSun !== undefined
        ? normalizeSunAmount(input.claimableRewardsSun, "claimableRewardsSun")
        : existing?.claimableRewardsSun ?? "0",
    lifetimeRewardsSun:
      input.lifetimeRewardsSun !== undefined
        ? normalizeSunAmount(input.lifetimeRewardsSun, "lifetimeRewardsSun")
        : existing?.lifetimeRewardsSun ?? "0",
    withdrawnRewardsSun:
      input.withdrawnRewardsSun !== undefined
        ? normalizeSunAmount(input.withdrawnRewardsSun, "withdrawnRewardsSun")
        : existing?.withdrawnRewardsSun ?? "0",

    nextThreshold:
      input.nextThreshold !== undefined
        ? normalizeNonNegativeInteger(input.nextThreshold, "nextThreshold")
        : existing?.nextThreshold ?? 0,
    remainingToNextLevel:
      input.remainingToNextLevel !== undefined
        ? normalizeNonNegativeInteger(
            input.remainingToNextLevel,
            "remainingToNextLevel"
          )
        : existing?.remainingToNextLevel ?? 0,

    rawCoreJson:
      input.rawCoreJson !== undefined
        ? safeJsonStringify(input.rawCoreJson)
        : existing?.rawCoreJson ?? null,
    rawProfileJson:
      input.rawProfileJson !== undefined
        ? safeJsonStringify(input.rawProfileJson)
        : existing?.rawProfileJson ?? null,
    rawProgressJson:
      input.rawProgressJson !== undefined
        ? safeJsonStringify(input.rawProgressJson)
        : existing?.rawProgressJson ?? null,
    rawStatsJson:
      input.rawStatsJson !== undefined
        ? safeJsonStringify(input.rawStatsJson)
        : existing?.rawStatsJson ?? null,

    syncStatus:
      input.syncStatus !== undefined
        ? normalizeSyncStatus(input.syncStatus)
        : existing?.syncStatus ?? "success",
    syncError:
      input.syncError !== undefined
        ? normalizeOptionalString(input.syncError)
        : existing?.syncError ?? null,
    lastSyncedAt:
      normalizeTimestamp(
        input.lastSyncedAt !== undefined ? input.lastSyncedAt : existing?.lastSyncedAt ?? Date.now(),
        "lastSyncedAt"
      ) ?? Date.now()
  };
}

export async function upsertDashboardSnapshot(
  input: UpsertAmbassadorDashboardSnapshotInput
): Promise<AmbassadorDashboardSnapshotRecord> {
  const existing = await getDashboardSnapshotByWallet(assertNonEmpty(input.wallet, "wallet"));
  const merged = mergeSnapshotState(existing, input);

  const result = await query(
    `
      INSERT INTO ambassador_dashboard_snapshots (
        wallet,
        slug,
        registry_status,

        exists_on_chain,
        active_on_chain,
        self_registered,
        manual_assigned,
        override_enabled,

        level,
        effective_level,
        current_level,
        override_level,
        reward_percent,

        created_at_on_chain,
        slug_hash,
        meta_hash,

        total_buyers,
        tracked_volume_sun,
        claimable_rewards_sun,
        lifetime_rewards_sun,
        withdrawn_rewards_sun,

        next_threshold,
        remaining_to_next_level,

        raw_core_json,
        raw_profile_json,
        raw_progress_json,
        raw_stats_json,

        sync_status,
        sync_error,
        last_synced_at,
        updated_at
      )
      VALUES (
        $1,  $2,  $3,
        $4,  $5,  $6,  $7,  $8,
        $9,  $10, $11, $12, $13,
        $14, $15, $16,
        $17, $18, $19, $20, $21,
        $22, $23,
        $24, $25, $26, $27,
        $28, $29, $30,
        NOW()
      )
      ON CONFLICT (wallet)
      DO UPDATE SET
        slug = EXCLUDED.slug,
        registry_status = EXCLUDED.registry_status,

        exists_on_chain = EXCLUDED.exists_on_chain,
        active_on_chain = EXCLUDED.active_on_chain,
        self_registered = EXCLUDED.self_registered,
        manual_assigned = EXCLUDED.manual_assigned,
        override_enabled = EXCLUDED.override_enabled,

        level = EXCLUDED.level,
        effective_level = EXCLUDED.effective_level,
        current_level = EXCLUDED.current_level,
        override_level = EXCLUDED.override_level,
        reward_percent = EXCLUDED.reward_percent,

        created_at_on_chain = EXCLUDED.created_at_on_chain,
        slug_hash = EXCLUDED.slug_hash,
        meta_hash = EXCLUDED.meta_hash,

        total_buyers = EXCLUDED.total_buyers,
        tracked_volume_sun = EXCLUDED.tracked_volume_sun,
        claimable_rewards_sun = EXCLUDED.claimable_rewards_sun,
        lifetime_rewards_sun = EXCLUDED.lifetime_rewards_sun,
        withdrawn_rewards_sun = EXCLUDED.withdrawn_rewards_sun,

        next_threshold = EXCLUDED.next_threshold,
        remaining_to_next_level = EXCLUDED.remaining_to_next_level,

        raw_core_json = EXCLUDED.raw_core_json,
        raw_profile_json = EXCLUDED.raw_profile_json,
        raw_progress_json = EXCLUDED.raw_progress_json,
        raw_stats_json = EXCLUDED.raw_stats_json,

        sync_status = EXCLUDED.sync_status,
        sync_error = EXCLUDED.sync_error,
        last_synced_at = EXCLUDED.last_synced_at,
        updated_at = NOW()
      RETURNING
        wallet,
        slug,
        registry_status,
        exists_on_chain,
        active_on_chain,
        self_registered,
        manual_assigned,
        override_enabled,
        level,
        effective_level,
        current_level,
        override_level,
        reward_percent,
        created_at_on_chain,
        slug_hash,
        meta_hash,
        total_buyers,
        tracked_volume_sun,
        claimable_rewards_sun,
        lifetime_rewards_sun,
        withdrawn_rewards_sun,
        next_threshold,
        remaining_to_next_level,
        raw_core_json,
        raw_profile_json,
        raw_progress_json,
        raw_stats_json,
        sync_status,
        sync_error,
        last_synced_at,
        FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
        FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms
    `,
    [
      merged.wallet,
      merged.slug,
      merged.registryStatus,

      merged.existsOnChain,
      merged.activeOnChain,
      merged.selfRegistered,
      merged.manualAssigned,
      merged.overrideEnabled,

      merged.level,
      merged.effectiveLevel,
      merged.currentLevel,
      merged.overrideLevel,
      merged.rewardPercent,

      merged.createdAtOnChain,
      merged.slugHash,
      merged.metaHash,

      merged.totalBuyers,
      merged.trackedVolumeSun,
      merged.claimableRewardsSun,
      merged.lifetimeRewardsSun,
      merged.withdrawnRewardsSun,

      merged.nextThreshold,
      merged.remainingToNextLevel,

      merged.rawCoreJson,
      merged.rawProfileJson,
      merged.rawProgressJson,
      merged.rawStatsJson,

      merged.syncStatus,
      merged.syncError,
      merged.lastSyncedAt
    ]
  );

  return rowToSnapshotRecord(result.rows[0]);
}

export async function markDashboardSnapshotSyncFailed(input: {
  wallet: string;
  slug?: string | null;
  registryStatus?: string | null;
  syncError: string;
  syncStatus?: DashboardSnapshotSyncStatus;
  lastSyncedAt?: number;
}): Promise<AmbassadorDashboardSnapshotRecord> {
  return upsertDashboardSnapshot({
    wallet: input.wallet,
    slug: input.slug,
    registryStatus: input.registryStatus,
    syncStatus: input.syncStatus ?? "failed",
    syncError: assertNonEmpty(input.syncError, "syncError"),
    lastSyncedAt: input.lastSyncedAt ?? Date.now()
  });
}
