import { query } from "./postgres";

export type PurchaseProcessingStatus =
  | "received"
  | "verified"
  | "deferred"
  | "allocation_in_progress"
  | "allocated"
  | "allocation_failed_retryable"
  | "allocation_failed_final"
  | "ignored";

export type PurchaseSource =
  | "frontend-attribution"
  | "event-scan"
  | "manual-replay"
  | "withdraw-prepare";

export type AllocationMode =
  | "eager"
  | "deferred"
  | "claim-first"
  | "maintenance-replay"
  | "manual-replay"
  | null;

export interface PurchaseRecord {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  purchaseAmountSun: string;
  ownerShareSun: string;
  status: PurchaseProcessingStatus;
  failureReason: string | null;
  source: PurchaseSource;
  allocationMode: AllocationMode;
  allocationAttempts: number;
  lastAllocationAttemptAt: number | null;
  lastAllocationErrorCode: string | null;
  lastAllocationErrorMessage: string | null;
  deferredReason: string | null;
  withdrawSessionId: string | null;
  createdAt: number;
  updatedAt: number;
  allocatedAt: number | null;
}

export interface AmbassadorStoreRecord {
  slug: string;
  slugHash: string;
  status: "pending" | "active" | "disabled";
  wallet: string | null;
  ambassadorId: string | null;
}

export interface CabinetStatsRecord {
  totalBuyers: number;
  trackedVolumeSun: string;
  claimableRewardsSun: string;
  lifetimeRewardsSun: string;
  withdrawnRewardsSun: string;
  availableOnChainSun: string;
  pendingBackendSyncSun: string;
  requestedForProcessingSun: string;
  availableOnChainCount: number;
  pendingBackendSyncCount: number;
  requestedForProcessingCount: number;
  hasProcessingWithdrawal: boolean;
}

export interface CreateOrGetReceivedPurchaseInput {
  txHash: string;
  buyerWallet: string;
  ambassadorSlug?: string | null;
  now?: number;
}

export interface CreateOrGetReceivedPurchaseResult {
  created: boolean;
  purchase: PurchaseRecord;
}

export interface AttachAmbassadorToPurchaseInput {
  purchaseId: string;
  ambassadorSlug: string;
  ambassadorWallet: string;
  purchaseAmountSun?: string;
  ownerShareSun?: string;
  now?: number;
}

export interface MarkVerifiedPurchaseInput {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  now?: number;
}

export interface CreatePurchaseRecordInput {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug?: string | null;
  ambassadorWallet?: string | null;
  purchaseAmountSun?: string;
  ownerShareSun?: string;
  source?: PurchaseSource;
  status?: PurchaseProcessingStatus;
  failureReason?: string | null;
  allocationMode?: AllocationMode;
  allocationAttempts?: number;
  lastAllocationAttemptAt?: number | null;
  lastAllocationErrorCode?: string | null;
  lastAllocationErrorMessage?: string | null;
  deferredReason?: string | null;
  withdrawSessionId?: string | null;
  allocatedAt?: number | null;
  now?: number;
}

export interface UpdatePurchaseRecordInput {
  purchaseAmountSun?: string;
  ownerShareSun?: string;
  ambassadorSlug?: string | null;
  ambassadorWallet?: string | null;
  status?: PurchaseProcessingStatus;
  failureReason?: string | null;
  allocationMode?: AllocationMode;
  allocationAttempts?: number;
  incrementAllocationAttempts?: boolean;
  lastAllocationAttemptAt?: number | null;
  lastAllocationErrorCode?: string | null;
  lastAllocationErrorMessage?: string | null;
  deferredReason?: string | null;
  withdrawSessionId?: string | null;
  allocatedAt?: number | null;
  now?: number;
}

export interface PendingPurchaseQuery {
  ambassadorWallet: string;
  statuses?: PurchaseProcessingStatus[];
  limit?: number;
}

export interface PurchaseStore {
  getByPurchaseId(purchaseId: string): Promise<PurchaseRecord | null>;
  getByTxHash(txHash: string): Promise<PurchaseRecord | null>;
  create(input: CreatePurchaseRecordInput): Promise<PurchaseRecord>;
  update(purchaseId: string, input: UpdatePurchaseRecordInput): Promise<PurchaseRecord>;

  getAmbassadorBySlug(slug: string): Promise<AmbassadorStoreRecord | null>;

  createOrGetReceivedPurchase(
    input: CreateOrGetReceivedPurchaseInput
  ): Promise<CreateOrGetReceivedPurchaseResult>;

  attachAmbassadorToPurchase(
    input: AttachAmbassadorToPurchaseInput
  ): Promise<PurchaseRecord>;

  markVerifiedPurchase(
    input: MarkVerifiedPurchaseInput
  ): Promise<PurchaseRecord>;

  markVerified(
    purchaseId: string,
    input: {
      purchaseAmountSun: string;
      ownerShareSun: string;
      ambassadorSlug?: string | null;
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markDeferred(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markAllocationInProgress(
    purchaseId: string,
    input?: {
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markAllocationRetryableFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markAllocationFinalFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markFailed(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord>;

  markIgnored(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord>;

  assignWithdrawSession(
    purchaseId: string,
    withdrawSessionId: string,
    now?: number
  ): Promise<PurchaseRecord>;

  clearWithdrawSession(
    purchaseId: string,
    now?: number
  ): Promise<PurchaseRecord>;

  listReplayableFailures(limit?: number): Promise<PurchaseRecord[]>;

  listPendingByAmbassador(
    input: PendingPurchaseQuery
  ): Promise<PurchaseRecord[]>;

  getCabinetStatsByAmbassadorWallet(
    ambassadorWallet: string
  ): Promise<CabinetStatsRecord>;

  hasProcessedPurchase(purchaseId: string): Promise<boolean>;
}

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

function normalizeOptionalString(value: string | null | undefined): string | null {
  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized || null;
}

function normalizeWallet(value: string | null | undefined): string | null {
  return normalizeOptionalString(value);
}

function normalizeSunAmount(value: string | number | bigint | undefined): string {
  if (value == null) {
    return "0";
  }

  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error("SUN amount must be a non-negative integer string");
  }

  return normalized;
}

function normalizeStatus(value?: PurchaseProcessingStatus): PurchaseProcessingStatus {
  return value ?? "received";
}

function normalizeSource(value?: PurchaseSource): PurchaseSource {
  return value ?? "frontend-attribution";
}

function normalizeAllocationMode(value?: AllocationMode | string | null): AllocationMode {
  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();

  if (!normalized) {
    return null;
  }

  if (
    normalized === "eager" ||
    normalized === "deferred" ||
    normalized === "claim-first" ||
    normalized === "maintenance-replay" ||
    normalized === "manual-replay"
  ) {
    return normalized;
  }

  return null;
}

function normalizeCount(value: number | undefined, fieldName: string): number {
  if (value == null) {
    return 0;
  }

  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${fieldName} must be a non-negative integer`);
  }

  return value;
}

function normalizeTimestamp(value: number | null | undefined, fieldName: string): number | null {
  if (value == null) {
    return null;
  }

  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${fieldName} must be a non-negative timestamp`);
  }

  return Math.floor(value);
}

function normalizePendingStatuses(
  statuses?: PurchaseProcessingStatus[]
): PurchaseProcessingStatus[] {
  const value = statuses?.length ? statuses : DEFAULT_PENDING_STATUSES;
  return [...new Set(value)];
}

function normalizeTxHash(value: string): string {
  return assertNonEmpty(value, "txHash").toLowerCase();
}

function buildPurchaseIdFromTxHash(txHash: string): string {
  const normalized = normalizeTxHash(txHash);

  if (/^0x[0-9a-f]{64}$/.test(normalized)) {
    return normalized;
  }

  if (/^[0-9a-f]{64}$/.test(normalized)) {
    return `0x${normalized}`;
  }

  return normalized;
}

function sumSunStrings(left: string, right: string): string {
  return (BigInt(left || "0") + BigInt(right || "0")).toString();
}

function emptyCabinetStatsRecord(): CabinetStatsRecord {
  return {
    totalBuyers: 0,
    trackedVolumeSun: "0",
    claimableRewardsSun: "0",
    lifetimeRewardsSun: "0",
    withdrawnRewardsSun: "0",
    availableOnChainSun: "0",
    pendingBackendSyncSun: "0",
    requestedForProcessingSun: "0",
    availableOnChainCount: 0,
    pendingBackendSyncCount: 0,
    requestedForProcessingCount: 0,
    hasProcessingWithdrawal: false
  };
}

function createRecord(input: CreatePurchaseRecordInput): PurchaseRecord {
  const now = input.now ?? Date.now();
  const status = normalizeStatus(input.status);
  const allocatedAt =
    input.allocatedAt !== undefined
      ? normalizeTimestamp(input.allocatedAt, "allocatedAt")
      : status === "allocated"
        ? now
        : null;

  return {
    purchaseId: assertNonEmpty(input.purchaseId, "purchaseId"),
    txHash: normalizeTxHash(input.txHash),
    buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
    ambassadorSlug: normalizeOptionalString(input.ambassadorSlug),
    ambassadorWallet: normalizeWallet(input.ambassadorWallet),
    purchaseAmountSun: normalizeSunAmount(input.purchaseAmountSun),
    ownerShareSun: normalizeSunAmount(input.ownerShareSun),
    status,
    failureReason: normalizeOptionalString(input.failureReason),
    source: normalizeSource(input.source),
    allocationMode: normalizeAllocationMode(input.allocationMode),
    allocationAttempts: normalizeCount(input.allocationAttempts, "allocationAttempts"),
    lastAllocationAttemptAt: normalizeTimestamp(
      input.lastAllocationAttemptAt,
      "lastAllocationAttemptAt"
    ),
    lastAllocationErrorCode: normalizeOptionalString(input.lastAllocationErrorCode),
    lastAllocationErrorMessage: normalizeOptionalString(input.lastAllocationErrorMessage),
    deferredReason: normalizeOptionalString(input.deferredReason),
    withdrawSessionId: normalizeOptionalString(input.withdrawSessionId),
    createdAt: now,
    updatedAt: now,
    allocatedAt
  };
}

function mergeRecord(
  current: PurchaseRecord,
  input: UpdatePurchaseRecordInput
): PurchaseRecord {
  const now = input.now ?? Date.now();
  const nextStatus = input.status ?? current.status;

  const nextAllocationAttempts =
    input.allocationAttempts !== undefined
      ? normalizeCount(input.allocationAttempts, "allocationAttempts")
      : input.incrementAllocationAttempts
        ? current.allocationAttempts + 1
        : current.allocationAttempts;

  const nextAllocatedAt =
    input.allocatedAt !== undefined
      ? normalizeTimestamp(input.allocatedAt, "allocatedAt")
      : nextStatus === "allocated"
        ? current.allocatedAt ?? now
        : current.allocatedAt;

  return {
    ...current,
    purchaseAmountSun:
      input.purchaseAmountSun !== undefined
        ? normalizeSunAmount(input.purchaseAmountSun)
        : current.purchaseAmountSun,
    ownerShareSun:
      input.ownerShareSun !== undefined
        ? normalizeSunAmount(input.ownerShareSun)
        : current.ownerShareSun,
    ambassadorSlug:
      input.ambassadorSlug !== undefined
        ? normalizeOptionalString(input.ambassadorSlug)
        : current.ambassadorSlug,
    ambassadorWallet:
      input.ambassadorWallet !== undefined
        ? normalizeWallet(input.ambassadorWallet)
        : current.ambassadorWallet,
    status: nextStatus,
    failureReason:
      input.failureReason !== undefined
        ? normalizeOptionalString(input.failureReason)
        : current.failureReason,
    allocationMode:
      input.allocationMode !== undefined
        ? normalizeAllocationMode(input.allocationMode)
        : current.allocationMode,
    allocationAttempts: nextAllocationAttempts,
    lastAllocationAttemptAt:
      input.lastAllocationAttemptAt !== undefined
        ? normalizeTimestamp(input.lastAllocationAttemptAt, "lastAllocationAttemptAt")
        : current.lastAllocationAttemptAt,
    lastAllocationErrorCode:
      input.lastAllocationErrorCode !== undefined
        ? normalizeOptionalString(input.lastAllocationErrorCode)
        : current.lastAllocationErrorCode,
    lastAllocationErrorMessage:
      input.lastAllocationErrorMessage !== undefined
        ? normalizeOptionalString(input.lastAllocationErrorMessage)
        : current.lastAllocationErrorMessage,
    deferredReason:
      input.deferredReason !== undefined
        ? normalizeOptionalString(input.deferredReason)
        : current.deferredReason,
    withdrawSessionId:
      input.withdrawSessionId !== undefined
        ? normalizeOptionalString(input.withdrawSessionId)
        : current.withdrawSessionId,
    allocatedAt: nextAllocatedAt,
    updatedAt: now
  };
}

function rowToPurchaseRecord(row: any): PurchaseRecord {
  return {
    purchaseId: String(row.purchase_id),
    txHash: String(row.tx_hash),
    buyerWallet: String(row.buyer_wallet),
    ambassadorSlug: normalizeOptionalString(row.ambassador_slug),
    ambassadorWallet: normalizeWallet(row.ambassador_wallet),
    purchaseAmountSun: String(row.purchase_amount_sun),
    ownerShareSun: String(row.owner_share_sun),
    status: String(row.status) as PurchaseProcessingStatus,
    failureReason: normalizeOptionalString(row.failure_reason),
    source: String(row.source) as PurchaseSource,
    allocationMode: normalizeAllocationMode(row.allocation_mode),
    allocationAttempts: Number(row.allocation_attempts || 0),
    lastAllocationAttemptAt:
      row.last_allocation_attempt_at_ms == null
        ? null
        : Number(row.last_allocation_attempt_at_ms),
    lastAllocationErrorCode: normalizeOptionalString(row.last_allocation_error_code),
    lastAllocationErrorMessage: normalizeOptionalString(
      row.last_allocation_error_message
    ),
    deferredReason: normalizeOptionalString(row.deferred_reason),
    withdrawSessionId: normalizeOptionalString(row.withdraw_session_id),
    createdAt: Number(row.created_at_ms),
    updatedAt: Number(row.updated_at_ms),
    allocatedAt: row.allocated_at_ms == null ? null : Number(row.allocated_at_ms)
  };
}

function rowToCabinetStatsRecord(row: any): CabinetStatsRecord {
  const requestedCount = Number(row.requested_for_processing_count || 0);

  return {
    totalBuyers: Number(row.total_buyers || 0),
    trackedVolumeSun: String(row.tracked_volume_sun || "0"),
    claimableRewardsSun: String(row.claimable_rewards_sun || "0"),
    lifetimeRewardsSun: String(row.lifetime_rewards_sun || "0"),
    withdrawnRewardsSun: String(row.withdrawn_rewards_sun || "0"),
    availableOnChainSun: String(row.available_on_chain_sun || "0"),
    pendingBackendSyncSun: String(row.pending_backend_sync_sun || "0"),
    requestedForProcessingSun: String(row.requested_for_processing_sun || "0"),
    availableOnChainCount: Number(row.available_on_chain_count || 0),
    pendingBackendSyncCount: Number(row.pending_backend_sync_count || 0),
    requestedForProcessingCount: requestedCount,
    hasProcessingWithdrawal: requestedCount > 0
  };
}

function mapPgConflict(error: unknown): Error {
  const isPgUniqueViolation =
    !!error &&
    typeof error === "object" &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505";

  if (isPgUniqueViolation) {
    const constraint =
      "constraint" in (error as Record<string, unknown>) &&
      typeof (error as Record<string, unknown>).constraint === "string"
        ? String((error as Record<string, unknown>).constraint)
        : "";

    if (constraint.includes("purchase_id")) {
      return new Error("Purchase already exists for purchaseId");
    }

    if (constraint.includes("tx_hash")) {
      return new Error("Purchase already exists for txHash");
    }

    return new Error("Purchase already exists");
  }

  return error instanceof Error ? error : new Error("Purchase store error");
}

function buildSelectSql(): string {
  return `
    SELECT
      purchase_id,
      tx_hash,
      buyer_wallet,
      ambassador_slug,
      ambassador_wallet,
      purchase_amount_sun,
      owner_share_sun,
      status,
      failure_reason,
      source,
      allocation_mode,
      allocation_attempts,
      CASE
        WHEN last_allocation_attempt_at IS NULL THEN NULL
        ELSE FLOOR(EXTRACT(EPOCH FROM last_allocation_attempt_at) * 1000)
      END AS last_allocation_attempt_at_ms,
      last_allocation_error_code,
      last_allocation_error_message,
      deferred_reason,
      withdraw_session_id,
      FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
      FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms,
      CASE
        WHEN allocated_at IS NULL THEN NULL
        ELSE FLOOR(EXTRACT(EPOCH FROM allocated_at) * 1000)
      END AS allocated_at_ms
    FROM purchases
  `;
}

export async function initPurchaseTables(): Promise<void> {
  await query(`
    CREATE TABLE IF NOT EXISTS purchases (
      purchase_id TEXT PRIMARY KEY,
      tx_hash TEXT NOT NULL UNIQUE,
      buyer_wallet TEXT NOT NULL,
      ambassador_slug TEXT NULL,
      ambassador_wallet TEXT NULL,
      purchase_amount_sun TEXT NOT NULL DEFAULT '0',
      owner_share_sun TEXT NOT NULL DEFAULT '0',
      status TEXT NOT NULL,
      failure_reason TEXT NULL,
      source TEXT NOT NULL,
      allocation_mode TEXT NULL,
      allocation_attempts INTEGER NOT NULL DEFAULT 0,
      last_allocation_attempt_at TIMESTAMPTZ NULL,
      last_allocation_error_code TEXT NULL,
      last_allocation_error_message TEXT NULL,
      deferred_reason TEXT NULL,
      withdraw_session_id TEXT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      allocated_at TIMESTAMPTZ NULL
    )
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS allocation_mode TEXT NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS allocation_attempts INTEGER NOT NULL DEFAULT 0
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS last_allocation_attempt_at TIMESTAMPTZ NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS last_allocation_error_code TEXT NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS last_allocation_error_message TEXT NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS deferred_reason TEXT NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS withdraw_session_id TEXT NULL
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_tx_hash
    ON purchases(tx_hash)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_status
    ON purchases(status)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_buyer_wallet
    ON purchases(buyer_wallet)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_ambassador_slug
    ON purchases(ambassador_slug)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_ambassador_wallet
    ON purchases(ambassador_wallet)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_status_ambassador_wallet_created_at
    ON purchases(status, ambassador_wallet, created_at)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_withdraw_session_id
    ON purchases(withdraw_session_id)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_retry_queue
    ON purchases(status, updated_at)
  `);
}

async function getAmbassadorBySlugFromDb(slug: string): Promise<AmbassadorStoreRecord | null> {
  const normalizedSlug = assertNonEmpty(slug, "slug").toLowerCase();

  const result = await query(
    `
      SELECT
        p.id,
        p.slug,
        p.slug_hash,
        p.status,
        i.wallet
      FROM ambassador_public_profiles p
      LEFT JOIN ambassador_private_identities i
        ON i.ambassador_id = p.id
      WHERE p.slug = $1
      LIMIT 1
    `,
    [normalizedSlug]
  );

  const row = result.rows[0];

  if (!row) {
    return null;
  }

  return {
    slug: String(row.slug),
    slugHash: String(row.slug_hash),
    status: String(row.status) as AmbassadorStoreRecord["status"],
    wallet: normalizeWallet(row.wallet),
    ambassadorId: row.id == null ? null : String(row.id)
  };
}

export class PostgresPurchaseStore implements PurchaseStore {
  async getByPurchaseId(purchaseId: string): Promise<PurchaseRecord | null> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");

    const result = await query(
      `
        ${buildSelectSql()}
        WHERE purchase_id = $1
        LIMIT 1
      `,
      [normalizedPurchaseId]
    );

    const row = result.rows[0];
    return row ? rowToPurchaseRecord(row) : null;
  }

  async getByTxHash(txHash: string): Promise<PurchaseRecord | null> {
    const normalizedTxHash = normalizeTxHash(txHash);

    const result = await query(
      `
        ${buildSelectSql()}
        WHERE tx_hash = $1
        LIMIT 1
      `,
      [normalizedTxHash]
    );

    const row = result.rows[0];
    return row ? rowToPurchaseRecord(row) : null;
  }

  async getAmbassadorBySlug(slug: string): Promise<AmbassadorStoreRecord | null> {
    return getAmbassadorBySlugFromDb(slug);
  }

  async createOrGetReceivedPurchase(
    input: CreateOrGetReceivedPurchaseInput
  ): Promise<CreateOrGetReceivedPurchaseResult> {
    const txHash = normalizeTxHash(input.txHash);
    const existing = await this.getByTxHash(txHash);

    if (existing) {
      return {
        created: false,
        purchase: existing
      };
    }

    try {
      const created = await this.create({
        purchaseId: buildPurchaseIdFromTxHash(txHash),
        txHash,
        buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
        ambassadorSlug: normalizeOptionalString(input.ambassadorSlug),
        purchaseAmountSun: "0",
        ownerShareSun: "0",
        source: "frontend-attribution",
        status: "received",
        now: input.now
      });

      return {
        created: true,
        purchase: created
      };
    } catch (error) {
      const fallback = await this.getByTxHash(txHash);

      if (fallback) {
        return {
          created: false,
          purchase: fallback
        };
      }

      throw error;
    }
  }

  async attachAmbassadorToPurchase(
    input: AttachAmbassadorToPurchaseInput
  ): Promise<PurchaseRecord> {
    return this.update(input.purchaseId, {
      ambassadorSlug: input.ambassadorSlug,
      ambassadorWallet: input.ambassadorWallet,
      purchaseAmountSun: input.purchaseAmountSun ?? "0",
      ownerShareSun: input.ownerShareSun ?? "0",
      now: input.now
    });
  }

  async markVerifiedPurchase(
    input: MarkVerifiedPurchaseInput
  ): Promise<PurchaseRecord> {
    const current = await this.getByPurchaseId(input.purchaseId);

    if (!current) {
      throw new Error(`Purchase not found: ${input.purchaseId}`);
    }

    return this.markVerified(input.purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorSlug: current.ambassadorSlug,
      ambassadorWallet: current.ambassadorWallet,
      allocationMode: current.allocationMode,
      now: input.now
    });
  }

  async create(input: CreatePurchaseRecordInput): Promise<PurchaseRecord> {
    const record = createRecord(input);

    try {
      const result = await query(
        `
          INSERT INTO purchases (
            purchase_id,
            tx_hash,
            buyer_wallet,
            ambassador_slug,
            ambassador_wallet,
            purchase_amount_sun,
            owner_share_sun,
            status,
            failure_reason,
            source,
            allocation_mode,
            allocation_attempts,
            last_allocation_attempt_at,
            last_allocation_error_code,
            last_allocation_error_message,
            deferred_reason,
            withdraw_session_id,
            created_at,
            updated_at,
            allocated_at
          )
          VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12,
            CASE WHEN $13::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($13 / 1000.0) END,
            $14, $15, $16, $17,
            TO_TIMESTAMP($18 / 1000.0),
            TO_TIMESTAMP($19 / 1000.0),
            CASE WHEN $20::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($20 / 1000.0) END
          )
          RETURNING
            purchase_id,
            tx_hash,
            buyer_wallet,
            ambassador_slug,
            ambassador_wallet,
            purchase_amount_sun,
            owner_share_sun,
            status,
            failure_reason,
            source,
            allocation_mode,
            allocation_attempts,
            CASE
              WHEN last_allocation_attempt_at IS NULL THEN NULL
              ELSE FLOOR(EXTRACT(EPOCH FROM last_allocation_attempt_at) * 1000)
            END AS last_allocation_attempt_at_ms,
            last_allocation_error_code,
            last_allocation_error_message,
            deferred_reason,
            withdraw_session_id,
            FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
            FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms,
            CASE
              WHEN allocated_at IS NULL THEN NULL
              ELSE FLOOR(EXTRACT(EPOCH FROM allocated_at) * 1000)
            END AS allocated_at_ms
        `,
        [
          record.purchaseId,
          record.txHash,
          record.buyerWallet,
          record.ambassadorSlug,
          record.ambassadorWallet,
          record.purchaseAmountSun,
          record.ownerShareSun,
          record.status,
          record.failureReason,
          record.source,
          record.allocationMode,
          record.allocationAttempts,
          record.lastAllocationAttemptAt,
          record.lastAllocationErrorCode,
          record.lastAllocationErrorMessage,
          record.deferredReason,
          record.withdrawSessionId,
          record.createdAt,
          record.updatedAt,
          record.allocatedAt
        ]
      );

      return rowToPurchaseRecord(result.rows[0]);
    } catch (error) {
      throw mapPgConflict(error);
    }
  }

  async update(
    purchaseId: string,
    input: UpdatePurchaseRecordInput
  ): Promise<PurchaseRecord> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");
    const current = await this.getByPurchaseId(normalizedPurchaseId);

    if (!current) {
      throw new Error(`Purchase not found: ${normalizedPurchaseId}`);
    }

    const updated = mergeRecord(current, input);

    const result = await query(
      `
        UPDATE purchases
        SET
          purchase_amount_sun = $2,
          owner_share_sun = $3,
          ambassador_slug = $4,
          ambassador_wallet = $5,
          status = $6,
          failure_reason = $7,
          allocation_mode = $8,
          allocation_attempts = $9,
          last_allocation_attempt_at = CASE WHEN $10::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($10 / 1000.0) END,
          last_allocation_error_code = $11,
          last_allocation_error_message = $12,
          deferred_reason = $13,
          withdraw_session_id = $14,
          updated_at = TO_TIMESTAMP($15 / 1000.0),
          allocated_at = CASE WHEN $16::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($16 / 1000.0) END
        WHERE purchase_id = $1
        RETURNING
          purchase_id,
          tx_hash,
          buyer_wallet,
          ambassador_slug,
          ambassador_wallet,
          purchase_amount_sun,
          owner_share_sun,
          status,
          failure_reason,
          source,
          allocation_mode,
          allocation_attempts,
          CASE
            WHEN last_allocation_attempt_at IS NULL THEN NULL
            ELSE FLOOR(EXTRACT(EPOCH FROM last_allocation_attempt_at) * 1000)
          END AS last_allocation_attempt_at_ms,
          last_allocation_error_code,
          last_allocation_error_message,
          deferred_reason,
          withdraw_session_id,
          FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
          FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms,
          CASE
            WHEN allocated_at IS NULL THEN NULL
            ELSE FLOOR(EXTRACT(EPOCH FROM allocated_at) * 1000)
          END AS allocated_at_ms
      `,
      [
        normalizedPurchaseId,
        updated.purchaseAmountSun,
        updated.ownerShareSun,
        updated.ambassadorSlug,
        updated.ambassadorWallet,
        updated.status,
        updated.failureReason,
        updated.allocationMode,
        updated.allocationAttempts,
        updated.lastAllocationAttemptAt,
        updated.lastAllocationErrorCode,
        updated.lastAllocationErrorMessage,
        updated.deferredReason,
        updated.withdrawSessionId,
        updated.updatedAt,
        updated.allocatedAt
      ]
    );

    return rowToPurchaseRecord(result.rows[0]);
  }

  async markVerified(
    purchaseId: string,
    input: {
      purchaseAmountSun: string;
      ownerShareSun: string;
      ambassadorSlug?: string | null;
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorSlug: input.ambassadorSlug,
      ambassadorWallet: input.ambassadorWallet,
      status: "verified",
      failureReason: null,
      allocationMode: input.allocationMode,
      deferredReason: null,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      withdrawSessionId: null,
      now: input.now
    });
  }

  async markDeferred(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "deferred",
      failureReason: null,
      deferredReason: assertNonEmpty(input.reason, "reason"),
      allocationMode: input.allocationMode ?? "deferred",
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ??
        assertNonEmpty(input.reason, "reason"),
      now
    });
  }

  async markAllocationInProgress(
    purchaseId: string,
    input?: {
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input?.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_in_progress",
      failureReason: null,
      allocationMode: input?.allocationMode,
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      deferredReason: null,
      now
    });
  }

  async markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input?.now ?? Date.now();

    return this.update(purchaseId, {
      ambassadorWallet: input?.ambassadorWallet,
      status: "allocated",
      failureReason: null,
      allocationMode: input?.allocationMode,
      deferredReason: null,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      allocatedAt: now,
      now
    });
  }

  async markAllocationRetryableFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const reason = assertNonEmpty(input.reason, "reason");
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_failed_retryable",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markAllocationFinalFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const reason = assertNonEmpty(input.reason, "reason");
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_failed_final",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markFailed(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.markAllocationRetryableFailed(purchaseId, {
      reason,
      allocationMode: "manual-replay",
      now
    });
  }

  async markIgnored(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "ignored",
      failureReason: assertNonEmpty(reason, "reason"),
      now
    });
  }

  async assignWithdrawSession(
    purchaseId: string,
    withdrawSessionId: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      withdrawSessionId: assertNonEmpty(withdrawSessionId, "withdrawSessionId"),
      now
    });
  }

  async clearWithdrawSession(
    purchaseId: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      withdrawSessionId: null,
      now
    });
  }

  async listReplayableFailures(limit?: number): Promise<PurchaseRecord[]> {
    const params: unknown[] = [];
    let sql = `
      ${buildSelectSql()}
      WHERE status IN ('deferred', 'allocation_failed_retryable')
      ORDER BY created_at ASC
    `;

    if (limit && limit > 0) {
      params.push(Math.floor(limit));
      sql += ` LIMIT $1`;
    }

    const result = await query(sql, params);
    return result.rows.map(rowToPurchaseRecord);
  }

  async listPendingByAmbassador(
    input: PendingPurchaseQuery
  ): Promise<PurchaseRecord[]> {
    const ambassadorWallet = assertNonEmpty(
      input.ambassadorWallet,
      "ambassadorWallet"
    );
    const statuses = normalizePendingStatuses(input.statuses);
    const limit = input.limit && input.limit > 0 ? Math.floor(input.limit) : null;

    const params: unknown[] = [ambassadorWallet, statuses];
    let sql = `
      ${buildSelectSql()}
      WHERE ambassador_wallet = $1
        AND status = ANY($2::text[])
      ORDER BY created_at ASC
    `;

    if (limit != null) {
      params.push(limit);
      sql += ` LIMIT $3`;
    }

    const result = await query(sql, params);
    return result.rows.map(rowToPurchaseRecord);
  }

  async getCabinetStatsByAmbassadorWallet(
    ambassadorWallet: string
  ): Promise<CabinetStatsRecord> {
    const normalizedAmbassadorWallet = assertNonEmpty(
      ambassadorWallet,
      "ambassadorWallet"
    );

    const result = await query(
      `
        SELECT
          COUNT(DISTINCT CASE
            WHEN status <> 'received' AND buyer_wallet <> '' THEN buyer_wallet
            ELSE NULL
          END) AS total_buyers,

          COALESCE(SUM(CASE
            WHEN status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocated',
              'allocation_failed_retryable',
              'allocation_failed_final'
            ) THEN purchase_amount_sun::numeric
            ELSE 0
          END)::text, '0') AS tracked_volume_sun,

          COALESCE(SUM(CASE
            WHEN status = 'allocated'
              AND withdraw_session_id IS NULL
              AND owner_share_sun::numeric > 0
            THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS claimable_rewards_sun,

          COALESCE(SUM(CASE
            WHEN status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocated',
              'allocation_failed_retryable',
              'allocation_failed_final'
            ) THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS lifetime_rewards_sun,

          COALESCE(SUM(CASE
            WHEN withdraw_session_id IS NOT NULL THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS withdrawn_rewards_sun,

          COALESCE(SUM(CASE
            WHEN status = 'allocated'
              AND withdraw_session_id IS NULL
              AND owner_share_sun::numeric > 0
            THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS available_on_chain_sun,

          COALESCE(SUM(CASE
            WHEN status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocation_failed_retryable'
            )
              AND withdraw_session_id IS NULL
              AND owner_share_sun::numeric > 0
            THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS pending_backend_sync_sun,

          COALESCE(SUM(CASE
            WHEN withdraw_session_id IS NOT NULL THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS requested_for_processing_sun,

          COUNT(*) FILTER (
            WHERE status = 'allocated'
              AND withdraw_session_id IS NULL
              AND owner_share_sun::numeric > 0
          ) AS available_on_chain_count,

          COUNT(*) FILTER (
            WHERE status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocation_failed_retryable'
            )
              AND withdraw_session_id IS NULL
              AND owner_share_sun::numeric > 0
          ) AS pending_backend_sync_count,

          COUNT(*) FILTER (
            WHERE withdraw_session_id IS NOT NULL
              AND owner_share_sun::numeric > 0
          ) AS requested_for_processing_count
        FROM purchases
        WHERE ambassador_wallet = $1
      `,
      [normalizedAmbassadorWallet]
    );

    const row = result.rows[0];
    return row ? rowToCabinetStatsRecord(row) : emptyCabinetStatsRecord();
  }

  async hasProcessedPurchase(purchaseId: string): Promise<boolean> {
    const record = await this.getByPurchaseId(purchaseId);

    if (!record) {
      return false;
    }

    return (
      record.status === "allocated" ||
      record.status === "ignored" ||
      record.status === "allocation_failed_final"
    );
  }
}

export class InMemoryPurchaseStore implements PurchaseStore {
  private readonly byPurchaseId = new Map<string, PurchaseRecord>();
  private readonly purchaseIdByTxHash = new Map<string, string>();
  private readonly ambassadorsBySlug = new Map<string, AmbassadorStoreRecord>();

  async getByPurchaseId(purchaseId: string): Promise<PurchaseRecord | null> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");
    return this.byPurchaseId.get(normalizedPurchaseId) ?? null;
  }

  async getByTxHash(txHash: string): Promise<PurchaseRecord | null> {
    const normalizedTxHash = normalizeTxHash(txHash);
    const purchaseId = this.purchaseIdByTxHash.get(normalizedTxHash);

    if (!purchaseId) {
      return null;
    }

    return this.byPurchaseId.get(purchaseId) ?? null;
  }

  async getAmbassadorBySlug(slug: string): Promise<AmbassadorStoreRecord | null> {
    const normalizedSlug = assertNonEmpty(slug, "slug").toLowerCase();
    return this.ambassadorsBySlug.get(normalizedSlug) ?? null;
  }

  async createOrGetReceivedPurchase(
    input: CreateOrGetReceivedPurchaseInput
  ): Promise<CreateOrGetReceivedPurchaseResult> {
    const txHash = normalizeTxHash(input.txHash);
    const existing = await this.getByTxHash(txHash);

    if (existing) {
      return {
        created: false,
        purchase: existing
      };
    }

    const created = await this.create({
      purchaseId: buildPurchaseIdFromTxHash(txHash),
      txHash,
      buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
      ambassadorSlug: normalizeOptionalString(input.ambassadorSlug),
      purchaseAmountSun: "0",
      ownerShareSun: "0",
      source: "frontend-attribution",
      status: "received",
      now: input.now
    });

    return {
      created: true,
      purchase: created
    };
  }

  async attachAmbassadorToPurchase(
    input: AttachAmbassadorToPurchaseInput
  ): Promise<PurchaseRecord> {
    return this.update(input.purchaseId, {
      ambassadorSlug: input.ambassadorSlug,
      ambassadorWallet: input.ambassadorWallet,
      purchaseAmountSun: input.purchaseAmountSun ?? "0",
      ownerShareSun: input.ownerShareSun ?? "0",
      now: input.now
    });
  }

  async markVerifiedPurchase(
    input: MarkVerifiedPurchaseInput
  ): Promise<PurchaseRecord> {
    const current = await this.getByPurchaseId(input.purchaseId);

    if (!current) {
      throw new Error(`Purchase not found: ${input.purchaseId}`);
    }

    return this.markVerified(input.purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorSlug: current.ambassadorSlug,
      ambassadorWallet: current.ambassadorWallet,
      allocationMode: current.allocationMode,
      now: input.now
    });
  }

  async create(input: CreatePurchaseRecordInput): Promise<PurchaseRecord> {
    const record = createRecord(input);

    if (this.byPurchaseId.has(record.purchaseId)) {
      throw new Error(`Purchase already exists for purchaseId: ${record.purchaseId}`);
    }

    if (this.purchaseIdByTxHash.has(record.txHash)) {
      throw new Error(`Purchase already exists for txHash: ${record.txHash}`);
    }

    this.byPurchaseId.set(record.purchaseId, record);
    this.purchaseIdByTxHash.set(record.txHash, record.purchaseId);

    return record;
  }

  async update(
    purchaseId: string,
    input: UpdatePurchaseRecordInput
  ): Promise<PurchaseRecord> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");
    const current = this.byPurchaseId.get(normalizedPurchaseId);

    if (!current) {
      throw new Error(`Purchase not found: ${normalizedPurchaseId}`);
    }

    const updated = mergeRecord(current, input);
    this.byPurchaseId.set(normalizedPurchaseId, updated);

    return updated;
  }

  async markVerified(
    purchaseId: string,
    input: {
      purchaseAmountSun: string;
      ownerShareSun: string;
      ambassadorSlug?: string | null;
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorSlug: input.ambassadorSlug,
      ambassadorWallet: input.ambassadorWallet,
      status: "verified",
      failureReason: null,
      allocationMode: input.allocationMode,
      deferredReason: null,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      withdrawSessionId: null,
      now: input.now
    });
  }

  async markDeferred(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "deferred",
      failureReason: null,
      deferredReason: assertNonEmpty(input.reason, "reason"),
      allocationMode: input.allocationMode ?? "deferred",
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ??
        assertNonEmpty(input.reason, "reason"),
      now
    });
  }

  async markAllocationInProgress(
    purchaseId: string,
    input?: {
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input?.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_in_progress",
      failureReason: null,
      allocationMode: input?.allocationMode,
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      deferredReason: null,
      now
    });
  }

  async markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input?.now ?? Date.now();

    return this.update(purchaseId, {
      ambassadorWallet: input?.ambassadorWallet,
      status: "allocated",
      failureReason: null,
      allocationMode: input?.allocationMode,
      deferredReason: null,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      allocatedAt: now,
      now
    });
  }

  async markAllocationRetryableFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const reason = assertNonEmpty(input.reason, "reason");
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_failed_retryable",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markAllocationFinalFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const reason = assertNonEmpty(input.reason, "reason");
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_failed_final",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markFailed(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.markAllocationRetryableFailed(purchaseId, {
      reason,
      allocationMode: "manual-replay",
      now
    });
  }

  async markIgnored(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "ignored",
      failureReason: assertNonEmpty(reason, "reason"),
      now
    });
  }

  async assignWithdrawSession(
    purchaseId: string,
    withdrawSessionId: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      withdrawSessionId: assertNonEmpty(withdrawSessionId, "withdrawSessionId"),
      now
    });
  }

  async clearWithdrawSession(
    purchaseId: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      withdrawSessionId: null,
      now
    });
  }

  async listReplayableFailures(limit?: number): Promise<PurchaseRecord[]> {
    let rows = Array.from(this.byPurchaseId.values())
      .filter(
        (record) =>
          record.status === "deferred" ||
          record.status === "allocation_failed_retryable"
      )
      .sort((left, right) => left.createdAt - right.createdAt);

    if (limit && limit > 0) {
      rows = rows.slice(0, Math.floor(limit));
    }

    return rows;
  }

  async listPendingByAmbassador(
    input: PendingPurchaseQuery
  ): Promise<PurchaseRecord[]> {
    const ambassadorWallet = assertNonEmpty(
      input.ambassadorWallet,
      "ambassadorWallet"
    );
    const statuses = normalizePendingStatuses(input.statuses);
    const allowed = new Set(statuses);

    let rows = Array.from(this.byPurchaseId.values())
      .filter(
        (record) =>
          record.ambassadorWallet === ambassadorWallet &&
          allowed.has(record.status)
      )
      .sort((left, right) => left.createdAt - right.createdAt);

    if (input.limit && input.limit > 0) {
      rows = rows.slice(0, Math.floor(input.limit));
    }

    return rows;
  }

  async getCabinetStatsByAmbassadorWallet(
    ambassadorWallet: string
  ): Promise<CabinetStatsRecord> {
    const normalizedAmbassadorWallet = assertNonEmpty(
      ambassadorWallet,
      "ambassadorWallet"
    );

    const rows = Array.from(this.byPurchaseId.values()).filter(
      (record) => record.ambassadorWallet === normalizedAmbassadorWallet
    );

    const buyers = new Set<string>();

    let trackedVolumeSun = "0";
    let claimableRewardsSun = "0";
    let lifetimeRewardsSun = "0";
    let withdrawnRewardsSun = "0";
    let availableOnChainSun = "0";
    let pendingBackendSyncSun = "0";
    let requestedForProcessingSun = "0";

    let availableOnChainCount = 0;
    let pendingBackendSyncCount = 0;
    let requestedForProcessingCount = 0;

    for (const row of rows) {
      if (row.status !== "received" && row.buyerWallet) {
        buyers.add(row.buyerWallet);
      }

      const rewardAmount = BigInt(row.ownerShareSun || "0");

      const contributesVolume =
        row.status === "verified" ||
        row.status === "deferred" ||
        row.status === "allocation_in_progress" ||
        row.status === "allocated" ||
        row.status === "allocation_failed_retryable" ||
        row.status === "allocation_failed_final";

      if (contributesVolume) {
        trackedVolumeSun = sumSunStrings(trackedVolumeSun, row.purchaseAmountSun);
        lifetimeRewardsSun = sumSunStrings(lifetimeRewardsSun, row.ownerShareSun);
      }

      const isAvailableOnChain =
        row.status === "allocated" &&
        !row.withdrawSessionId &&
        rewardAmount > 0n;

      if (isAvailableOnChain) {
        claimableRewardsSun = sumSunStrings(claimableRewardsSun, row.ownerShareSun);
        availableOnChainSun = sumSunStrings(availableOnChainSun, row.ownerShareSun);
        availableOnChainCount += 1;
      }

      const isPendingBackendSync =
        (row.status === "verified" ||
          row.status === "deferred" ||
          row.status === "allocation_in_progress" ||
          row.status === "allocation_failed_retryable") &&
        !row.withdrawSessionId &&
        rewardAmount > 0n;

      if (isPendingBackendSync) {
        pendingBackendSyncSun = sumSunStrings(pendingBackendSyncSun, row.ownerShareSun);
        pendingBackendSyncCount += 1;
      }

      const isRequestedForProcessing =
        !!row.withdrawSessionId && rewardAmount > 0n;

      if (isRequestedForProcessing) {
        withdrawnRewardsSun = sumSunStrings(withdrawnRewardsSun, row.ownerShareSun);
        requestedForProcessingSun = sumSunStrings(
          requestedForProcessingSun,
          row.ownerShareSun
        );
        requestedForProcessingCount += 1;
      }
    }

    return {
      totalBuyers: buyers.size,
      trackedVolumeSun,
      claimableRewardsSun,
      lifetimeRewardsSun,
      withdrawnRewardsSun,
      availableOnChainSun,
      pendingBackendSyncSun,
      requestedForProcessingSun,
      availableOnChainCount,
      pendingBackendSyncCount,
      requestedForProcessingCount,
      hasProcessingWithdrawal: requestedForProcessingCount > 0
    };
  }

  async hasProcessedPurchase(purchaseId: string): Promise<boolean> {
    const record = await this.getByPurchaseId(purchaseId);

    if (!record) {
      return false;
    }

    return (
      record.status === "allocated" ||
      record.status === "ignored" ||
      record.status === "allocation_failed_final"
    );
  }
}

export function createPurchaseStore(): PurchaseStore {
  return new PostgresPurchaseStore();
}
