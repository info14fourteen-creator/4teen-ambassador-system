import { query } from "./postgres";

export type PurchaseProcessingStatus =
  | "received"
  | "verified"
  | "deferred"
  | "allocation_in_progress"
  | "allocated"
  | "allocation_failed_retryable"
  | "allocation_failed_final"
  | "withdraw_included"
  | "withdraw_completed"
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
  ambassadorRewardSun: string;
  ownerPayoutSun: string;
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

  /**
   * Real claimable / available balances must come from contract reads.
   * DB only stores operational and historical backend state.
   */
  claimableRewardsSun: string;
  availableOnChainSun: string;
  availableOnChainCount: number;

  allocatedInDbSun: string;
  allocatedInDbCount: number;

  pendingBackendSyncSun: string;
  pendingBackendSyncCount: number;

  requestedForProcessingSun: string;
  requestedForProcessingCount: number;

  lifetimeRewardsSun: string;
  withdrawnRewardsSun: string;

  /**
   * Debug / legacy visibility:
   * rows where we have volume and owner share, but reward was never written.
   */
  missingRewardCount: number;
  missingRewardOwnerShareSun: string;
  hasBrokenPendingRewards: boolean;

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
  ambassadorRewardSun?: string;
  ownerPayoutSun?: string;
  now?: number;
}

export interface MarkVerifiedPurchaseInput {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  ambassadorRewardSun: string;
  ownerPayoutSun: string;
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
  ambassadorRewardSun?: string;
  ownerPayoutSun?: string;
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
  ambassadorRewardSun?: string;
  ownerPayoutSun?: string;
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
      ambassadorRewardSun: string;
      ownerPayoutSun: string;
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

  markWithdrawIncluded(
    purchaseId: string,
    input: {
      withdrawSessionId: string;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markWithdrawCompleted(
    purchaseId: string,
    input?: {
      withdrawSessionId?: string | null;
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

const TRACKED_VOLUME_STATUSES = new Set<PurchaseProcessingStatus>([
  "verified",
  "deferred",
  "allocation_in_progress",
  "allocated",
  "allocation_failed_retryable",
  "allocation_failed_final",
  "withdraw_included",
  "withdraw_completed"
]);

const PENDING_BACKEND_SYNC_STATUSES = new Set<PurchaseProcessingStatus>([
  "verified",
  "deferred",
  "allocation_in_progress",
  "allocation_failed_retryable"
]);

const RATE_LIMIT_ERROR_CODES = new Set([
  "429",
  "ERR_BAD_REQUEST",
  "TRON_RATE_LIMIT",
  "GASSTATION_RATE_LIMIT"
]);

const RATE_LIMIT_MESSAGE_PARTS = [
  "status code 429",
  "http 429",
  "too many requests",
  "rate limit",
  "rate limited"
];

const RESOURCE_ERROR_CODES = new Set([
  "ACCOUNT_RESOURCE_INSUFFICIENT",
  "ACCOUNT_RESOURCE_INSUFFICIENT_AFTER_RENTAL",
  "ACCOUNT_RESOURCE_CONSUMED_BEFORE_SEND",
  "ACCOUNT_RESOURCE_INSUFFICIENT_DURING_SEND",
  "GASSTATION_OPERATOR_BALANCE_LOW",
  "GASSTATION_TOPUP_NOT_SETTLED",
  "GASSTATION_TOPUP_TRANSFER_FAILED",
  "GASSTATION_TOPUP_FAILED",
  "GASSTATION_SERVICE_BALANCE_LOW_AFTER_TOPUP",
  "GASSTATION_ORDER_FAILED"
]);

const RESOURCE_MESSAGE_PARTS = [
  "out of energy",
  "resource insufficient",
  "bandwidth",
  "energy",
  "gasstation",
  "top-up",
  "top up"
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

function normalizeSunAmount(
  value: string | number | bigint | undefined,
  fieldName = "sunAmount"
): string {
  if (value == null) {
    return "0";
  }

  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${fieldName} must be a non-negative integer string`);
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
  const resolved = statuses?.length ? statuses : DEFAULT_PENDING_STATUSES;
  return Array.from(new Set(resolved));
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

function toBigIntSafe(value: string | null | undefined): bigint {
  const normalized = String(value || "0").trim();

  try {
    return BigInt(/^\d+$/.test(normalized) ? normalized : "0");
  } catch {
    return 0n;
  }
}

function hasPositiveReward(record: PurchaseRecord): boolean {
  return toBigIntSafe(record.ambassadorRewardSun) > 0n;
}

function hasMissingRewardButHasOwnerShare(record: PurchaseRecord): boolean {
  return (
    toBigIntSafe(record.ownerShareSun) > 0n &&
    toBigIntSafe(record.ambassadorRewardSun) === 0n
  );
}

function assertSplitConsistency(input: {
  ownerShareSun: string;
  ambassadorRewardSun: string;
  ownerPayoutSun: string;
  context: string;
}): void {
  const ownerShareSun = normalizeSunAmount(input.ownerShareSun, "ownerShareSun");
  const ambassadorRewardSun = normalizeSunAmount(
    input.ambassadorRewardSun,
    "ambassadorRewardSun"
  );
  const ownerPayoutSun = normalizeSunAmount(input.ownerPayoutSun, "ownerPayoutSun");

  const ownerShare = BigInt(ownerShareSun);
  const reward = BigInt(ambassadorRewardSun);
  const ownerPayout = BigInt(ownerPayoutSun);

  if (reward > ownerShare) {
    throw new Error(
      `${input.context}: ambassadorRewardSun cannot exceed ownerShareSun`
    );
  }

  if (ownerPayout > ownerShare) {
    throw new Error(
      `${input.context}: ownerPayoutSun cannot exceed ownerShareSun`
    );
  }

  if (reward + ownerPayout !== ownerShare) {
    throw new Error(
      `${input.context}: invalid reward split, ambassadorRewardSun + ownerPayoutSun must equal ownerShareSun`
    );
  }
}

function emptyCabinetStatsRecord(): CabinetStatsRecord {
  return {
    totalBuyers: 0,
    trackedVolumeSun: "0",
    claimableRewardsSun: "0",
    availableOnChainSun: "0",
    availableOnChainCount: 0,
    allocatedInDbSun: "0",
    allocatedInDbCount: 0,
    pendingBackendSyncSun: "0",
    pendingBackendSyncCount: 0,
    requestedForProcessingSun: "0",
    requestedForProcessingCount: 0,
    lifetimeRewardsSun: "0",
    withdrawnRewardsSun: "0",
    missingRewardCount: 0,
    missingRewardOwnerShareSun: "0",
    hasBrokenPendingRewards: false,
    hasProcessingWithdrawal: false
  };
}

export function isRateLimitedAllocationFailure(record: PurchaseRecord): boolean {
  const code = String(record.lastAllocationErrorCode || "").trim().toUpperCase();
  const message = String(
    record.lastAllocationErrorMessage || record.failureReason || ""
  ).toLowerCase();

  if (RATE_LIMIT_ERROR_CODES.has(code)) {
    return (
      RATE_LIMIT_MESSAGE_PARTS.some((part) => message.includes(part)) ||
      code !== "ERR_BAD_REQUEST"
    );
  }

  return RATE_LIMIT_MESSAGE_PARTS.some((part) => message.includes(part));
}

export function isResourceLimitedAllocationFailure(record: PurchaseRecord): boolean {
  const code = String(record.lastAllocationErrorCode || "").trim().toUpperCase();
  const message = String(
    record.lastAllocationErrorMessage ||
      record.deferredReason ||
      record.failureReason ||
      ""
  ).toLowerCase();

  if (RESOURCE_ERROR_CODES.has(code)) {
    return true;
  }

  return RESOURCE_MESSAGE_PARTS.some((part) => message.includes(part));
}

export function computeAllocationRetryDelayMs(record: PurchaseRecord): number {
  if (
    record.status !== "allocation_failed_retryable" &&
    record.status !== "deferred"
  ) {
    return 0;
  }

  const attempts = Math.max(1, Number(record.allocationAttempts || 0));

  if (isRateLimitedAllocationFailure(record)) {
    if (attempts <= 1) return 30_000;
    if (attempts === 2) return 60_000;
    if (attempts === 3) return 180_000;
    if (attempts === 4) return 600_000;
    return 1_800_000;
  }

  if (isResourceLimitedAllocationFailure(record)) {
    if (attempts <= 1) return 60_000;
    if (attempts === 2) return 180_000;
    if (attempts === 3) return 300_000;
    if (attempts === 4) return 600_000;
    return 1_800_000;
  }

  if (record.status === "deferred") {
    if (attempts <= 1) return 15_000;
    if (attempts === 2) return 30_000;
    if (attempts === 3) return 60_000;
    if (attempts === 4) return 180_000;
    return 300_000;
  }

  if (attempts <= 1) return 10_000;
  if (attempts === 2) return 30_000;
  if (attempts === 3) return 60_000;
  if (attempts === 4) return 180_000;
  return 600_000;
}

export function getAllocationRetryReadyAt(record: PurchaseRecord): number {
  const base =
    Number(record.lastAllocationAttemptAt || 0) ||
    Number(record.updatedAt || 0) ||
    Number(record.createdAt || 0);

  return base + computeAllocationRetryDelayMs(record);
}

export function isPurchaseReadyForAllocationRetry(
  record: PurchaseRecord,
  now: number = Date.now()
): boolean {
  if (record.status === "verified") {
    return true;
  }

  if (
    record.status !== "deferred" &&
    record.status !== "allocation_failed_retryable"
  ) {
    return false;
  }

  return now >= getAllocationRetryReadyAt(record);
}

function createRecord(input: CreatePurchaseRecordInput): PurchaseRecord {
  const now = input.now ?? Date.now();
  const status = normalizeStatus(input.status);
  const purchaseAmountSun = normalizeSunAmount(input.purchaseAmountSun, "purchaseAmountSun");
  const ownerShareSun = normalizeSunAmount(input.ownerShareSun, "ownerShareSun");
  const ambassadorRewardSun = normalizeSunAmount(
    input.ambassadorRewardSun,
    "ambassadorRewardSun"
  );
  const ownerPayoutSun = normalizeSunAmount(input.ownerPayoutSun, "ownerPayoutSun");

  assertSplitConsistency({
    ownerShareSun,
    ambassadorRewardSun,
    ownerPayoutSun,
    context: "createRecord"
  });

  const allocatedAt =
    input.allocatedAt !== undefined
      ? normalizeTimestamp(input.allocatedAt, "allocatedAt")
      : status === "allocated" ||
          status === "withdraw_included" ||
          status === "withdraw_completed"
        ? now
        : null;

  return {
    purchaseId: assertNonEmpty(input.purchaseId, "purchaseId"),
    txHash: normalizeTxHash(input.txHash),
    buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
    ambassadorSlug: normalizeOptionalString(input.ambassadorSlug),
    ambassadorWallet: normalizeWallet(input.ambassadorWallet),
    purchaseAmountSun,
    ownerShareSun,
    ambassadorRewardSun,
    ownerPayoutSun,
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

  const nextPurchaseAmountSun =
    input.purchaseAmountSun !== undefined
      ? normalizeSunAmount(input.purchaseAmountSun, "purchaseAmountSun")
      : current.purchaseAmountSun;

  const nextOwnerShareSun =
    input.ownerShareSun !== undefined
      ? normalizeSunAmount(input.ownerShareSun, "ownerShareSun")
      : current.ownerShareSun;

  const nextAmbassadorRewardSun =
    input.ambassadorRewardSun !== undefined
      ? normalizeSunAmount(input.ambassadorRewardSun, "ambassadorRewardSun")
      : current.ambassadorRewardSun;

  const nextOwnerPayoutSun =
    input.ownerPayoutSun !== undefined
      ? normalizeSunAmount(input.ownerPayoutSun, "ownerPayoutSun")
      : current.ownerPayoutSun;

  assertSplitConsistency({
    ownerShareSun: nextOwnerShareSun,
    ambassadorRewardSun: nextAmbassadorRewardSun,
    ownerPayoutSun: nextOwnerPayoutSun,
    context: "mergeRecord"
  });

  const nextAllocationAttempts =
    input.allocationAttempts !== undefined
      ? normalizeCount(input.allocationAttempts, "allocationAttempts")
      : input.incrementAllocationAttempts
        ? current.allocationAttempts + 1
        : current.allocationAttempts;

  const nextAllocatedAt =
    input.allocatedAt !== undefined
      ? normalizeTimestamp(input.allocatedAt, "allocatedAt")
      : nextStatus === "allocated" ||
          nextStatus === "withdraw_included" ||
          nextStatus === "withdraw_completed"
        ? current.allocatedAt ?? now
        : current.allocatedAt;

  return {
    ...current,
    purchaseAmountSun: nextPurchaseAmountSun,
    ownerShareSun: nextOwnerShareSun,
    ambassadorRewardSun: nextAmbassadorRewardSun,
    ownerPayoutSun: nextOwnerPayoutSun,
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
    ambassadorRewardSun: String(row.ambassador_reward_sun || "0"),
    ownerPayoutSun: String(row.owner_payout_sun || "0"),
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
    lastAllocationErrorMessage: normalizeOptionalString(row.last_allocation_error_message),
    deferredReason: normalizeOptionalString(row.deferred_reason),
    withdrawSessionId: normalizeOptionalString(row.withdraw_session_id),
    createdAt: Number(row.created_at_ms),
    updatedAt: Number(row.updated_at_ms),
    allocatedAt: row.allocated_at_ms == null ? null : Number(row.allocated_at_ms)
  };
}

function rowToCabinetStatsRecord(row: any): CabinetStatsRecord {
  const requestedForProcessingCount = Number(row.requested_for_processing_count || 0);
  const missingRewardCount = Number(row.missing_reward_count || 0);

  return {
    totalBuyers: Number(row.total_buyers || 0),
    trackedVolumeSun: String(row.tracked_volume_sun || "0"),

    claimableRewardsSun: "0",
    availableOnChainSun: "0",
    availableOnChainCount: 0,

    allocatedInDbSun: String(row.allocated_in_db_sun || "0"),
    allocatedInDbCount: Number(row.allocated_in_db_count || 0),

    pendingBackendSyncSun: String(row.pending_backend_sync_sun || "0"),
    pendingBackendSyncCount: Number(row.pending_backend_sync_count || 0),

    requestedForProcessingSun: String(row.requested_for_processing_sun || "0"),
    requestedForProcessingCount,

    lifetimeRewardsSun: String(row.lifetime_rewards_sun || "0"),
    withdrawnRewardsSun: String(row.withdrawn_rewards_sun || "0"),

    missingRewardCount,
    missingRewardOwnerShareSun: String(row.missing_reward_owner_share_sun || "0"),
    hasBrokenPendingRewards: missingRewardCount > 0,

    hasProcessingWithdrawal: requestedForProcessingCount > 0
  };
}

function mapPgConflict(error: unknown): Error {
  const isUniqueViolation =
    !!error &&
    typeof error === "object" &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505";

  if (!isUniqueViolation) {
    return error instanceof Error ? error : new Error("Purchase store error");
  }

  const constraint =
    error &&
    typeof error === "object" &&
    "constraint" in error &&
    typeof (error as { constraint?: unknown }).constraint === "string"
      ? String((error as { constraint: string }).constraint)
      : "";

  if (constraint.includes("purchase_id")) {
    return new Error("Purchase already exists for purchaseId");
  }

  if (constraint.includes("tx_hash")) {
    return new Error("Purchase already exists for txHash");
  }

  return new Error("Purchase already exists");
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
      ambassador_reward_sun,
      owner_payout_sun,
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

function buildCabinetStatsSql(): string {
  return `
    WITH scoped AS (
      SELECT *
      FROM purchases
      WHERE ambassador_wallet = $1
    )
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
          'allocation_failed_final',
          'withdraw_included',
          'withdraw_completed'
        ) THEN purchase_amount_sun::numeric
        ELSE 0
      END)::text, '0') AS tracked_volume_sun,

      COALESCE(SUM(CASE
        WHEN status IN (
          'verified',
          'deferred',
          'allocation_in_progress',
          'allocated',
          'allocation_failed_retryable',
          'allocation_failed_final',
          'withdraw_included',
          'withdraw_completed'
        ) THEN ambassador_reward_sun::numeric
        ELSE 0
      END)::text, '0') AS lifetime_rewards_sun,

      COALESCE(SUM(CASE
        WHEN status = 'withdraw_completed'
        THEN ambassador_reward_sun::numeric
        ELSE 0
      END)::text, '0') AS withdrawn_rewards_sun,

      COALESCE(SUM(CASE
        WHEN status = 'allocated'
          AND withdraw_session_id IS NULL
          AND ambassador_reward_sun::numeric > 0
        THEN ambassador_reward_sun::numeric
        ELSE 0
      END)::text, '0') AS allocated_in_db_sun,

      COUNT(*) FILTER (
        WHERE status = 'allocated'
          AND withdraw_session_id IS NULL
          AND ambassador_reward_sun::numeric > 0
      ) AS allocated_in_db_count,

      COALESCE(SUM(CASE
        WHEN status IN (
          'verified',
          'deferred',
          'allocation_in_progress',
          'allocation_failed_retryable'
        )
          AND withdraw_session_id IS NULL
          AND ambassador_reward_sun::numeric > 0
        THEN ambassador_reward_sun::numeric
        ELSE 0
      END)::text, '0') AS pending_backend_sync_sun,

      COUNT(*) FILTER (
        WHERE status IN (
          'verified',
          'deferred',
          'allocation_in_progress',
          'allocation_failed_retryable'
        )
          AND withdraw_session_id IS NULL
          AND (
            ambassador_reward_sun::numeric > 0
            OR (
              owner_share_sun::numeric > 0
              AND ambassador_reward_sun::numeric = 0
            )
          )
      ) AS pending_backend_sync_count,

      COALESCE(SUM(CASE
        WHEN status = 'withdraw_included'
          AND withdraw_session_id IS NOT NULL
          AND ambassador_reward_sun::numeric > 0
        THEN ambassador_reward_sun::numeric
        ELSE 0
      END)::text, '0') AS requested_for_processing_sun,

      COUNT(*) FILTER (
        WHERE status = 'withdraw_included'
          AND withdraw_session_id IS NOT NULL
          AND ambassador_reward_sun::numeric > 0
      ) AS requested_for_processing_count,

      COUNT(*) FILTER (
        WHERE status IN (
          'verified',
          'deferred',
          'allocation_in_progress',
          'allocated',
          'allocation_failed_retryable',
          'allocation_failed_final',
          'withdraw_included',
          'withdraw_completed'
        )
          AND owner_share_sun::numeric > 0
          AND ambassador_reward_sun::numeric = 0
      ) AS missing_reward_count,

      COALESCE(SUM(CASE
        WHEN status IN (
          'verified',
          'deferred',
          'allocation_in_progress',
          'allocated',
          'allocation_failed_retryable',
          'allocation_failed_final',
          'withdraw_included',
          'withdraw_completed'
        )
          AND owner_share_sun::numeric > 0
          AND ambassador_reward_sun::numeric = 0
        THEN owner_share_sun::numeric
        ELSE 0
      END)::text, '0') AS missing_reward_owner_share_sun
    FROM scoped
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
      ambassador_reward_sun TEXT NOT NULL DEFAULT '0',
      owner_payout_sun TEXT NOT NULL DEFAULT '0',
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
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS ambassador_reward_sun TEXT NOT NULL DEFAULT '0'
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS owner_payout_sun TEXT NOT NULL DEFAULT '0'
  `);

  await query(`
    UPDATE purchases
    SET
      ambassador_reward_sun = CASE
        WHEN ambassador_reward_sun IS NULL OR TRIM(ambassador_reward_sun) = '' THEN '0'
        ELSE ambassador_reward_sun
      END
  `);

  await query(`
    UPDATE purchases
    SET
      owner_payout_sun = CASE
        WHEN owner_payout_sun IS NULL
          OR TRIM(owner_payout_sun) = ''
        THEN GREATEST(
          owner_share_sun::numeric - ambassador_reward_sun::numeric,
          0
        )::text
        WHEN owner_payout_sun = '0'
          AND owner_share_sun::numeric >= ambassador_reward_sun::numeric
        THEN GREATEST(
          owner_share_sun::numeric - ambassador_reward_sun::numeric,
          0
        )::text
        ELSE owner_payout_sun
      END
  `);

  await query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'purchases_reward_split_check'
      ) THEN
        ALTER TABLE purchases
        ADD CONSTRAINT purchases_reward_split_check
        CHECK (
          owner_share_sun::numeric >= 0
          AND ambassador_reward_sun::numeric >= 0
          AND owner_payout_sun::numeric >= 0
          AND ambassador_reward_sun::numeric <= owner_share_sun::numeric
          AND owner_payout_sun::numeric <= owner_share_sun::numeric
          AND owner_share_sun::numeric =
              ambassador_reward_sun::numeric + owner_payout_sun::numeric
        ) NOT VALID;
      END IF;
    END
    $$;
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

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_ambassador_wallet_status_withdraw
    ON purchases(ambassador_wallet, status, withdraw_session_id, created_at)
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
        ambassadorRewardSun: "0",
        ownerPayoutSun: "0",
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
      ambassadorRewardSun: input.ambassadorRewardSun ?? "0",
      ownerPayoutSun: input.ownerPayoutSun ?? "0",
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
      ambassadorRewardSun: input.ambassadorRewardSun,
      ownerPayoutSun: input.ownerPayoutSun,
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
            ambassador_reward_sun,
            owner_payout_sun,
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
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            $12, $13, $14,
            CASE WHEN $15::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($15 / 1000.0) END,
            $16, $17, $18, $19,
            TO_TIMESTAMP($20 / 1000.0),
            TO_TIMESTAMP($21 / 1000.0),
            CASE WHEN $22::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($22 / 1000.0) END
          )
          RETURNING
            purchase_id,
            tx_hash,
            buyer_wallet,
            ambassador_slug,
            ambassador_wallet,
            purchase_amount_sun,
            owner_share_sun,
            ambassador_reward_sun,
            owner_payout_sun,
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
          record.ambassadorRewardSun,
          record.ownerPayoutSun,
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
          ambassador_reward_sun = $4,
          owner_payout_sun = $5,
          ambassador_slug = $6,
          ambassador_wallet = $7,
          status = $8,
          failure_reason = $9,
          allocation_mode = $10,
          allocation_attempts = $11,
          last_allocation_attempt_at = CASE WHEN $12::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($12 / 1000.0) END,
          last_allocation_error_code = $13,
          last_allocation_error_message = $14,
          deferred_reason = $15,
          withdraw_session_id = $16,
          updated_at = TO_TIMESTAMP($17 / 1000.0),
          allocated_at = CASE WHEN $18::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($18 / 1000.0) END
        WHERE purchase_id = $1
        RETURNING
          purchase_id,
          tx_hash,
          buyer_wallet,
          ambassador_slug,
          ambassador_wallet,
          purchase_amount_sun,
          owner_share_sun,
          ambassador_reward_sun,
          owner_payout_sun,
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
        updated.ambassadorRewardSun,
        updated.ownerPayoutSun,
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
      ambassadorRewardSun: string;
      ownerPayoutSun: string;
      ambassadorSlug?: string | null;
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    assertSplitConsistency({
      ownerShareSun: input.ownerShareSun,
      ambassadorRewardSun: input.ambassadorRewardSun,
      ownerPayoutSun: input.ownerPayoutSun,
      context: "markVerified"
    });

    return this.update(purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorRewardSun: input.ambassadorRewardSun,
      ownerPayoutSun: input.ownerPayoutSun,
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
    const reason = assertNonEmpty(input.reason, "reason");

    return this.update(purchaseId, {
      status: "deferred",
      failureReason: null,
      deferredReason: reason,
      allocationMode: input.allocationMode ?? "deferred",
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage: normalizeOptionalString(input.errorMessage) ?? reason,
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
    const now = input.now ?? Date.now();
    const reason = assertNonEmpty(input.reason, "reason");

    return this.update(purchaseId, {
      status: "allocation_failed_retryable",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage: normalizeOptionalString(input.errorMessage) ?? reason,
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
    const now = input.now ?? Date.now();
    const reason = assertNonEmpty(input.reason, "reason");

    return this.update(purchaseId, {
      status: "allocation_failed_final",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage: normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markWithdrawIncluded(
    purchaseId: string,
    input: {
      withdrawSessionId: string;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "withdraw_included",
      withdrawSessionId: assertNonEmpty(input.withdrawSessionId, "withdrawSessionId"),
      now: input.now
    });
  }

  async markWithdrawCompleted(
    purchaseId: string,
    input?: {
      withdrawSessionId?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "withdraw_completed",
      withdrawSessionId:
        input?.withdrawSessionId !== undefined
          ? normalizeOptionalString(input.withdrawSessionId)
          : null,
      now: input?.now
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
    const ambassadorWallet = assertNonEmpty(input.ambassadorWallet, "ambassadorWallet");
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

    const result = await query(buildCabinetStatsSql(), [normalizedAmbassadorWallet]);
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
      record.status === "withdraw_completed" ||
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
      ambassadorRewardSun: "0",
      ownerPayoutSun: "0",
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
      ambassadorRewardSun: input.ambassadorRewardSun ?? "0",
      ownerPayoutSun: input.ownerPayoutSun ?? "0",
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
      ambassadorRewardSun: input.ambassadorRewardSun,
      ownerPayoutSun: input.ownerPayoutSun,
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
      ambassadorRewardSun: string;
      ownerPayoutSun: string;
      ambassadorSlug?: string | null;
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    assertSplitConsistency({
      ownerShareSun: input.ownerShareSun,
      ambassadorRewardSun: input.ambassadorRewardSun,
      ownerPayoutSun: input.ownerPayoutSun,
      context: "markVerified"
    });

    return this.update(purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorRewardSun: input.ambassadorRewardSun,
      ownerPayoutSun: input.ownerPayoutSun,
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
    const reason = assertNonEmpty(input.reason, "reason");

    return this.update(purchaseId, {
      status: "deferred",
      failureReason: null,
      deferredReason: reason,
      allocationMode: input.allocationMode ?? "deferred",
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage: normalizeOptionalString(input.errorMessage) ?? reason,
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
    const now = input.now ?? Date.now();
    const reason = assertNonEmpty(input.reason, "reason");

    return this.update(purchaseId, {
      status: "allocation_failed_retryable",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage: normalizeOptionalString(input.errorMessage) ?? reason,
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
    const now = input.now ?? Date.now();
    const reason = assertNonEmpty(input.reason, "reason");

    return this.update(purchaseId, {
      status: "allocation_failed_final",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage: normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markWithdrawIncluded(
    purchaseId: string,
    input: {
      withdrawSessionId: string;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "withdraw_included",
      withdrawSessionId: assertNonEmpty(input.withdrawSessionId, "withdrawSessionId"),
      now: input.now
    });
  }

  async markWithdrawCompleted(
    purchaseId: string,
    input?: {
      withdrawSessionId?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "withdraw_completed",
      withdrawSessionId:
        input?.withdrawSessionId !== undefined
          ? normalizeOptionalString(input.withdrawSessionId)
          : null,
      now: input?.now
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
    const ambassadorWallet = assertNonEmpty(input.ambassadorWallet, "ambassadorWallet");
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
    let lifetimeRewardsSun = "0";
    let withdrawnRewardsSun = "0";
    let allocatedInDbSun = "0";
    let pendingBackendSyncSun = "0";
    let requestedForProcessingSun = "0";
    let missingRewardOwnerShareSun = "0";

    let allocatedInDbCount = 0;
    let pendingBackendSyncCount = 0;
    let requestedForProcessingCount = 0;
    let missingRewardCount = 0;

    for (const record of rows) {
      if (record.status !== "received" && record.buyerWallet) {
        buyers.add(record.buyerWallet);
      }

      if (TRACKED_VOLUME_STATUSES.has(record.status)) {
        trackedVolumeSun = sumSunStrings(trackedVolumeSun, record.purchaseAmountSun);
        lifetimeRewardsSun = sumSunStrings(lifetimeRewardsSun, record.ambassadorRewardSun);
      }

      const hasReward = hasPositiveReward(record);
      const hasMissingReward = hasMissingRewardButHasOwnerShare(record);

      if (TRACKED_VOLUME_STATUSES.has(record.status) && hasMissingReward) {
        missingRewardCount += 1;
        missingRewardOwnerShareSun = sumSunStrings(
          missingRewardOwnerShareSun,
          record.ownerShareSun
        );
      }

      if (record.status === "withdraw_completed" && hasReward) {
        withdrawnRewardsSun = sumSunStrings(withdrawnRewardsSun, record.ambassadorRewardSun);
      }

      if (
        record.status === "allocated" &&
        !record.withdrawSessionId &&
        hasReward
      ) {
        allocatedInDbSun = sumSunStrings(allocatedInDbSun, record.ambassadorRewardSun);
        allocatedInDbCount += 1;
      }

      if (
        PENDING_BACKEND_SYNC_STATUSES.has(record.status) &&
        !record.withdrawSessionId
      ) {
        if (hasReward) {
          pendingBackendSyncSun = sumSunStrings(
            pendingBackendSyncSun,
            record.ambassadorRewardSun
          );
        }

        if (hasReward || hasMissingReward) {
          pendingBackendSyncCount += 1;
        }
      }

      if (
        record.status === "withdraw_included" &&
        !!record.withdrawSessionId &&
        hasReward
      ) {
        requestedForProcessingSun = sumSunStrings(
          requestedForProcessingSun,
          record.ambassadorRewardSun
        );
        requestedForProcessingCount += 1;
      }
    }

    return {
      totalBuyers: buyers.size,
      trackedVolumeSun,

      claimableRewardsSun: "0",
      availableOnChainSun: "0",
      availableOnChainCount: 0,

      allocatedInDbSun,
      allocatedInDbCount,

      pendingBackendSyncSun,
      pendingBackendSyncCount,

      requestedForProcessingSun,
      requestedForProcessingCount,

      lifetimeRewardsSun,
      withdrawnRewardsSun,

      missingRewardCount,
      missingRewardOwnerShareSun,
      hasBrokenPendingRewards: missingRewardCount > 0,

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
      record.status === "withdraw_completed" ||
      record.status === "ignored" ||
      record.status === "allocation_failed_final"
    );
  }
}

export function createPurchaseStore(): PurchaseStore {
  return new PostgresPurchaseStore();
}
