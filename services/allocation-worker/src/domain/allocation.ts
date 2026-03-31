import type {
  AllocationMode,
  PurchaseRecord,
  PurchaseStore
} from "../db/purchases";

export interface AllocationDependencies {
  store: PurchaseStore;
  executor: AllocationExecutor;
  now?: () => number;
  logger?: AllocationLogger;
}

export interface AllocationLogger {
  info?(payload: Record<string, unknown>): void;
  warn?(payload: Record<string, unknown>): void;
  error?(payload: Record<string, unknown>): void;
}

export interface AllocationExecutorInput {
  purchase: PurchaseRecord;
  feeLimitSun?: number;
}

export interface AllocationExecutorResult {
  txid: string;
}

export interface AllocationExecutor {
  allocate(
    input: AllocationExecutorInput
  ): Promise<AllocationExecutorResult>;
}

export type AllocationAttemptStatus =
  | "allocated"
  | "deferred"
  | "retryable-failed"
  | "final-failed"
  | "skipped-already-final"
  | "skipped-no-ambassador-wallet"
  | "stopped-on-resource-shortage";

export interface AllocationAttemptResult {
  status: AllocationAttemptStatus;
  purchase: PurchaseRecord;
  txid: string | null;
  reason: string | null;
  errorCode: string | null;
  errorMessage: string | null;
}

export type AllocationDecision = AllocationAttemptResult;

export interface ReplayFailedAllocationResult {
  status: "allocated" | "failed" | "skipped";
  purchase: PurchaseRecord;
  ambassadorWallet: string | null;
  txid: string | null;
  reason: string | null;
  errorCode: string | null;
  errorMessage: string | null;
}

export interface PrepareWithdrawBatchInput {
  ambassadorWallet: string;
  limit?: number;
}

export interface PrepareWithdrawBatchResult {
  ambassadorWallet: string;
  purchases: PurchaseRecord[];
}

export interface AllocatePendingBatchInput {
  ambassadorWallet: string;
  feeLimitSun?: number;
  limit?: number;
  allocationMode?: AllocationMode;
  stopOnFirstDeferred?: boolean;
}

export interface AllocatePendingBatchResult {
  ambassadorWallet: string;
  processed: AllocationAttemptResult[];
  stoppedEarly: boolean;
  stopReason: string | null;
}

type AllocationErrorKind = "resource" | "retryable" | "final" | "unknown";

interface ClassifiedAllocationError {
  kind: AllocationErrorKind;
  code: string | null;
  reason: string;
  message: string;
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

  try {
    return JSON.stringify(error);
  } catch {
    return "Unknown allocation error";
  }
}

function toLowerSafe(value: unknown): string {
  return typeof value === "string" ? value.toLowerCase() : "";
}

function extractErrorCode(error: unknown): string | null {
  if (!error || typeof error !== "object") {
    return null;
  }

  const candidates: unknown[] = [
    (error as any).code,
    (error as any).errorCode,
    (error as any).error_code,
    (error as any).name,
    (error as any).response?.status,
    (error as any).statusCode,
    (error as any).status
  ];

  for (const candidate of candidates) {
    if (candidate == null) {
      continue;
    }

    const normalized = String(candidate).trim();

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function isResourceInsufficientMessage(message: string): boolean {
  const value = message.toLowerCase();

  return (
    value.includes("account resource insufficient") ||
    value.includes("resource insufficient") ||
    value.includes("out of energy") ||
    value.includes("energy limit") ||
    value.includes("insufficient energy") ||
    value.includes("insufficient bandwidth") ||
    value.includes("bandwidth limit") ||
    value.includes("account resources are insufficient") ||
    value.includes("not enough energy") ||
    value.includes("not enough bandwidth") ||
    value.includes("gasstation service balance") ||
    value.includes("gasstation balance") ||
    value.includes("top-up") ||
    value.includes("top up")
  );
}

function isRateLimitedMessage(message: string): boolean {
  const value = message.toLowerCase();

  return (
    value.includes("status code 429") ||
    value.includes("http 429") ||
    value.includes("429") ||
    value.includes("too many requests") ||
    value.includes("rate limit") ||
    value.includes("rate limited")
  );
}

function isRetryableTransportMessage(message: string): boolean {
  const value = message.toLowerCase();

  return (
    value.includes("timeout") ||
    value.includes("timed out") ||
    value.includes("network") ||
    value.includes("socket hang up") ||
    value.includes("econnreset") ||
    value.includes("503") ||
    value.includes("502") ||
    value.includes("gateway") ||
    value.includes("temporar") ||
    value.includes("service unavailable")
  );
}

function isFinalMessage(message: string): boolean {
  const value = message.toLowerCase();

  return (
    value.includes("contract validate error") ||
    value.includes("revert") ||
    value.includes("invalid address") ||
    value.includes("owner address is not set") ||
    value.includes("purchase not eligible") ||
    value.includes("already allocated") ||
    value.includes("already processed") ||
    value.includes("permission denied")
  );
}

function isKnownRetryableCode(lowerCode: string): boolean {
  return (
    lowerCode === "429" ||
    lowerCode.includes("rate_limit") ||
    lowerCode.includes("rate limited") ||
    lowerCode.includes("too_many_requests") ||
    lowerCode.includes("timeout") ||
    lowerCode.includes("network") ||
    lowerCode.includes("econnreset") ||
    lowerCode.includes("temporar") ||
    lowerCode.includes("gasstation_rate_limit") ||
    lowerCode.includes("tron_rate_limit") ||
    lowerCode.includes("gasstation_order_failed") ||
    lowerCode.includes("gasstation_topup_transfer_failed") ||
    lowerCode.includes("gasstation_topup_not_settled") ||
    lowerCode.includes("gasstation_topup_failed") ||
    lowerCode.includes("gasstation_fetch_failed") ||
    lowerCode.includes("gasstation_invalid_response") ||
    lowerCode.includes("gasstation_timeout") ||
    lowerCode.includes("gasstation_http_5") ||
    lowerCode.includes("gasstation_error_100003")
  );
}

function isKnownResourceCode(lowerCode: string): boolean {
  return (
    lowerCode.includes("out_of_energy") ||
    lowerCode.includes("insufficient") ||
    lowerCode.includes("bandwidth") ||
    lowerCode.includes("energy") ||
    lowerCode.includes("account_resource") ||
    lowerCode.includes("gasstation_operator_balance_low") ||
    lowerCode.includes("gasstation_service_balance_low") ||
    lowerCode.includes("gasstation_topup_not_settled") ||
    lowerCode.includes("gasstation_topup_transfer_failed") ||
    lowerCode.includes("gasstation_topup_failed") ||
    lowerCode.includes("account_resource_consumed_before_send") ||
    lowerCode.includes("account_resource_insufficient_during_send")
  );
}

function isKnownFinalCode(lowerCode: string): boolean {
  return (
    lowerCode.includes("invalid_status") ||
    lowerCode.includes("no_ambassador_wallet")
  );
}

function classifyAllocationError(error: unknown): ClassifiedAllocationError {
  const message = toErrorMessage(error);
  const code = extractErrorCode(error);
  const lowerCode = toLowerSafe(code);

  if (isKnownResourceCode(lowerCode) || isResourceInsufficientMessage(message)) {
    return {
      kind: "resource",
      code,
      reason: message,
      message
    };
  }

  if (
    isRateLimitedMessage(message) ||
    lowerCode === "429" ||
    lowerCode.includes("rate_limit") ||
    lowerCode.includes("rate limited") ||
    lowerCode.includes("too_many_requests") ||
    lowerCode.includes("gasstation_rate_limit") ||
    lowerCode.includes("tron_rate_limit")
  ) {
    return {
      kind: "retryable",
      code,
      reason: "Temporary allocation rate limit error.",
      message
    };
  }

  if (
    isRetryableTransportMessage(message) ||
    lowerCode.includes("timeout") ||
    lowerCode.includes("network") ||
    lowerCode.includes("econnreset") ||
    lowerCode.includes("temporar") ||
    lowerCode.includes("gasstation_fetch_failed") ||
    lowerCode.includes("gasstation_invalid_response") ||
    lowerCode.includes("gasstation_timeout")
  ) {
    return {
      kind: "retryable",
      code,
      reason: "Temporary allocation transport error.",
      message
    };
  }

  if (isKnownFinalCode(lowerCode) || isFinalMessage(message)) {
    return {
      kind: "final",
      code,
      reason: message,
      message
    };
  }

  if (lowerCode === "err_bad_request" && !isRateLimitedMessage(message)) {
    return {
      kind: "final",
      code,
      reason: message,
      message
    };
  }

  if (isKnownRetryableCode(lowerCode)) {
    return {
      kind: "retryable",
      code,
      reason: message,
      message
    };
  }

  return {
    kind: "unknown",
    code,
    reason: message,
    message
  };
}

function isFinalPurchaseStatus(status: PurchaseRecord["status"]): boolean {
  return (
    status === "allocated" ||
    status === "withdraw_completed" ||
    status === "ignored" ||
    status === "allocation_failed_final"
  );
}

function isClaimQueueEligible(status: PurchaseRecord["status"]): boolean {
  return (
    status === "verified" ||
    status === "deferred" ||
    status === "allocation_failed_retryable"
  );
}

export class AllocationService {
  private readonly store: PurchaseStore;
  private readonly executor: AllocationExecutor;
  private readonly now: () => number;
  private readonly logger?: AllocationLogger;

  constructor(deps: AllocationDependencies) {
    this.store = deps.store;
    this.executor = deps.executor;
    this.now = deps.now ?? (() => Date.now());
    this.logger = deps.logger;
  }

  async executeAllocation(input: {
    purchaseId: string;
    feeLimitSun?: number;
    allocationMode?: AllocationMode;
    now?: number;
  }): Promise<AllocationDecision> {
    const purchase = await this.store.getByPurchaseId(input.purchaseId);

    if (!purchase) {
      throw new Error(`Purchase not found: ${input.purchaseId}`);
    }

    return this.tryAllocatePurchaseRecord(purchase, {
      feeLimitSun: input.feeLimitSun,
      allocationMode: input.allocationMode,
      nowOverride: input.now
    });
  }

  async tryAllocateVerifiedPurchase(
    purchaseId: string,
    options?: {
      feeLimitSun?: number;
      allocationMode?: AllocationMode;
    }
  ): Promise<AllocationAttemptResult> {
    const purchase = await this.store.getByPurchaseId(purchaseId);

    if (!purchase) {
      throw new Error(`Purchase not found: ${purchaseId}`);
    }

    return this.tryAllocatePurchaseRecord(purchase, {
      feeLimitSun: options?.feeLimitSun,
      allocationMode: options?.allocationMode ?? "eager"
    });
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<ReplayFailedAllocationResult> {
    const purchase = await this.store.getByPurchaseId(purchaseId);

    if (!purchase) {
      throw new Error(`Purchase not found: ${purchaseId}`);
    }

    if (isFinalPurchaseStatus(purchase.status) && purchase.status !== "allocated") {
      return {
        status: "skipped",
        purchase,
        ambassadorWallet: purchase.ambassadorWallet,
        txid: null,
        reason: `Purchase already finalized with status: ${purchase.status}`,
        errorCode: null,
        errorMessage: null
      };
    }

    const result = await this.tryAllocatePurchaseRecord(purchase, {
      feeLimitSun,
      allocationMode: "manual-replay",
      nowOverride: now
    });

    return {
      status:
        result.status === "allocated"
          ? "allocated"
          : result.status === "skipped-already-final" ||
              result.status === "skipped-no-ambassador-wallet"
            ? "skipped"
            : "failed",
      purchase: result.purchase,
      ambassadorWallet: result.purchase.ambassadorWallet,
      txid: result.txid,
      reason: result.reason,
      errorCode: result.errorCode,
      errorMessage: result.errorMessage
    };
  }

  async prepareWithdrawBatch(
    input: PrepareWithdrawBatchInput
  ): Promise<PrepareWithdrawBatchResult> {
    const ambassadorWallet = String(input.ambassadorWallet || "").trim();

    if (!ambassadorWallet) {
      throw new Error("ambassadorWallet is required");
    }

    const purchases = await this.store.listPendingByAmbassador({
      ambassadorWallet,
      limit: input.limit
    });

    return {
      ambassadorWallet,
      purchases
    };
  }

  async allocatePendingBatch(
    input: AllocatePendingBatchInput
  ): Promise<AllocatePendingBatchResult> {
    const ambassadorWallet = String(input.ambassadorWallet || "").trim();

    if (!ambassadorWallet) {
      throw new Error("ambassadorWallet is required");
    }

    const pending = await this.store.listPendingByAmbassador({
      ambassadorWallet,
      limit: input.limit
    });

    const processed: AllocationAttemptResult[] = [];
    let stoppedEarly = false;
    let stopReason: string | null = null;
    const stopOnFirstDeferred = input.stopOnFirstDeferred !== false;

    for (const purchase of pending) {
      const result = await this.tryAllocatePurchaseRecord(purchase, {
        feeLimitSun: input.feeLimitSun,
        allocationMode: input.allocationMode ?? "claim-first"
      });

      processed.push(result);

      if (
        stopOnFirstDeferred &&
        (result.status === "deferred" || result.status === "stopped-on-resource-shortage")
      ) {
        stoppedEarly = true;
        stopReason =
          result.reason ||
          "Allocation stopped because resources were not sufficient.";
        break;
      }
    }

    return {
      ambassadorWallet,
      processed,
      stoppedEarly,
      stopReason
    };
  }

  private async tryAllocatePurchaseRecord(
    purchase: PurchaseRecord,
    options: {
      feeLimitSun?: number;
      allocationMode?: AllocationMode;
      nowOverride?: number;
    }
  ): Promise<AllocationAttemptResult> {
    const now = options.nowOverride ?? this.now();
    const allocationMode = options.allocationMode ?? "eager";

    if (isFinalPurchaseStatus(purchase.status)) {
      return {
        status: "skipped-already-final",
        purchase,
        txid: null,
        reason: `Purchase already finalized with status: ${purchase.status}`,
        errorCode: null,
        errorMessage: null
      };
    }

    if (!isClaimQueueEligible(purchase.status) && purchase.status !== "allocation_in_progress") {
      const refreshed = await this.store.markAllocationRetryableFailed(purchase.purchaseId, {
        reason: `Purchase is not eligible for allocation from status: ${purchase.status}`,
        allocationMode,
        errorCode: "INVALID_STATUS",
        errorMessage: `Purchase is not eligible for allocation from status: ${purchase.status}`,
        now
      });

      return {
        status: "retryable-failed",
        purchase: refreshed,
        txid: null,
        reason: refreshed.failureReason,
        errorCode: "INVALID_STATUS",
        errorMessage: `Purchase is not eligible for allocation from status: ${purchase.status}`
      };
    }

    if (!purchase.ambassadorWallet) {
      const deferred = await this.store.markDeferred(purchase.purchaseId, {
        reason: "Ambassador wallet is missing for this purchase.",
        allocationMode,
        errorCode: "NO_AMBASSADOR_WALLET",
        errorMessage: "Ambassador wallet is missing for this purchase.",
        now
      });

      return {
        status: "skipped-no-ambassador-wallet",
        purchase: deferred,
        txid: null,
        reason: "Ambassador wallet is missing for this purchase.",
        errorCode: "NO_AMBASSADOR_WALLET",
        errorMessage: "Ambassador wallet is missing for this purchase."
      };
    }

    const inProgress = await this.store.markAllocationInProgress(purchase.purchaseId, {
      allocationMode,
      now
    });

    try {
      this.logger?.info?.({
        scope: "allocation",
        stage: "execute-start",
        purchaseId: inProgress.purchaseId,
        txHash: inProgress.txHash,
        ambassadorWallet: inProgress.ambassadorWallet,
        allocationMode,
        feeLimitSun: options.feeLimitSun ?? null
      });

      const execution = await this.executor.allocate({
        purchase: inProgress,
        feeLimitSun: options.feeLimitSun
      });

      const allocated = await this.store.markAllocated(inProgress.purchaseId, {
        ambassadorWallet: inProgress.ambassadorWallet,
        allocationMode,
        now: this.now()
      });

      this.logger?.info?.({
        scope: "allocation",
        stage: "execute-success",
        purchaseId: allocated.purchaseId,
        txHash: allocated.txHash,
        ambassadorWallet: allocated.ambassadorWallet,
        allocationMode,
        txid: execution.txid
      });

      return {
        status: "allocated",
        purchase: allocated,
        txid: execution.txid,
        reason: null,
        errorCode: null,
        errorMessage: null
      };
    } catch (error) {
      const classified = classifyAllocationError(error);

      this.logger?.warn?.({
        scope: "allocation",
        stage: "execute-failed",
        purchaseId: inProgress.purchaseId,
        txHash: inProgress.txHash,
        ambassadorWallet: inProgress.ambassadorWallet,
        allocationMode,
        kind: classified.kind,
        code: classified.code,
        reason: classified.reason,
        message: classified.message
      });

      if (classified.kind === "resource") {
        const deferred = await this.store.markDeferred(inProgress.purchaseId, {
          reason: classified.reason,
          allocationMode,
          errorCode: classified.code,
          errorMessage: classified.message,
          now: this.now()
        });

        return {
          status:
            allocationMode === "claim-first"
              ? "stopped-on-resource-shortage"
              : "deferred",
          purchase: deferred,
          txid: null,
          reason: classified.reason,
          errorCode: classified.code,
          errorMessage: classified.message
        };
      }

      if (classified.kind === "retryable" || classified.kind === "unknown") {
        const failed = await this.store.markAllocationRetryableFailed(
          inProgress.purchaseId,
          {
            reason: classified.reason,
            allocationMode,
            errorCode: classified.code,
            errorMessage: classified.message,
            now: this.now()
          }
        );

        return {
          status: "retryable-failed",
          purchase: failed,
          txid: null,
          reason: classified.reason,
          errorCode: classified.code,
          errorMessage: classified.message
        };
      }

      const finalFailed = await this.store.markAllocationFinalFailed(
        inProgress.purchaseId,
        {
          reason: classified.reason,
          allocationMode,
          errorCode: classified.code,
          errorMessage: classified.message,
          now: this.now()
        }
      );

      return {
        status: "final-failed",
        purchase: finalFailed,
        txid: null,
        reason: classified.reason,
        errorCode: classified.code,
        errorMessage: classified.message
      };
    }
  }
}
