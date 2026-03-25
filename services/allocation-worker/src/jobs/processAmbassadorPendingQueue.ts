import type { AllocationWorker } from "../index";

export interface ProcessAmbassadorPendingQueueJobOptions {
  ambassadorSlug?: string;
  ambassadorWallet?: string;
  limit?: number;
  now?: number;
  feeLimitSun?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface ProcessAmbassadorPendingQueueJobItem {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug: string;
  ambassadorWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  status: string;
  action: "allocated" | "deferred" | "skipped" | "failed";
  reason: string | null;
  txid: string | null;
}

export interface ProcessAmbassadorPendingQueueJobResult {
  ok: boolean;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  scanned: number;
  allocated: number;
  deferred: number;
  skipped: number;
  failed: number;
  items: ProcessAmbassadorPendingQueueJobItem[];
  startedAt: number;
  finishedAt: number;
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
}

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

function toOptionalPositiveInteger(value: unknown): number | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return undefined;
  }

  return parsed;
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

function isResourceError(message: string): boolean {
  const normalized = message.toLowerCase();

  return (
    normalized.includes("resource insufficient") ||
    normalized.includes("account resource insufficient") ||
    normalized.includes("out of energy") ||
    normalized.includes("insufficient energy") ||
    normalized.includes("insufficient bandwidth") ||
    normalized.includes("bandwidth") ||
    normalized.includes("energy")
  );
}

async function listPendingQueuePurchases(
  worker: AllocationWorker,
  options: {
    ambassadorSlug?: string;
    ambassadorWallet?: string;
    limit: number;
  }
): Promise<any[]> {
  const storeAny = worker.store as any;

  if (typeof storeAny.listPendingAmbassadorWithdrawalPurchases === "function") {
    return storeAny.listPendingAmbassadorWithdrawalPurchases({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  if (typeof storeAny.listQueuedWithdrawalPurchases === "function") {
    return storeAny.listQueuedWithdrawalPurchases({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  if (typeof storeAny.listPurchasesByStatus === "function") {
    return storeAny.listPurchasesByStatus("pending_ambassador_withdrawal", {
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  throw new Error(
    "Purchase store does not support listing pending ambassador withdrawal queue"
  );
}

async function markDeferred(
  worker: AllocationWorker,
  purchaseId: string,
  reason: string,
  now: number
): Promise<void> {
  const storeAny = worker.store as any;

  if (typeof storeAny.markPurchaseDeferred === "function") {
    await storeAny.markPurchaseDeferred(purchaseId, reason, now);
    return;
  }

  if (typeof storeAny.updatePurchaseStatus === "function") {
    await storeAny.updatePurchaseStatus(
      purchaseId,
      "deferred",
      reason,
      now
    );
    return;
  }

  throw new Error("Purchase store does not support deferred status updates");
}

async function markFailed(
  worker: AllocationWorker,
  purchaseId: string,
  reason: string,
  now: number
): Promise<void> {
  const storeAny = worker.store as any;

  if (typeof storeAny.markPurchaseFailed === "function") {
    await storeAny.markPurchaseFailed(purchaseId, reason, now);
    return;
  }

  if (typeof storeAny.updatePurchaseStatus === "function") {
    await storeAny.updatePurchaseStatus(
      purchaseId,
      "failed",
      reason,
      now
    );
    return;
  }

  throw new Error("Purchase store does not support failure status updates");
}

async function canAllocateNow(
  worker: AllocationWorker,
  purchase: any,
  feeLimitSun: number | undefined
): Promise<{ ok: boolean; reason: string | null }> {
  const processorAny = worker.processor as any;

  if (typeof processorAny.checkAllocationResources === "function") {
    const result = await processorAny.checkAllocationResources({
      purchaseId: purchase.purchaseId,
      buyerWallet: purchase.buyerWallet,
      ambassadorWallet: purchase.ambassadorWallet,
      purchaseAmountSun: purchase.purchaseAmountSun,
      ownerShareSun: purchase.ownerShareSun,
      feeLimitSun
    });

    if (result && typeof result === "object") {
      return {
        ok: Boolean((result as any).ok),
        reason: normalizeOptionalString((result as any).reason) ?? null
      };
    }
  }

  return { ok: true, reason: null };
}

async function replayOnePurchase(
  worker: AllocationWorker,
  purchaseId: string,
  feeLimitSun: number | undefined,
  now: number
): Promise<any> {
  const processorAny = worker.processor as any;

  if (typeof processorAny.replayFailedAllocation === "function") {
    return processorAny.replayFailedAllocation(purchaseId, feeLimitSun, now);
  }

  if (typeof processorAny.processPendingPurchaseAllocation === "function") {
    return processorAny.processPendingPurchaseAllocation({
      purchaseId,
      feeLimitSun,
      now
    });
  }

  throw new Error("Allocation processor does not support replaying pending purchases");
}

export async function processAmbassadorPendingQueue(
  worker: AllocationWorker,
  options: ProcessAmbassadorPendingQueueJobOptions = {}
): Promise<ProcessAmbassadorPendingQueueJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 100);
  const feeLimitSun = toOptionalPositiveInteger(options.feeLimitSun);

  const ambassadorSlug = normalizeOptionalString(options.ambassadorSlug) ?? null;
  const ambassadorWallet = normalizeOptionalString(options.ambassadorWallet) ?? null;

  const result: ProcessAmbassadorPendingQueueJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    scanned: 0,
    allocated: 0,
    deferred: 0,
    skipped: 0,
    failed: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    const purchases = await listPendingQueuePurchases(worker, {
      ambassadorSlug: ambassadorSlug ?? undefined,
      ambassadorWallet: ambassadorWallet ?? undefined,
      limit
    });

    result.scanned = purchases.length;

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "processAmbassadorPendingQueue",
        message: "Loaded pending ambassador withdrawal queue",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        limit
      })
    );

    for (const purchase of purchases) {
      const purchaseId = String(purchase.purchaseId || "").trim();
      const status = String(purchase.status || "").trim();

      if (!purchaseId) {
        result.skipped += 1;
        result.items.push({
          purchaseId: "",
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: status || "unknown",
          action: "skipped",
          reason: "Missing purchaseId",
          txid: null
        });
        continue;
      }

      if (
        status !== "pending_ambassador_withdrawal" &&
        status !== "queued_for_withdrawal" &&
        status !== "deferred"
      ) {
        result.skipped += 1;
        result.items.push({
          purchaseId,
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: status || "unknown",
          action: "skipped",
          reason: `Unsupported queue status: ${status || "unknown"}`,
          txid: null
        });
        continue;
      }

      const resourceCheck = await canAllocateNow(worker, purchase, feeLimitSun);

      if (!resourceCheck.ok) {
        const reason = resourceCheck.reason || "Not enough resources for allocation";

        await markDeferred(worker, purchaseId, reason, now);

        result.deferred += 1;
        result.items.push({
          purchaseId,
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: "deferred",
          action: "deferred",
          reason,
          txid: null
        });

        continue;
      }

      try {
        const replayResult = await replayOnePurchase(worker, purchaseId, feeLimitSun, now);
        const replayStatus = String(replayResult?.status || "").trim().toLowerCase();
        const txid = normalizeOptionalString(replayResult?.txid) ?? null;
        const reason = normalizeOptionalString(replayResult?.reason) ?? null;

        if (replayStatus === "allocated") {
          result.allocated += 1;
          result.items.push({
            purchaseId,
            txHash: String(purchase.txHash || ""),
            buyerWallet: String(purchase.buyerWallet || ""),
            ambassadorSlug: String(purchase.ambassadorSlug || ""),
            ambassadorWallet: String(purchase.ambassadorWallet || ""),
            purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
            ownerShareSun: String(purchase.ownerShareSun ?? "0"),
            status: "allocated",
            action: "allocated",
            reason: null,
            txid
          });
          continue;
        }

        if (replayStatus === "deferred") {
          result.deferred += 1;
          result.items.push({
            purchaseId,
            txHash: String(purchase.txHash || ""),
            buyerWallet: String(purchase.buyerWallet || ""),
            ambassadorSlug: String(purchase.ambassadorSlug || ""),
            ambassadorWallet: String(purchase.ambassadorWallet || ""),
            purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
            ownerShareSun: String(purchase.ownerShareSun ?? "0"),
            status: "deferred",
            action: "deferred",
            reason: reason || "Deferred by allocation processor",
            txid
          });
          continue;
        }

        if (replayStatus === "failed") {
          const failureReason = reason || "Allocation processor returned failed status";

          if (isResourceError(failureReason)) {
            await markDeferred(worker, purchaseId, failureReason, now);

            result.deferred += 1;
            result.items.push({
              purchaseId,
              txHash: String(purchase.txHash || ""),
              buyerWallet: String(purchase.buyerWallet || ""),
              ambassadorSlug: String(purchase.ambassadorSlug || ""),
              ambassadorWallet: String(purchase.ambassadorWallet || ""),
              purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
              ownerShareSun: String(purchase.ownerShareSun ?? "0"),
              status: "deferred",
              action: "deferred",
              reason: failureReason,
              txid
            });
          } else {
            await markFailed(worker, purchaseId, failureReason, now);

            result.failed += 1;
            result.items.push({
              purchaseId,
              txHash: String(purchase.txHash || ""),
              buyerWallet: String(purchase.buyerWallet || ""),
              ambassadorSlug: String(purchase.ambassadorSlug || ""),
              ambassadorWallet: String(purchase.ambassadorWallet || ""),
              purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
              ownerShareSun: String(purchase.ownerShareSun ?? "0"),
              status: "failed",
              action: "failed",
              reason: failureReason,
              txid
            });
          }

          continue;
        }

        result.skipped += 1;
        result.items.push({
          purchaseId,
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: replayStatus || "unknown",
          action: "skipped",
          reason: reason || `Unexpected replay status: ${replayStatus || "unknown"}`,
          txid
        });
      } catch (error) {
        const reason = toErrorMessage(error);

        if (isResourceError(reason)) {
          await markDeferred(worker, purchaseId, reason, now);

          result.deferred += 1;
          result.items.push({
            purchaseId,
            txHash: String(purchase.txHash || ""),
            buyerWallet: String(purchase.buyerWallet || ""),
            ambassadorSlug: String(purchase.ambassadorSlug || ""),
            ambassadorWallet: String(purchase.ambassadorWallet || ""),
            purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
            ownerShareSun: String(purchase.ownerShareSun ?? "0"),
            status: "deferred",
            action: "deferred",
            reason,
            txid: null
          });
        } else {
          await markFailed(worker, purchaseId, reason, now);

          result.failed += 1;
          result.items.push({
            purchaseId,
            txHash: String(purchase.txHash || ""),
            buyerWallet: String(purchase.buyerWallet || ""),
            ambassadorSlug: String(purchase.ambassadorSlug || ""),
            ambassadorWallet: String(purchase.ambassadorWallet || ""),
            purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
            ownerShareSun: String(purchase.ownerShareSun ?? "0"),
            status: "failed",
            action: "failed",
            reason,
            txid: null
          });
        }
      }
    }

    result.finishedAt = Date.now();

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "processAmbassadorPendingQueue",
        message: "Ambassador pending queue processing finished",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        allocated: result.allocated,
        deferred: result.deferred,
        skipped: result.skipped,
        failed: result.failed,
        durationMs: result.finishedAt - result.startedAt
      })
    );

    return result;
  } catch (error) {
    result.ok = false;
    result.finishedAt = Date.now();

    logger.error?.(
      JSON.stringify({
        ok: false,
        job: "processAmbassadorPendingQueue",
        ambassadorSlug,
        ambassadorWallet,
        error: toErrorMessage(error)
      })
    );

    throw error;
  }
}
