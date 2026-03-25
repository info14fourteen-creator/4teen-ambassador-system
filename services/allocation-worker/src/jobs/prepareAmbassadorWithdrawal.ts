import type { AllocationWorker } from "../index";

export interface PrepareAmbassadorWithdrawalJobOptions {
  ambassadorSlug?: string;
  ambassadorWallet?: string;
  limit?: number;
  now?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface PrepareAmbassadorWithdrawalJobItem {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug: string;
  ambassadorWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  status: string;
  queuedForWithdrawal: boolean;
  reason: string | null;
}

export interface PrepareAmbassadorWithdrawalJobResult {
  ok: boolean;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  scanned: number;
  prepared: number;
  skipped: number;
  items: PrepareAmbassadorWithdrawalJobItem[];
  startedAt: number;
  finishedAt: number;
}

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
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

async function loadCandidatePurchases(
  worker: AllocationWorker,
  options: {
    ambassadorSlug?: string;
    ambassadorWallet?: string;
    limit: number;
  }
): Promise<any[]> {
  const storeAny = worker.store as any;

  if (typeof storeAny.listAllocatedPurchasesForWithdrawalPreparation === "function") {
    return storeAny.listAllocatedPurchasesForWithdrawalPreparation({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  if (typeof storeAny.listAllocatedPurchasesByAmbassador === "function") {
    return storeAny.listAllocatedPurchasesByAmbassador({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  if (typeof storeAny.listAllocatedPurchases === "function") {
    return storeAny.listAllocatedPurchases({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  throw new Error(
    "Purchase store does not support listing allocated purchases for withdrawal preparation"
  );
}

async function markPreparedForWithdrawal(
  worker: AllocationWorker,
  purchaseId: string,
  now: number
): Promise<void> {
  const storeAny = worker.store as any;

  if (typeof storeAny.markPurchasePendingAmbassadorWithdrawal === "function") {
    await storeAny.markPurchasePendingAmbassadorWithdrawal(purchaseId, now);
    return;
  }

  if (typeof storeAny.markPurchaseQueuedForWithdrawal === "function") {
    await storeAny.markPurchaseQueuedForWithdrawal(purchaseId, now);
    return;
  }

  if (typeof storeAny.updatePurchaseStatus === "function") {
    await storeAny.updatePurchaseStatus(purchaseId, "pending_ambassador_withdrawal", null, now);
    return;
  }

  throw new Error(
    "Purchase store does not support marking purchase as pending ambassador withdrawal"
  );
}

export async function prepareAmbassadorWithdrawal(
  worker: AllocationWorker,
  options: PrepareAmbassadorWithdrawalJobOptions = {}
): Promise<PrepareAmbassadorWithdrawalJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 500);

  const ambassadorSlug = normalizeOptionalString(options.ambassadorSlug) ?? null;
  const ambassadorWallet = normalizeOptionalString(options.ambassadorWallet) ?? null;

  const result: PrepareAmbassadorWithdrawalJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    scanned: 0,
    prepared: 0,
    skipped: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    const purchases = await loadCandidatePurchases(worker, {
      ambassadorSlug: ambassadorSlug ?? undefined,
      ambassadorWallet: ambassadorWallet ?? undefined,
      limit
    });

    result.scanned = purchases.length;

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "prepareAmbassadorWithdrawal",
        message: "Loaded purchases for ambassador withdrawal preparation",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        limit
      })
    );

    for (const purchase of purchases) {
      const currentStatus = String(purchase.status || "").trim();

      if (
        currentStatus !== "allocated" &&
        currentStatus !== "ready_for_withdrawal" &&
        currentStatus !== "queued_for_withdrawal"
      ) {
        result.skipped += 1;
        result.items.push({
          purchaseId: purchase.purchaseId,
          txHash: purchase.txHash,
          buyerWallet: purchase.buyerWallet,
          ambassadorSlug: purchase.ambassadorSlug,
          ambassadorWallet: purchase.ambassadorWallet,
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus || "unknown",
          queuedForWithdrawal: false,
          reason: `Unsupported status for withdrawal preparation: ${currentStatus || "unknown"}`
        });
        continue;
      }

      if (currentStatus === "queued_for_withdrawal") {
        result.skipped += 1;
        result.items.push({
          purchaseId: purchase.purchaseId,
          txHash: purchase.txHash,
          buyerWallet: purchase.buyerWallet,
          ambassadorSlug: purchase.ambassadorSlug,
          ambassadorWallet: purchase.ambassadorWallet,
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus,
          queuedForWithdrawal: true,
          reason: "Already queued for withdrawal"
        });
        continue;
      }

      await markPreparedForWithdrawal(worker, purchase.purchaseId, now);

      result.prepared += 1;
      result.items.push({
        purchaseId: purchase.purchaseId,
        txHash: purchase.txHash,
        buyerWallet: purchase.buyerWallet,
        ambassadorSlug: purchase.ambassadorSlug,
        ambassadorWallet: purchase.ambassadorWallet,
        purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
        ownerShareSun: String(purchase.ownerShareSun ?? "0"),
        status: "pending_ambassador_withdrawal",
        queuedForWithdrawal: true,
        reason: null
      });
    }

    result.finishedAt = Date.now();

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "prepareAmbassadorWithdrawal",
        message: "Ambassador withdrawal preparation finished",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        prepared: result.prepared,
        skipped: result.skipped,
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
        job: "prepareAmbassadorWithdrawal",
        ambassadorSlug,
        ambassadorWallet,
        error: toErrorMessage(error)
      })
    );

    throw error;
  }
}
