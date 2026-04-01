import type { AllocationWorker } from "../index";

export interface FinalizeAmbassadorWithdrawalJobOptions {
  ambassadorSlug?: string;
  ambassadorWallet?: string;
  withdrawSessionId?: string;
  txid?: string;
  limit?: number;
  now?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface FinalizeAmbassadorWithdrawalJobItem {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug: string;
  ambassadorWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  previousStatus: string;
  status: string;
  withdrawSessionId: string | null;
  finalized: boolean;
  reason: string | null;
}

export interface FinalizeAmbassadorWithdrawalJobResult {
  ok: boolean;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  withdrawSessionId: string | null;
  txid: string | null;
  scanned: number;
  finalized: number;
  skipped: number;
  items: FinalizeAmbassadorWithdrawalJobItem[];
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

async function resolveAmbassadorWallet(
  worker: AllocationWorker,
  input: {
    ambassadorSlug?: string;
    ambassadorWallet?: string;
  }
): Promise<{
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
}> {
  const ambassadorWallet = normalizeOptionalString(input.ambassadorWallet) ?? null;
  const ambassadorSlug = normalizeOptionalString(input.ambassadorSlug) ?? null;

  if (ambassadorWallet) {
    return {
      ambassadorSlug,
      ambassadorWallet
    };
  }

  if (!ambassadorSlug) {
    throw new Error("ambassadorWallet or ambassadorSlug is required");
  }

  const record = await worker.store.getAmbassadorBySlug(ambassadorSlug);

  if (!record?.wallet) {
    throw new Error(`Ambassador wallet not found for slug: ${ambassadorSlug}`);
  }

  return {
    ambassadorSlug,
    ambassadorWallet: record.wallet
  };
}

async function loadWithdrawIncludedPurchases(
  worker: AllocationWorker,
  options: {
    ambassadorWallet: string;
    withdrawSessionId?: string;
    limit: number;
  }
): Promise<any[]> {
  const rows = await worker.store.listPendingByAmbassador({
    ambassadorWallet: options.ambassadorWallet,
    statuses: ["withdraw_included"],
    limit: options.limit
  });

  if (!options.withdrawSessionId) {
    return rows;
  }

  return rows.filter((purchase) => {
    return String(purchase.withdrawSessionId || "").trim() === options.withdrawSessionId;
  });
}

export async function finalizeAmbassadorWithdrawal(
  worker: AllocationWorker,
  options: FinalizeAmbassadorWithdrawalJobOptions = {}
): Promise<FinalizeAmbassadorWithdrawalJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 1000);
  const txid = normalizeOptionalString(options.txid) ?? null;
  const requestedWithdrawSessionId = normalizeOptionalString(options.withdrawSessionId) ?? null;

  const resolved = await resolveAmbassadorWallet(worker, {
    ambassadorSlug: options.ambassadorSlug,
    ambassadorWallet: options.ambassadorWallet
  });

  const ambassadorSlug = resolved.ambassadorSlug;
  const ambassadorWallet = resolved.ambassadorWallet;

  const result: FinalizeAmbassadorWithdrawalJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    withdrawSessionId: requestedWithdrawSessionId,
    txid,
    scanned: 0,
    finalized: 0,
    skipped: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    if (!ambassadorWallet) {
      throw new Error("Ambassador wallet is required");
    }

    const purchases = await loadWithdrawIncludedPurchases(worker, {
      ambassadorWallet,
      withdrawSessionId: requestedWithdrawSessionId ?? undefined,
      limit
    });

    result.scanned = purchases.length;

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "finalizeAmbassadorWithdrawal",
        message: "Loaded purchases for withdrawal finalization",
        ambassadorSlug,
        ambassadorWallet,
        withdrawSessionId: requestedWithdrawSessionId,
        txid,
        scanned: result.scanned,
        limit
      })
    );

    if (!purchases.length) {
      result.finishedAt = Date.now();

      logger.info?.(
        JSON.stringify({
          ok: true,
          job: "finalizeAmbassadorWithdrawal",
          message: "No withdraw_included purchases found for finalization",
          ambassadorSlug,
          ambassadorWallet,
          withdrawSessionId: requestedWithdrawSessionId,
          txid,
          scanned: 0,
          durationMs: result.finishedAt - result.startedAt
        })
      );

      return result;
    }

    for (const purchase of purchases) {
      const previousStatus = String(purchase.status || "").trim();
      const currentWithdrawSessionId = String(purchase.withdrawSessionId || "").trim();

      if (previousStatus !== "withdraw_included") {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          previousStatus,
          status: previousStatus || "unknown",
          withdrawSessionId: currentWithdrawSessionId || null,
          finalized: false,
          reason: `Unsupported status for withdrawal finalization: ${previousStatus || "unknown"}`
        });
        continue;
      }

      if (
        requestedWithdrawSessionId &&
        currentWithdrawSessionId !== requestedWithdrawSessionId
      ) {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          previousStatus,
          status: previousStatus,
          withdrawSessionId: currentWithdrawSessionId || null,
          finalized: false,
          reason: "Withdraw session mismatch"
        });
        continue;
      }

      const updated = await worker.store.markWithdrawCompleted(String(purchase.purchaseId), {
        withdrawSessionId: currentWithdrawSessionId || null,
        now
      });

      result.finalized += 1;

      result.items.push({
        purchaseId: String(updated?.purchaseId || purchase.purchaseId || ""),
        txHash: String(updated?.txHash || purchase.txHash || ""),
        buyerWallet: String(updated?.buyerWallet || purchase.buyerWallet || ""),
        ambassadorSlug: String(updated?.ambassadorSlug || purchase.ambassadorSlug || ""),
        ambassadorWallet: String(updated?.ambassadorWallet || purchase.ambassadorWallet || ""),
        purchaseAmountSun: String(updated?.purchaseAmountSun ?? purchase.purchaseAmountSun ?? "0"),
        ownerShareSun: String(updated?.ownerShareSun ?? purchase.ownerShareSun ?? "0"),
        previousStatus,
        status: String(updated?.status || "withdraw_completed"),
        withdrawSessionId: String(updated?.withdrawSessionId || currentWithdrawSessionId || "") || null,
        finalized: true,
        reason: null
      });
    }

    result.finishedAt = Date.now();

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "finalizeAmbassadorWithdrawal",
        message: "Ambassador withdrawal finalization finished",
        ambassadorSlug,
        ambassadorWallet,
        withdrawSessionId: requestedWithdrawSessionId,
        txid,
        scanned: result.scanned,
        finalized: result.finalized,
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
        job: "finalizeAmbassadorWithdrawal",
        ambassadorSlug,
        ambassadorWallet,
        withdrawSessionId: requestedWithdrawSessionId,
        txid,
        error: toErrorMessage(error)
      })
    );

    throw error;
  }
}
