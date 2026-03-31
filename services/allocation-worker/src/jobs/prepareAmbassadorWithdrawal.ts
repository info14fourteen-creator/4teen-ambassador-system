import crypto from "node:crypto";
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
  withdrawSessionId: string | null;
  reason: string | null;
}

export interface PrepareAmbassadorWithdrawalJobResult {
  ok: boolean;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  withdrawSessionId: string | null;
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

function buildWithdrawSessionId(input: {
  ambassadorWallet: string;
  now: number;
}): string {
  const hash = crypto
    .createHash("sha256")
    .update(`withdraw:${input.ambassadorWallet}:${input.now}:${Math.random()}`)
    .digest("hex");

  return hash.slice(0, 32);
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

async function loadCandidatePurchases(
  worker: AllocationWorker,
  options: {
    ambassadorWallet: string;
    limit: number;
  }
): Promise<any[]> {
  const pending = await worker.store.listPendingByAmbassador({
    ambassadorWallet: options.ambassadorWallet,
    statuses: ["allocated"],
    limit: options.limit
  });

  return pending.filter((purchase) => {
    const reward = BigInt(String(purchase.ownerShareSun || "0"));
    return (
      purchase.status === "allocated" &&
      !purchase.withdrawSessionId &&
      reward > 0n
    );
  });
}

async function markPreparedForWithdrawal(
  worker: AllocationWorker,
  purchaseId: string,
  withdrawSessionId: string,
  now: number
): Promise<any> {
  await worker.store.assignWithdrawSession(purchaseId, withdrawSessionId, now);

  if (typeof (worker.store as any).markWithdrawIncluded === "function") {
    return (worker.store as any).markWithdrawIncluded(purchaseId, {
      withdrawSessionId,
      now
    });
  }

  return worker.store.getByPurchaseId(purchaseId);
}

export async function prepareAmbassadorWithdrawal(
  worker: AllocationWorker,
  options: PrepareAmbassadorWithdrawalJobOptions = {}
): Promise<PrepareAmbassadorWithdrawalJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 500);

  const resolved = await resolveAmbassadorWallet(worker, {
    ambassadorSlug: options.ambassadorSlug,
    ambassadorWallet: options.ambassadorWallet
  });

  const ambassadorSlug = resolved.ambassadorSlug;
  const ambassadorWallet = resolved.ambassadorWallet;

  const result: PrepareAmbassadorWithdrawalJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    withdrawSessionId: null,
    scanned: 0,
    prepared: 0,
    skipped: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    if (!ambassadorWallet) {
      throw new Error("Ambassador wallet is required");
    }

    const purchases = await loadCandidatePurchases(worker, {
      ambassadorWallet,
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

    if (!purchases.length) {
      result.finishedAt = Date.now();

      logger.info?.(
        JSON.stringify({
          ok: true,
          job: "prepareAmbassadorWithdrawal",
          message: "No allocated purchases found for withdrawal preparation",
          ambassadorSlug,
          ambassadorWallet,
          scanned: 0,
          durationMs: result.finishedAt - result.startedAt
        })
      );

      return result;
    }

    const withdrawSessionId = buildWithdrawSessionId({
      ambassadorWallet,
      now
    });

    result.withdrawSessionId = withdrawSessionId;

    for (const purchase of purchases) {
      const currentStatus = String(purchase.status || "").trim();
      const rewardAmount = BigInt(String(purchase.ownerShareSun ?? "0"));

      if (currentStatus !== "allocated") {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus || "unknown",
          queuedForWithdrawal: false,
          withdrawSessionId: null,
          reason: `Unsupported status for withdrawal preparation: ${currentStatus || "unknown"}`
        });
        continue;
      }

      if (purchase.withdrawSessionId) {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus,
          queuedForWithdrawal: true,
          withdrawSessionId: String(purchase.withdrawSessionId),
          reason: "Already included in withdrawal preparation"
        });
        continue;
      }

      if (rewardAmount <= 0n) {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus,
          queuedForWithdrawal: false,
          withdrawSessionId: null,
          reason: "Reward amount is zero"
        });
        continue;
      }

      const updated = await markPreparedForWithdrawal(
        worker,
        String(purchase.purchaseId),
        withdrawSessionId,
        now
      );

      result.prepared += 1;
      result.items.push({
        purchaseId: String(updated?.purchaseId || purchase.purchaseId || ""),
        txHash: String(updated?.txHash || purchase.txHash || ""),
        buyerWallet: String(updated?.buyerWallet || purchase.buyerWallet || ""),
        ambassadorSlug: String(updated?.ambassadorSlug || purchase.ambassadorSlug || ""),
        ambassadorWallet: String(updated?.ambassadorWallet || purchase.ambassadorWallet || ""),
        purchaseAmountSun: String(updated?.purchaseAmountSun ?? purchase.purchaseAmountSun ?? "0"),
        ownerShareSun: String(updated?.ownerShareSun ?? purchase.ownerShareSun ?? "0"),
        status: String(updated?.status || "withdraw_included"),
        queuedForWithdrawal: true,
        withdrawSessionId,
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
        withdrawSessionId,
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
