import crypto from "node:crypto";
import type { AllocationWorker } from "../index";
import type { PurchaseRecord, PurchaseProcessingStatus } from "../db/purchases";

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
  ambassadorRewardSun: string;
  previousStatus: string;
  nextStatus: "withdraw_included" | "skipped";
  withdrawSessionId: string | null;
  reason: string | null;
}

export interface PrepareAmbassadorWithdrawalJobResult {
  ok: boolean;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  withdrawSessionId: string | null;
  scanned: number;
  included: number;
  skipped: number;
  totalRewardSun: string;
  startedAt: number;
  finishedAt: number;
  items: PrepareAmbassadorWithdrawalJobItem[];
}

const ELIGIBLE_STATUSES: PurchaseProcessingStatus[] = ["allocated"];

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
}

function assertNonEmpty(value: unknown, fieldName: string): string {
  const normalized = normalizeOptionalString(value);

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
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

function sumSun(left: string, right: string): string {
  return (BigInt(left || "0") + BigInt(right || "0")).toString();
}

function buildWithdrawSessionId(input: {
  ambassadorWallet: string;
  now: number;
}): string {
  return crypto
    .createHash("sha256")
    .update(`withdraw:${input.ambassadorWallet}:${input.now}:${Math.random()}`)
    .digest("hex")
    .slice(0, 32);
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

function isPositiveSun(value: string | null | undefined): boolean {
  try {
    return BigInt(String(value || "0")) > 0n;
  } catch {
    return false;
  }
}

function mapSkippedItem(purchase: PurchaseRecord, reason: string): PrepareAmbassadorWithdrawalJobItem {
  return {
    purchaseId: purchase.purchaseId,
    txHash: purchase.txHash,
    buyerWallet: purchase.buyerWallet,
    ambassadorSlug: String(purchase.ambassadorSlug || "").trim(),
    ambassadorWallet: String(purchase.ambassadorWallet || "").trim(),
    purchaseAmountSun: purchase.purchaseAmountSun,
    ownerShareSun: purchase.ownerShareSun,
    ambassadorRewardSun: purchase.ambassadorRewardSun,
    previousStatus: purchase.status,
    nextStatus: "skipped",
    withdrawSessionId: purchase.withdrawSessionId,
    reason
  };
}

function mapIncludedItem(
  purchase: PurchaseRecord,
  withdrawSessionId: string
): PrepareAmbassadorWithdrawalJobItem {
  return {
    purchaseId: purchase.purchaseId,
    txHash: purchase.txHash,
    buyerWallet: purchase.buyerWallet,
    ambassadorSlug: String(purchase.ambassadorSlug || "").trim(),
    ambassadorWallet: String(purchase.ambassadorWallet || "").trim(),
    purchaseAmountSun: purchase.purchaseAmountSun,
    ownerShareSun: purchase.ownerShareSun,
    ambassadorRewardSun: purchase.ambassadorRewardSun,
    previousStatus: "allocated",
    nextStatus: "withdraw_included",
    withdrawSessionId,
    reason: null
  };
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
    included: 0,
    skipped: 0,
    totalRewardSun: "0",
    startedAt,
    finishedAt: startedAt,
    items: []
  };

  try {
    const candidates = await worker.store.listPendingByAmbassador({
      ambassadorWallet: ambassadorWallet || "",
      statuses: ELIGIBLE_STATUSES,
      limit
    });

    result.scanned = candidates.length;

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "prepareAmbassadorWithdrawal",
        stage: "loaded",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        limit,
        now
      })
    );

    if (!candidates.length) {
      result.finishedAt = Date.now();

      logger.info?.(
        JSON.stringify({
          ok: true,
          job: "prepareAmbassadorWithdrawal",
          stage: "finished-empty",
          ambassadorSlug,
          ambassadorWallet,
          scanned: 0,
          durationMs: result.finishedAt - result.startedAt
        })
      );

      return result;
    }

    const eligible = candidates.filter((purchase) => {
      if (purchase.status !== "allocated") {
        return false;
      }

      if (purchase.withdrawSessionId) {
        return false;
      }

      if (!isPositiveSun(purchase.ambassadorRewardSun)) {
        return false;
      }

      return true;
    });

    if (!eligible.length) {
      result.items = candidates.map((purchase) => {
        if (purchase.withdrawSessionId) {
          return mapSkippedItem(purchase, "Purchase is already attached to a withdrawal session");
        }

        if (!isPositiveSun(purchase.ambassadorRewardSun)) {
          return mapSkippedItem(purchase, "Purchase has zero ambassador reward");
        }

        return mapSkippedItem(purchase, `Purchase is not eligible from status: ${purchase.status}`);
      });

      result.skipped = result.items.length;
      result.finishedAt = Date.now();

      logger.info?.(
        JSON.stringify({
          ok: true,
          job: "prepareAmbassadorWithdrawal",
          stage: "finished-no-eligible-items",
          ambassadorSlug,
          ambassadorWallet,
          scanned: result.scanned,
          skipped: result.skipped,
          durationMs: result.finishedAt - result.startedAt
        })
      );

      return result;
    }

    const withdrawSessionId = buildWithdrawSessionId({
      ambassadorWallet: ambassadorWallet || "",
      now
    });

    result.withdrawSessionId = withdrawSessionId;

    for (const purchase of candidates) {
      if (purchase.status !== "allocated") {
        result.items.push(
          mapSkippedItem(purchase, `Purchase is not eligible from status: ${purchase.status}`)
        );
        result.skipped += 1;
        continue;
      }

      if (purchase.withdrawSessionId) {
        result.items.push(
          mapSkippedItem(purchase, "Purchase is already attached to a withdrawal session")
        );
        result.skipped += 1;
        continue;
      }

      if (!isPositiveSun(purchase.ambassadorRewardSun)) {
        result.items.push(
          mapSkippedItem(purchase, "Purchase has zero ambassador reward")
        );
        result.skipped += 1;
        continue;
      }

      const updated = await worker.store.markWithdrawIncluded(purchase.purchaseId, {
        withdrawSessionId,
        now
      });

      result.items.push(mapIncludedItem(updated, withdrawSessionId));
      result.included += 1;
      result.totalRewardSun = sumSun(result.totalRewardSun, updated.ambassadorRewardSun);
    }

    result.finishedAt = Date.now();

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "prepareAmbassadorWithdrawal",
        stage: "finished",
        ambassadorSlug,
        ambassadorWallet,
        withdrawSessionId: result.withdrawSessionId,
        scanned: result.scanned,
        included: result.included,
        skipped: result.skipped,
        totalRewardSun: result.totalRewardSun,
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
        stage: "failed",
        ambassadorSlug,
        ambassadorWallet,
        withdrawSessionId: result.withdrawSessionId,
        error: toErrorMessage(error),
        durationMs: result.finishedAt - result.startedAt
      })
    );

    throw error;
  }
}
