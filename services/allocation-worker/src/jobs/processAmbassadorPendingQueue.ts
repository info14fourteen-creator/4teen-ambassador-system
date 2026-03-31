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
  action: "allocated" | "deferred" | "skipped" | "failed" | "stopped";
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
  stoppedEarly: boolean;
  stopReason: string | null;
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

function mapProcessedItem(attempt: any): ProcessAmbassadorPendingQueueJobItem {
  const purchase = attempt?.purchase ?? {};

  let action: "allocated" | "deferred" | "skipped" | "failed" | "stopped" = "failed";
  const status = String(attempt?.status || "").trim();

  if (status === "allocated") {
    action = "allocated";
  } else if (status === "deferred") {
    action = "deferred";
  } else if (status === "stopped-on-resource-shortage") {
    action = "stopped";
  } else if (
    status === "skipped-already-final" ||
    status === "skipped-no-ambassador-wallet"
  ) {
    action = "skipped";
  } else if (status === "retryable-failed" || status === "final-failed") {
    action = "failed";
  }

  return {
    purchaseId: String(purchase.purchaseId || "").trim(),
    txHash: String(purchase.txHash || "").trim(),
    buyerWallet: String(purchase.buyerWallet || "").trim(),
    ambassadorSlug: String(purchase.ambassadorSlug || "").trim(),
    ambassadorWallet: String(purchase.ambassadorWallet || "").trim(),
    purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
    ownerShareSun: String(purchase.ownerShareSun ?? "0"),
    status: String(purchase.status || status || "unknown"),
    action,
    reason: attempt?.reason ? String(attempt.reason) : null,
    txid: attempt?.txid ? String(attempt.txid) : null
  };
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

  const resolved = await resolveAmbassadorWallet(worker, {
    ambassadorSlug: options.ambassadorSlug,
    ambassadorWallet: options.ambassadorWallet
  });

  const ambassadorSlug = resolved.ambassadorSlug;
  const ambassadorWallet = resolved.ambassadorWallet;

  const result: ProcessAmbassadorPendingQueueJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    scanned: 0,
    allocated: 0,
    deferred: 0,
    skipped: 0,
    failed: 0,
    stoppedEarly: false,
    stopReason: null,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    const prepared = await worker.processor.prepareWithdrawBatch({
      ambassadorWallet: ambassadorWallet || "",
      limit
    });

    result.scanned = prepared.purchases.length;

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "processAmbassadorPendingQueue",
        message: "Loaded ambassador pending queue",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        limit,
        now
      })
    );

    if (!prepared.purchases.length) {
      result.finishedAt = Date.now();

      logger.info?.(
        JSON.stringify({
          ok: true,
          job: "processAmbassadorPendingQueue",
          message: "No pending purchases found",
          ambassadorSlug,
          ambassadorWallet,
          scanned: 0,
          durationMs: result.finishedAt - result.startedAt
        })
      );

      return result;
    }

    const allocationResult = await worker.processor.allocatePendingBatch({
      ambassadorWallet: ambassadorWallet || "",
      feeLimitSun,
      limit,
      allocationMode: "claim-first",
      stopOnFirstDeferred: true
    });

    result.stoppedEarly = Boolean(allocationResult.stoppedEarly);
    result.stopReason = allocationResult.stopReason ?? null;
    result.items = allocationResult.processed.map(mapProcessedItem);

    for (const item of result.items) {
      if (item.action === "allocated") {
        result.allocated += 1;
      } else if (item.action === "deferred" || item.action === "stopped") {
        result.deferred += 1;
      } else if (item.action === "skipped") {
        result.skipped += 1;
      } else {
        result.failed += 1;
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
        stoppedEarly: result.stoppedEarly,
        stopReason: result.stopReason,
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
