import type { AllocationWorker } from "../index";

export interface ReplayDeferredPurchasesJobOptions {
  limit?: number;
  stopOnFirstFailure?: boolean;
  now?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface ReplayDeferredPurchaseJobItemResult {
  purchaseId: string;
  status: "allocated" | "skipped" | "failed";
  reason: string | null;
  txid: string | null;
}

export interface ReplayDeferredPurchasesJobResult {
  ok: boolean;
  scanned: number;
  attempted: number;
  allocated: number;
  skipped: number;
  failed: number;
  items: ReplayDeferredPurchaseJobItemResult[];
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

export async function replayDeferredPurchases(
  worker: AllocationWorker,
  options: ReplayDeferredPurchasesJobOptions = {}
): Promise<ReplayDeferredPurchasesJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 100);
  const stopOnFirstFailure = Boolean(options.stopOnFirstFailure);

  const failures = await worker.store.listReplayableFailures(limit);

  const result: ReplayDeferredPurchasesJobResult = {
    ok: true,
    scanned: failures.length,
    attempted: 0,
    allocated: 0,
    skipped: 0,
    failed: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  if (!failures.length) {
    result.finishedAt = Date.now();
    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "replayDeferredPurchases",
        message: "No deferred purchases found",
        scanned: 0
      })
    );
    return result;
  }

  logger.info?.(
    JSON.stringify({
      ok: true,
      job: "replayDeferredPurchases",
      message: "Starting replay of deferred purchases",
      scanned: failures.length,
      limit
    })
  );

  for (const purchase of failures) {
    result.attempted += 1;

    try {
      const replayResult = await worker.processor.replayFailedAllocation(
        purchase.purchaseId,
        undefined,
        now
      );

      if (replayResult.status === "allocated") {
        result.allocated += 1;
        result.items.push({
          purchaseId: purchase.purchaseId,
          status: "allocated",
          reason: replayResult.reason ?? null,
          txid: replayResult.txid ?? null
        });

        logger.info?.(
          JSON.stringify({
            ok: true,
            job: "replayDeferredPurchases",
            purchaseId: purchase.purchaseId,
            status: "allocated",
            txid: replayResult.txid ?? null
          })
        );

        continue;
      }

      if (
        replayResult.status === "skipped-resource-check-failed" ||
        replayResult.status === "not-ready" ||
        replayResult.status === "failed"
      ) {
        result.skipped += 1;
        result.items.push({
          purchaseId: purchase.purchaseId,
          status: "skipped",
          reason: replayResult.reason ?? "Replay skipped",
          txid: replayResult.txid ?? null
        });

        logger.warn?.(
          JSON.stringify({
            ok: true,
            job: "replayDeferredPurchases",
            purchaseId: purchase.purchaseId,
            status: "skipped",
            reason: replayResult.reason ?? "Replay skipped"
          })
        );

        continue;
      }

      result.skipped += 1;
      result.items.push({
        purchaseId: purchase.purchaseId,
        status: "skipped",
        reason: replayResult.reason ?? `Unexpected replay status: ${replayResult.status}`,
        txid: replayResult.txid ?? null
      });

      logger.warn?.(
        JSON.stringify({
          ok: true,
          job: "replayDeferredPurchases",
          purchaseId: purchase.purchaseId,
          status: "skipped",
          reason: replayResult.reason ?? `Unexpected replay status: ${replayResult.status}`
        })
      );
    } catch (error) {
      const message = toErrorMessage(error);

      result.ok = false;
      result.failed += 1;
      result.items.push({
        purchaseId: purchase.purchaseId,
        status: "failed",
        reason: message,
        txid: null
      });

      logger.error?.(
        JSON.stringify({
          ok: false,
          job: "replayDeferredPurchases",
          purchaseId: purchase.purchaseId,
          status: "failed",
          error: message
        })
      );

      if (stopOnFirstFailure) {
        break;
      }
    }
  }

  result.finishedAt = Date.now();

  logger.info?.(
    JSON.stringify({
      ok: result.ok,
      job: "replayDeferredPurchases",
      message: "Replay finished",
      scanned: result.scanned,
      attempted: result.attempted,
      allocated: result.allocated,
      skipped: result.skipped,
      failed: result.failed,
      durationMs: result.finishedAt - result.startedAt
    })
  );

  return result;
}
