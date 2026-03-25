import type { AllocationWorker } from "../index";
import {
  processAmbassadorPendingQueue,
  type ProcessAmbassadorPendingQueueJobResult
} from "./processAmbassadorPendingQueue";
import {
  replayDeferredPurchases,
  type ReplayDeferredPurchasesJobResult
} from "./replayDeferredPurchases";

export interface DailyMaintenanceJobOptions {
  now?: number;
  replayLimit?: number;
  queueLimit?: number;
  feeLimitSun?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface DailyMaintenanceJobResult {
  ok: boolean;
  startedAt: number;
  finishedAt: number;
  replayDeferredPurchases: ReplayDeferredPurchasesJobResult | null;
  processAmbassadorPendingQueue: ProcessAmbassadorPendingQueueJobResult | null;
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

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

export async function dailyMaintenance(
  worker: AllocationWorker,
  options: DailyMaintenanceJobOptions = {}
): Promise<DailyMaintenanceJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const replayLimit = toPositiveInteger(options.replayLimit, 200);
  const queueLimit = toPositiveInteger(options.queueLimit, 200);
  const feeLimitSun = toOptionalPositiveInteger(options.feeLimitSun);

  const result: DailyMaintenanceJobResult = {
    ok: true,
    startedAt,
    finishedAt: startedAt,
    replayDeferredPurchases: null,
    processAmbassadorPendingQueue: null
  };

  logger.info?.(
    JSON.stringify({
      ok: true,
      job: "dailyMaintenance",
      stage: "started",
      startedAt,
      now,
      replayLimit,
      queueLimit,
      feeLimitSun: feeLimitSun ?? null
    })
  );

  try {
    result.replayDeferredPurchases = await replayDeferredPurchases(worker, {
      now,
      limit: replayLimit,
      feeLimitSun,
      logger
    });

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "dailyMaintenance",
        stage: "replayDeferredPurchases-finished",
        replay: {
          scanned: result.replayDeferredPurchases.scanned,
          allocated: result.replayDeferredPurchases.allocated,
          deferred: result.replayDeferredPurchases.deferred,
          skipped: result.replayDeferredPurchases.skipped,
          failed: result.replayDeferredPurchases.failed
        }
      })
    );

    result.processAmbassadorPendingQueue = await processAmbassadorPendingQueue(worker, {
      now,
      limit: queueLimit,
      feeLimitSun,
      logger
    });

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "dailyMaintenance",
        stage: "processAmbassadorPendingQueue-finished",
        queue: {
          scanned: result.processAmbassadorPendingQueue.scanned,
          allocated: result.processAmbassadorPendingQueue.allocated,
          deferred: result.processAmbassadorPendingQueue.deferred,
          skipped: result.processAmbassadorPendingQueue.skipped,
          failed: result.processAmbassadorPendingQueue.failed
        }
      })
    );

    result.finishedAt = Date.now();

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "dailyMaintenance",
        stage: "finished",
        startedAt: result.startedAt,
        finishedAt: result.finishedAt,
        durationMs: result.finishedAt - result.startedAt,
        replayDeferredPurchases: result.replayDeferredPurchases
          ? {
              scanned: result.replayDeferredPurchases.scanned,
              allocated: result.replayDeferredPurchases.allocated,
              deferred: result.replayDeferredPurchases.deferred,
              skipped: result.replayDeferredPurchases.skipped,
              failed: result.replayDeferredPurchases.failed
            }
          : null,
        processAmbassadorPendingQueue: result.processAmbassadorPendingQueue
          ? {
              scanned: result.processAmbassadorPendingQueue.scanned,
              allocated: result.processAmbassadorPendingQueue.allocated,
              deferred: result.processAmbassadorPendingQueue.deferred,
              skipped: result.processAmbassadorPendingQueue.skipped,
              failed: result.processAmbassadorPendingQueue.failed
            }
          : null
      })
    );

    return result;
  } catch (error) {
    result.ok = false;
    result.finishedAt = Date.now();

    logger.error?.(
      JSON.stringify({
        ok: false,
        job: "dailyMaintenance",
        stage: "failed",
        error: toErrorMessage(error),
        startedAt: result.startedAt,
        finishedAt: result.finishedAt,
        durationMs: result.finishedAt - result.startedAt,
        replayDeferredPurchases: result.replayDeferredPurchases
          ? {
              scanned: result.replayDeferredPurchases.scanned,
              allocated: result.replayDeferredPurchases.allocated,
              deferred: result.replayDeferredPurchases.deferred,
              skipped: result.replayDeferredPurchases.skipped,
              failed: result.replayDeferredPurchases.failed
            }
          : null,
        processAmbassadorPendingQueue: result.processAmbassadorPendingQueue
          ? {
              scanned: result.processAmbassadorPendingQueue.scanned,
              allocated: result.processAmbassadorPendingQueue.allocated,
              deferred: result.processAmbassadorPendingQueue.deferred,
              skipped: result.processAmbassadorPendingQueue.skipped,
              failed: result.processAmbassadorPendingQueue.failed
            }
          : null
      })
    );

    throw error;
  }
}
