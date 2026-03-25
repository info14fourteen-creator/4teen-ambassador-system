# REPOSITORY: 4teen-ambassador-system
# SECTION: WORKER TRON AND JOBS
# GENERATED_AT: 2026-03-25T17:11:08.872Z

## INCLUDED FILES

- services/allocation-worker/src/jobs/allocatePurchase.ts
- services/allocation-worker/src/jobs/dailyMaintenance.ts
- services/allocation-worker/src/jobs/prepareAmbassadorWithdrawal.ts
- services/allocation-worker/src/jobs/processAmbassadorPendingQueue.ts
- services/allocation-worker/src/jobs/rentEnergy.ts
- services/allocation-worker/src/jobs/replayDeferredPurchases.ts
- services/allocation-worker/src/tron/controller.ts
- services/allocation-worker/src/tron/hashing.ts
- services/allocation-worker/src/tron/resources.ts

## REPOSITORY LINK BASE

- https://raw.githubusercontent.com/info14fourteen-creator/4teen-ambassador-system/main/ai/latest/4teen-ambassador-system

---

## FILE: services/allocation-worker/src/jobs/allocatePurchase.ts

```ts
import {
  AttributionProcessor,
  ProcessVerifiedPurchaseAndAllocateInput,
  ProcessVerifiedPurchaseAndAllocateResult
} from "../app/processAttribution";

export interface AllocatePurchaseJobConfig {
  processor: AttributionProcessor;
}

export interface AllocatePurchaseJobInput {
  txHash: string;
  buyerWallet: string;
  slug: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  feeLimitSun?: number;
  now?: number;
}

export interface AllocatePurchaseJobResult {
  ok: boolean;
  result: ProcessVerifiedPurchaseAndAllocateResult;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeSunAmount(value: string | number | bigint, fieldName: string): string {
  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${fieldName} must be a non-negative integer string`);
  }

  return normalized;
}

export class AllocatePurchaseJob {
  private readonly processor: AttributionProcessor;

  constructor(config: AllocatePurchaseJobConfig) {
    if (!config?.processor) {
      throw new Error("processor is required");
    }

    this.processor = config.processor;
  }

  async run(input: AllocatePurchaseJobInput): Promise<AllocatePurchaseJobResult> {
    const payload: ProcessVerifiedPurchaseAndAllocateInput = {
      txHash: assertNonEmpty(input.txHash, "txHash"),
      buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
      slug: assertNonEmpty(input.slug, "slug"),
      purchaseAmountSun: normalizeSunAmount(input.purchaseAmountSun, "purchaseAmountSun"),
      ownerShareSun: normalizeSunAmount(input.ownerShareSun, "ownerShareSun"),
      feeLimitSun: input.feeLimitSun,
      now: input.now
    };

    const result = await this.processor.processVerifiedPurchaseAndAllocate(payload);

    const ok =
      result.verification.canAllocate &&
      result.allocation !== null &&
      result.allocation.status === "allocated";

    return {
      ok,
      result
    };
  }

  async replayFailed(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ) {
    return this.processor.replayFailedAllocation(
      assertNonEmpty(purchaseId, "purchaseId"),
      feeLimitSun,
      now
    );
  }
}
```

---

## FILE: services/allocation-worker/src/jobs/dailyMaintenance.ts

```ts
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
```

---

## FILE: services/allocation-worker/src/jobs/prepareAmbassadorWithdrawal.ts

```ts
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
```

---

## FILE: services/allocation-worker/src/jobs/processAmbassadorPendingQueue.ts

```ts
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
```

---

## FILE: services/allocation-worker/src/jobs/rentEnergy.ts

```ts
import { createGasStationClientFromEnv } from "../services/gasStation";

function assertNonEmpty(value: string | undefined, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function getTargetEnergy(): number {
  const raw = Number(process.env.GASSTATION_TARGET_ENERGY || "220000");
  return Number.isFinite(raw) && raw >= 64400 ? Math.ceil(raw) : 220000;
}

function getServiceChargeType(): string {
  return String(process.env.GASSTATION_SERVICE_CHARGE_TYPE || "10010").trim() || "10010";
}

export interface RentEnergyJobResult {
  ok: boolean;
  stage: "checked-balance" | "order-created" | "skipped";
  gasBalance: string | null;
  tradeNo: string | null;
  reason: string | null;
}

export async function rentDailyEnergy(): Promise<RentEnergyJobResult> {
  const client = createGasStationClientFromEnv();

  const receiveAddress = assertNonEmpty(
    process.env.TRON_RESOURCE_ADDRESS || process.env.CONTROLLER_OWNER_WALLET,
    "TRON_RESOURCE_ADDRESS"
  );

  const balance = await client.getBalance();
  const gasBalance = balance.balance;

  const targetEnergy = getTargetEnergy();
  const serviceChargeType = getServiceChargeType();

  if (Number(gasBalance) <= 0) {
    return {
      ok: false,
      stage: "checked-balance",
      gasBalance,
      tradeNo: null,
      reason: "GasStation balance is empty"
    };
  }

  const requestId = `energy-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

  const order = await client.createEnergyOrder({
    requestId,
    receiveAddress,
    energyNum: targetEnergy,
    serviceChargeType
  });

  return {
    ok: true,
    stage: "order-created",
    gasBalance,
    tradeNo: order.trade_no,
    reason: null
  };
}
```

---

## FILE: services/allocation-worker/src/jobs/replayDeferredPurchases.ts

```ts
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
```

---

## FILE: services/allocation-worker/src/tron/controller.ts

```ts
import { FOURTEEN_CONTROLLER_CONTRACT } from "../../../../shared/config/contracts";

export interface ControllerClientConfig {
  tronWeb: any;
  contractAddress?: string;
}

export interface ResolveAmbassadorBySlugHashResult {
  slugHash: string;
  ambassadorWallet: string | null;
}

export interface RecordVerifiedPurchaseInput {
  purchaseId: string;
  buyerWallet: string;
  ambassadorWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  feeLimitSun?: number;
}

export interface RecordVerifiedPurchaseResult {
  txid: string;
}

export interface ControllerClient {
  getAmbassadorBySlugHash(slugHash: string): Promise<ResolveAmbassadorBySlugHashResult>;
  getBuyerAmbassador(buyerWallet: string): Promise<string | null>;
  isPurchaseProcessed(purchaseId: string): Promise<boolean>;
  canBindBuyerToAmbassador(buyerWallet: string, ambassadorWallet: string): Promise<boolean>;
  recordVerifiedPurchase(input: RecordVerifiedPurchaseInput): Promise<RecordVerifiedPurchaseResult>;
}

const TRON_HEX_ZERO_ADDRESS = "410000000000000000000000000000000000000000";

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeSunAmount(value: string | number | bigint, fieldName: string): string {
  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${fieldName} must be a non-negative integer string`);
  }

  return normalized;
}

function normalizeBytes32Hex(value: string, fieldName: string): string {
  const normalized = assertNonEmpty(value, fieldName).toLowerCase();

  if (!/^0x[0-9a-f]{64}$/.test(normalized)) {
    throw new Error(`${fieldName} must be a 32-byte hex string`);
  }

  return normalized;
}

function normalizeFeeLimitSun(value: number | undefined): number {
  const resolved = value ?? 300_000_000;

  if (!Number.isInteger(resolved) || resolved <= 0) {
    throw new Error("feeLimitSun must be a positive integer");
  }

  return resolved;
}

function isHexAddress(value: string): boolean {
  return /^41[0-9a-fA-F]{40}$/.test(value);
}

function isBase58Address(value: string): boolean {
  return /^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(value);
}

function normalizeAddress(value: string, fieldName: string): string {
  const normalized = assertNonEmpty(value, fieldName);

  if (!isBase58Address(normalized) && !isHexAddress(normalized)) {
    throw new Error(`${fieldName} must be a valid TRON address`);
  }

  return normalized;
}

function isZeroHexAddress(value: string): boolean {
  return value.toLowerCase() === TRON_HEX_ZERO_ADDRESS.toLowerCase();
}

function normalizeReturnedAddress(tronWeb: any, value: unknown): string | null {
  const raw = String(value || "").trim();

  if (!raw) {
    return null;
  }

  if (isHexAddress(raw)) {
    if (isZeroHexAddress(raw)) {
      return null;
    }

    if (tronWeb?.address?.fromHex) {
      return tronWeb.address.fromHex(raw);
    }

    return raw;
  }

  if (isBase58Address(raw)) {
    return raw;
  }

  return raw || null;
}

async function getContract(tronWeb: any, contractAddress: string): Promise<any> {
  if (!tronWeb || typeof tronWeb.contract !== "function") {
    throw new Error("Valid tronWeb instance is required");
  }

  return await tronWeb.contract().at(contractAddress);
}

export class TronControllerClient implements ControllerClient {
  private readonly tronWeb: any;
  private readonly contractAddress: string;
  private contractInstance: any | null = null;

  constructor(config: ControllerClientConfig) {
    if (!config?.tronWeb) {
      throw new Error("tronWeb is required");
    }

    this.tronWeb = config.tronWeb;
    this.contractAddress = normalizeAddress(
      config.contractAddress ?? FOURTEEN_CONTROLLER_CONTRACT,
      "contractAddress"
    );
  }

  private async contract(): Promise<any> {
    if (!this.contractInstance) {
      this.contractInstance = await getContract(this.tronWeb, this.contractAddress);
    }

    return this.contractInstance;
  }

  async getAmbassadorBySlugHash(slugHash: string): Promise<ResolveAmbassadorBySlugHashResult> {
    const normalizedSlugHash = normalizeBytes32Hex(slugHash, "slugHash");
    const contract = await this.contract();

    const result = await contract.getAmbassadorBySlugHash(normalizedSlugHash).call();
    const ambassadorWallet = normalizeReturnedAddress(this.tronWeb, result);

    return {
      slugHash: normalizedSlugHash,
      ambassadorWallet
    };
  }

  async getBuyerAmbassador(buyerWallet: string): Promise<string | null> {
    const normalizedBuyerWallet = normalizeAddress(buyerWallet, "buyerWallet");
    const contract = await this.contract();

    const result = await contract.getBuyerAmbassador(normalizedBuyerWallet).call();
    return normalizeReturnedAddress(this.tronWeb, result);
  }

  async isPurchaseProcessed(purchaseId: string): Promise<boolean> {
    const normalizedPurchaseId = normalizeBytes32Hex(purchaseId, "purchaseId");
    const contract = await this.contract();

    const result = await contract.isPurchaseProcessed(normalizedPurchaseId).call();
    return Boolean(result);
  }

  async canBindBuyerToAmbassador(
    buyerWallet: string,
    ambassadorWallet: string
  ): Promise<boolean> {
    const normalizedBuyerWallet = normalizeAddress(buyerWallet, "buyerWallet");
    const normalizedAmbassadorWallet = normalizeAddress(ambassadorWallet, "ambassadorWallet");
    const contract = await this.contract();

    const result = await contract
      .canBindBuyerToAmbassador(normalizedBuyerWallet, normalizedAmbassadorWallet)
      .call();

    return Boolean(result);
  }

  async recordVerifiedPurchase(
    input: RecordVerifiedPurchaseInput
  ): Promise<RecordVerifiedPurchaseResult> {
    const purchaseId = normalizeBytes32Hex(input.purchaseId, "purchaseId");
    const buyerWallet = normalizeAddress(input.buyerWallet, "buyerWallet");
    const ambassadorWallet = normalizeAddress(input.ambassadorWallet, "ambassadorWallet");
    const purchaseAmountSun = normalizeSunAmount(input.purchaseAmountSun, "purchaseAmountSun");
    const ownerShareSun = normalizeSunAmount(input.ownerShareSun, "ownerShareSun");
    const feeLimitSun = normalizeFeeLimitSun(input.feeLimitSun);

    const contract = await this.contract();

    const txid = await contract
      .recordVerifiedPurchase(
        purchaseId,
        buyerWallet,
        ambassadorWallet,
        purchaseAmountSun,
        ownerShareSun
      )
      .send({
        feeLimit: feeLimitSun
      });

    return {
      txid: assertNonEmpty(txid, "txid")
    };
  }
}
```

---

## FILE: services/allocation-worker/src/tron/hashing.ts

```ts
import { keccak_256 } from "@noble/hashes/sha3";
import { utf8ToBytes } from "@noble/hashes/utils";

export interface PurchaseIdInput {
  txHash: string;
  buyerWallet: string;
}

export interface AttributionHashing {
  hashSlugToBytes32Hex(slug: string): string;
  derivePurchaseId(input: PurchaseIdInput): string;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function stripHexPrefix(value: string): string {
  return value.startsWith("0x") ? value.slice(2) : value;
}

function isHex(value: string): boolean {
  return /^[0-9a-fA-F]+$/.test(value);
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function toBytes32HexFromUtf8(value: string): string {
  const bytes = utf8ToBytes(value);
  const hash = keccak_256(bytes);
  return `0x${bytesToHex(hash)}`;
}

function normalizeSlugForHashing(slug: string): string {
  return assertNonEmpty(slug, "slug").trim().toLowerCase();
}

function normalizeTxHash(txHash: string): string {
  const normalized = assertNonEmpty(txHash, "txHash").trim().toLowerCase();
  const stripped = stripHexPrefix(normalized);

  if (!isHex(stripped)) {
    throw new Error("txHash must be a hex string");
  }

  return stripped;
}

function normalizeWalletForPurchaseId(wallet: string): string {
  return assertNonEmpty(wallet, "buyerWallet").trim();
}

export class TronHashing implements AttributionHashing {
  hashSlugToBytes32Hex(slug: string): string {
    const normalizedSlug = normalizeSlugForHashing(slug);
    return toBytes32HexFromUtf8(normalizedSlug);
  }

  derivePurchaseId(input: PurchaseIdInput): string {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWalletForPurchaseId(input.buyerWallet);

    const combined = `${txHash}:${buyerWallet}`;
    return toBytes32HexFromUtf8(combined);
  }
}
```

---

## FILE: services/allocation-worker/src/tron/resources.ts

```ts
export interface AccountResourceSnapshot {
  address: string;
  bandwidth: {
    freeNetLimit: number;
    freeNetUsed: number;
    netLimit: number;
    netUsed: number;
    totalLimit: number;
    totalUsed: number;
    available: number;
  };
  energy: {
    energyLimit: number;
    energyUsed: number;
    available: number;
  };
  latestOperationTime?: number;
  raw: {
    account: any;
    resources: any;
  };
}

export interface AllocationResourcePolicy {
  minEnergyRequired: number;
  minBandwidthRequired: number;
  safetyEnergyBuffer: number;
  safetyBandwidthBuffer: number;
}

export interface AllocationResourceCheckResult {
  ok: boolean;
  address: string;
  availableEnergy: number;
  availableBandwidth: number;
  requiredEnergy: number;
  requiredBandwidth: number;
  shortEnergy: number;
  shortBandwidth: number;
  reason: string | null;
  snapshot: AccountResourceSnapshot;
}

export interface ResourceGateway {
  getAccountResourceSnapshot(address: string): Promise<AccountResourceSnapshot>;
  checkAllocationReadiness(
    address: string,
    policy: AllocationResourcePolicy
  ): Promise<AllocationResourceCheckResult>;
}

function toSafeNumber(value: unknown): number {
  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }

  return parsed;
}

function toAddressHex(address: string): string {
  return String(address || "").trim();
}

function normalizeAddressFromTronWeb(tronWeb: any, address: string): string {
  const raw = String(address || "").trim();

  if (!raw) {
    throw new Error("address is required");
  }

  if (typeof tronWeb?.address?.fromHex === "function" && raw.startsWith("41")) {
    try {
      return tronWeb.address.fromHex(raw);
    } catch {
      return raw;
    }
  }

  return raw;
}

function sumBandwidth(resources: any, account: any): {
  freeNetLimit: number;
  freeNetUsed: number;
  netLimit: number;
  netUsed: number;
  totalLimit: number;
  totalUsed: number;
  available: number;
} {
  const freeNetLimit = toSafeNumber(account?.free_net_limit);
  const freeNetUsed = toSafeNumber(account?.free_net_used);

  const netLimit =
    toSafeNumber(account?.net_limit) ||
    toSafeNumber(resources?.NetLimit) ||
    toSafeNumber(resources?.netLimit);

  const netUsed =
    toSafeNumber(account?.net_used) ||
    toSafeNumber(resources?.NetUsed) ||
    toSafeNumber(resources?.netUsed);

  const totalLimit = freeNetLimit + netLimit;
  const totalUsed = freeNetUsed + netUsed;
  const available = Math.max(totalLimit - totalUsed, 0);

  return {
    freeNetLimit,
    freeNetUsed,
    netLimit,
    netUsed,
    totalLimit,
    totalUsed,
    available
  };
}

function sumEnergy(resources: any): {
  energyLimit: number;
  energyUsed: number;
  available: number;
} {
  const energyLimit =
    toSafeNumber(resources?.EnergyLimit) ||
    toSafeNumber(resources?.energyLimit);

  const energyUsed =
    toSafeNumber(resources?.EnergyUsed) ||
    toSafeNumber(resources?.energyUsed);

  const available = Math.max(energyLimit - energyUsed, 0);

  return {
    energyLimit,
    energyUsed,
    available
  };
}

export function buildDefaultAllocationResourcePolicy(
  overrides?: Partial<AllocationResourcePolicy>
): AllocationResourcePolicy {
  return {
    minEnergyRequired: overrides?.minEnergyRequired ?? 180_000,
    minBandwidthRequired: overrides?.minBandwidthRequired ?? 1_000,
    safetyEnergyBuffer: overrides?.safetyEnergyBuffer ?? 20_000,
    safetyBandwidthBuffer: overrides?.safetyBandwidthBuffer ?? 300
  };
}

export function createResourceGateway(tronWeb: any): ResourceGateway {
  if (!tronWeb) {
    throw new Error("tronWeb is required");
  }

  async function getAccountResourceSnapshot(address: string): Promise<AccountResourceSnapshot> {
    const normalizedAddress = normalizeAddressFromTronWeb(tronWeb, address);
    const accountAddress =
      typeof tronWeb?.address?.toHex === "function"
        ? tronWeb.address.toHex(normalizedAddress)
        : toAddressHex(normalizedAddress);

    const [account, resources] = await Promise.all([
      tronWeb.trx.getAccount(accountAddress),
      tronWeb.trx.getAccountResources(accountAddress)
    ]);

    const bandwidth = sumBandwidth(resources, account);
    const energy = sumEnergy(resources);

    return {
      address: normalizedAddress,
      bandwidth,
      energy,
      latestOperationTime: toSafeNumber(account?.latest_opration_time) || undefined,
      raw: {
        account,
        resources
      }
    };
  }

  async function checkAllocationReadiness(
    address: string,
    policy: AllocationResourcePolicy
  ): Promise<AllocationResourceCheckResult> {
    const snapshot = await getAccountResourceSnapshot(address);

    const requiredEnergy = Math.max(
      toSafeNumber(policy.minEnergyRequired) + toSafeNumber(policy.safetyEnergyBuffer),
      0
    );

    const requiredBandwidth = Math.max(
      toSafeNumber(policy.minBandwidthRequired) + toSafeNumber(policy.safetyBandwidthBuffer),
      0
    );

    const availableEnergy = snapshot.energy.available;
    const availableBandwidth = snapshot.bandwidth.available;

    const shortEnergy = Math.max(requiredEnergy - availableEnergy, 0);
    const shortBandwidth = Math.max(requiredBandwidth - availableBandwidth, 0);

    let reason: string | null = null;

    if (shortEnergy > 0 && shortBandwidth > 0) {
      reason = `Insufficient energy and bandwidth. Need +${shortEnergy} energy and +${shortBandwidth} bandwidth.`;
    } else if (shortEnergy > 0) {
      reason = `Insufficient energy. Need +${shortEnergy} energy.`;
    } else if (shortBandwidth > 0) {
      reason = `Insufficient bandwidth. Need +${shortBandwidth} bandwidth.`;
    }

    return {
      ok: !reason,
      address: snapshot.address,
      availableEnergy,
      availableBandwidth,
      requiredEnergy,
      requiredBandwidth,
      shortEnergy,
      shortBandwidth,
      reason,
      snapshot
    };
  }

  return {
    getAccountResourceSnapshot,
    checkAllocationReadiness
  };
}

export interface GasStationBalanceSnapshot {
  ok: boolean;
  availableEnergy?: number;
  availableBandwidth?: number;
  raw: unknown;
}

export interface GasStationClient {
  getBalance(): Promise<GasStationBalanceSnapshot>;
}

export interface GasStationClientConfig {
  endpoint: string;
  apiKey?: string;
  projectId?: string;
  timeoutMs?: number;
  staticIpProxyUrl?: string;
}

function buildGasStationHeaders(config: GasStationClientConfig): Record<string, string> {
  const headers: Record<string, string> = {
    Accept: "application/json"
  };

  if (config.apiKey) {
    headers.Authorization = `Bearer ${config.apiKey}`;
  }

  if (config.projectId) {
    headers["X-Project-Id"] = config.projectId;
  }

  return headers;
}

export function createGasStationClient(config: GasStationClientConfig): GasStationClient {
  if (!config?.endpoint?.trim()) {
    throw new Error("Gas Station endpoint is required");
  }

  const endpoint = config.endpoint.trim();
  const timeoutMs = Math.max(toSafeNumber(config.timeoutMs) || 10_000, 1_000);

  async function getBalance(): Promise<GasStationBalanceSnapshot> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(endpoint, {
        method: "GET",
        headers: buildGasStationHeaders(config),
        signal: controller.signal
      });

      const text = await response.text();

      let parsed: any = null;
      try {
        parsed = text ? JSON.parse(text) : null;
      } catch {
        parsed = { rawText: text };
      }

      if (!response.ok) {
        return {
          ok: false,
          raw: parsed
        };
      }

      const availableEnergy =
        toSafeNumber(parsed?.availableEnergy) ||
        toSafeNumber(parsed?.energy) ||
        toSafeNumber(parsed?.data?.availableEnergy) ||
        toSafeNumber(parsed?.data?.energy) ||
        undefined;

      const availableBandwidth =
        toSafeNumber(parsed?.availableBandwidth) ||
        toSafeNumber(parsed?.bandwidth) ||
        toSafeNumber(parsed?.data?.availableBandwidth) ||
        toSafeNumber(parsed?.data?.bandwidth) ||
        undefined;

      return {
        ok: true,
        availableEnergy,
        availableBandwidth,
        raw: parsed
      };
    } catch (error) {
      return {
        ok: false,
        raw: {
          message:
            error instanceof Error ? error.message : typeof error === "string" ? error : "Unknown error"
        }
      };
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    getBalance
  };
}

export interface EffectiveAllocationResourceDecision {
  ok: boolean;
  reason: string | null;
  wallet: AllocationResourceCheckResult;
  gasStation?: {
    balance: GasStationBalanceSnapshot;
    energySatisfied: boolean;
    bandwidthSatisfied: boolean;
  };
}

export async function evaluateEffectiveAllocationReadiness(params: {
  gateway: ResourceGateway;
  address: string;
  policy: AllocationResourcePolicy;
  gasStationClient?: GasStationClient;
  requireGasStationReserve?: boolean;
}): Promise<EffectiveAllocationResourceDecision> {
  const wallet = await params.gateway.checkAllocationReadiness(params.address, params.policy);

  if (!params.gasStationClient || !params.requireGasStationReserve) {
    return {
      ok: wallet.ok,
      reason: wallet.reason,
      wallet
    };
  }

  const balance = await params.gasStationClient.getBalance();

  const requiredEnergy = wallet.requiredEnergy;
  const requiredBandwidth = wallet.requiredBandwidth;

  const gasEnergy = toSafeNumber(balance.availableEnergy);
  const gasBandwidth = toSafeNumber(balance.availableBandwidth);

  const energySatisfied = gasEnergy >= requiredEnergy;
  const bandwidthSatisfied = gasBandwidth >= requiredBandwidth;

  if (wallet.ok && energySatisfied && bandwidthSatisfied) {
    return {
      ok: true,
      reason: null,
      wallet,
      gasStation: {
        balance,
        energySatisfied,
        bandwidthSatisfied
      }
    };
  }

  const reasons: string[] = [];

  if (!wallet.ok && wallet.reason) {
    reasons.push(wallet.reason);
  }

  if (!energySatisfied) {
    reasons.push(
      `Gas Station reserve energy is insufficient. Need at least ${requiredEnergy}, got ${gasEnergy}.`
    );
  }

  if (!bandwidthSatisfied) {
    reasons.push(
      `Gas Station reserve bandwidth is insufficient. Need at least ${requiredBandwidth}, got ${gasBandwidth}.`
    );
  }

  return {
    ok: false,
    reason: reasons.join(" "),
    wallet,
    gasStation: {
      balance,
      energySatisfied,
      bandwidthSatisfied
    }
  };
}
```
