import type TronWeb from "tronweb";
import { AllocationService } from "./domain/allocation";
import {
  createPurchaseStore,
  type AllocationMode,
  type PurchaseRecord,
  type PurchaseStore
} from "./db/purchases";
import {
  TronControllerAllocationExecutor,
  type TronControllerAllocationExecutorConfig
} from "./tron/controller";

export interface CreateAllocationWorkerOptions {
  tronWeb: TronWeb;
  controllerContractAddress?: string;
  logger?: WorkerLogger;
}

export interface WorkerLogger {
  info?(payload: Record<string, unknown>): void;
  warn?(payload: Record<string, unknown>): void;
  error?(payload: Record<string, unknown>): void;
}

export interface FrontendAttributionInput {
  txHash: string;
  buyerWallet: string;
  slug: string;
  now: number;
  allocationMode?: AllocationMode;
  feeLimitSun?: number;
}

export interface FrontendAttributionResult {
  stage: "received-purchase" | "verified-purchase";
  purchaseId: string;
  attribution: {
    status:
      | "created"
      | "duplicate-local-record"
      | "slug-not-found"
      | "slug-inactive"
      | "wallet-missing";
    purchase: PurchaseRecord | null;
    slug: string;
    slugHash: string | null;
    ambassadorWallet: string | null;
    reason: string | null;
  };
  verification: {
    status:
      | "waiting-scan"
      | "ready-for-allocation"
      | "already-finalized"
      | "ignored";
    purchase: PurchaseRecord;
    slug: string;
    slugHash: string | null;
    ambassadorWallet: string | null;
    reason: string | null;
    canAllocate: boolean;
  };
  allocation?: {
    status: "allocated" | "deferred" | "failed" | "skipped";
    purchase: PurchaseRecord;
    ambassadorWallet: string | null;
    txid: string | null;
    reason: string | null;
  };
}

export interface ReplayFailedAllocationApiResult {
  status: "allocated" | "failed" | "skipped";
  purchase: PurchaseRecord;
  ambassadorWallet: string | null;
  txid: string | null;
  reason: string | null;
}

export interface ProcessChainEventInput {
  txHash: string;
  buyerWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  blockTimestamp: number;
  allocationMode?: AllocationMode;
  feeLimitSun?: number;
}

export interface ProcessChainEventResult {
  stage: "verified-purchase";
  purchaseId: string | null;
  attribution: {
    status:
      | "matched-local-record"
      | "duplicate-local-record"
      | "no-local-record"
      | "wallet-mismatch";
    purchase: PurchaseRecord | null;
    slug: string | null;
    slugHash: string | null;
    ambassadorWallet: string | null;
    reason: string | null;
  };
  verification: {
    status:
      | "ready-for-allocation"
      | "already-finalized"
      | "ignored"
      | "no-attribution";
    purchase: PurchaseRecord | null;
    slug: string | null;
    slugHash: string | null;
    ambassadorWallet: string | null;
    reason: string | null;
    canAllocate: boolean;
  };
  allocation?: {
    status: "allocated" | "deferred" | "failed" | "skipped";
    purchase: PurchaseRecord;
    ambassadorWallet: string | null;
    txid: string | null;
    reason: string | null;
  };
}

export interface AllocationWorkerProcessor {
  processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<FrontendAttributionResult>;

  processVerifiedChainEvent(
    input: ProcessChainEventInput
  ): Promise<ProcessChainEventResult>;

  replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<ReplayFailedAllocationApiResult>;
}

export interface AllocationWorker {
  store: PurchaseStore;
  allocation: AllocationService;
  processor: AllocationWorkerProcessor;
}

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

function normalizeTxHash(value: unknown): string {
  return assertNonEmpty(value, "txHash").toLowerCase();
}

function normalizeWallet(value: unknown, fieldName: string): string {
  return assertNonEmpty(value, fieldName);
}

function parseAmountAsString(value: unknown, fieldName: string): string {
  const raw = assertNonEmpty(value, fieldName);

  if (!/^\d+$/.test(raw)) {
    throw new Error(`${fieldName} must be a numeric string`);
  }

  return raw;
}

function isFinalPurchaseStatus(status: PurchaseRecord["status"]): boolean {
  return (
    status === "allocated" ||
    status === "ignored" ||
    status === "allocation_failed_final"
  );
}

function mapAllocationAttemptToApiResult(
  result: Awaited<ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>>
): {
  status: "allocated" | "deferred" | "failed" | "skipped";
  purchase: PurchaseRecord;
  ambassadorWallet: string | null;
  txid: string | null;
  reason: string | null;
} {
  if (result.status === "allocated") {
    return {
      status: "allocated",
      purchase: result.purchase,
      ambassadorWallet: result.purchase.ambassadorWallet,
      txid: result.txid,
      reason: null
    };
  }

  if (result.status === "deferred") {
    return {
      status: "deferred",
      purchase: result.purchase,
      ambassadorWallet: result.purchase.ambassadorWallet,
      txid: null,
      reason: result.reason
    };
  }

  if (
    result.status === "skipped-already-final" ||
    result.status === "skipped-no-ambassador-wallet"
  ) {
    return {
      status: "skipped",
      purchase: result.purchase,
      ambassadorWallet: result.purchase.ambassadorWallet,
      txid: null,
      reason: result.reason
    };
  }

  return {
    status: "failed",
    purchase: result.purchase,
    ambassadorWallet: result.purchase.ambassadorWallet,
    txid: null,
    reason: result.reason
  };
}

class AllocationWorkerProcessorImpl implements AllocationWorkerProcessor {
  private readonly store: PurchaseStore;
  private readonly allocation: AllocationService;
  private readonly logger?: WorkerLogger;

  constructor(options: {
    store: PurchaseStore;
    allocation: AllocationService;
    logger?: WorkerLogger;
  }) {
    this.store = options.store;
    this.allocation = options.allocation;
    this.logger = options.logger;
  }

  async processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<FrontendAttributionResult> {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const slug = assertNonEmpty(input.slug, "slug");
    const now = input.now;
    const allocationMode = input.allocationMode ?? "eager";

    const ambassador = await this.store.getAmbassadorBySlug(slug);

    const receivedPurchase = await this.store.createOrGetReceivedPurchase({
      txHash,
      buyerWallet,
      ambassadorSlug: slug,
      now
    });

    let attributionStatus:
      | "created"
      | "duplicate-local-record"
      | "slug-not-found"
      | "slug-inactive"
      | "wallet-missing" = receivedPurchase.created ? "created" : "duplicate-local-record";

    let attributionReason: string | null = receivedPurchase.created
      ? null
      : "Purchase already exists in local store";

    let slugHash: string | null = null;
    let ambassadorWallet: string | null = null;

    if (!ambassador) {
      attributionStatus = "slug-not-found";
      attributionReason = "Ambassador slug not found";
    } else if (ambassador.status !== "active") {
      attributionStatus = "slug-inactive";
      attributionReason = `Ambassador status is not active: ${ambassador.status}`;
      slugHash = ambassador.slugHash;
      ambassadorWallet = ambassador.wallet ?? null;
    } else if (!ambassador.wallet) {
      attributionStatus = "wallet-missing";
      attributionReason = "Ambassador wallet is missing";
      slugHash = ambassador.slugHash;
    } else {
      slugHash = ambassador.slugHash;
      ambassadorWallet = ambassador.wallet;

      const linkedPurchase = await this.store.attachAmbassadorToPurchase({
        purchaseId: receivedPurchase.purchase.purchaseId,
        ambassadorSlug: slug,
        ambassadorWallet: ambassador.wallet,
        purchaseAmountSun: "0",
        ownerShareSun: "0",
        now
      });

      if (linkedPurchase) {
        receivedPurchase.purchase = linkedPurchase;
      }
    }

    const currentPurchase = await this.store.getByPurchaseId(
      receivedPurchase.purchase.purchaseId
    );

    if (!currentPurchase) {
      throw new Error("Failed to reload purchase after frontend attribution");
    }

    if (!ambassador || ambassador.status !== "active" || !ambassador.wallet) {
      return {
        stage: "received-purchase",
        purchaseId: currentPurchase.purchaseId,
        attribution: {
          status: attributionStatus,
          purchase: currentPurchase,
          slug,
          slugHash,
          ambassadorWallet,
          reason: attributionReason
        },
        verification: {
          status: "waiting-scan",
          purchase: currentPurchase,
          slug,
          slugHash,
          ambassadorWallet,
          reason: attributionReason,
          canAllocate: false
        }
      };
    }

    return {
      stage: "received-purchase",
      purchaseId: currentPurchase.purchaseId,
      attribution: {
        status: attributionStatus,
        purchase: currentPurchase,
        slug,
        slugHash,
        ambassadorWallet,
        reason: attributionReason
      },
      verification: {
        status: "waiting-scan",
        purchase: currentPurchase,
        slug,
        slugHash,
        ambassadorWallet,
        reason: null,
        canAllocate: false
      }
    };
  }

  async processVerifiedChainEvent(
    input: ProcessChainEventInput
  ): Promise<ProcessChainEventResult> {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const purchaseAmountSun = parseAmountAsString(
      input.purchaseAmountSun,
      "purchaseAmountSun"
    );
    const ownerShareSun = parseAmountAsString(input.ownerShareSun, "ownerShareSun");
    const blockTimestamp = Number(input.blockTimestamp);
    const allocationMode = input.allocationMode ?? "eager";

    if (!Number.isFinite(blockTimestamp) || blockTimestamp <= 0) {
      throw new Error("blockTimestamp must be a positive number");
    }

    const purchase = await this.store.getByTxHash(txHash);

    if (!purchase) {
      return {
        stage: "verified-purchase",
        purchaseId: null,
        attribution: {
          status: "no-local-record",
          purchase: null,
          slug: null,
          slugHash: null,
          ambassadorWallet: null,
          reason: "No local attribution record found for txHash"
        },
        verification: {
          status: "no-attribution",
          purchase: null,
          slug: null,
          slugHash: null,
          ambassadorWallet: null,
          reason: "No local attribution record found for txHash",
          canAllocate: false
        }
      };
    }

    if (
      purchase.buyerWallet &&
      purchase.buyerWallet.toLowerCase() !== buyerWallet.toLowerCase()
    ) {
      return {
        stage: "verified-purchase",
        purchaseId: purchase.purchaseId,
        attribution: {
          status: "wallet-mismatch",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Buyer wallet mismatch for txHash"
        },
        verification: {
          status: "ignored",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Buyer wallet mismatch for txHash",
          canAllocate: false
        }
      };
    }

    const verifiedPurchase = await this.store.markVerifiedPurchase({
      purchaseId: purchase.purchaseId,
      txHash,
      buyerWallet,
      purchaseAmountSun,
      ownerShareSun,
      now: blockTimestamp
    });

    const ambassador =
      verifiedPurchase.ambassadorSlug
        ? await this.store.getAmbassadorBySlug(verifiedPurchase.ambassadorSlug)
        : null;

    const slugHash = ambassador?.slugHash ?? null;

    if (isFinalPurchaseStatus(verifiedPurchase.status)) {
      return {
        stage: "verified-purchase",
        purchaseId: verifiedPurchase.purchaseId,
        attribution: {
          status: "matched-local-record",
          purchase: verifiedPurchase,
          slug: verifiedPurchase.ambassadorSlug,
          slugHash,
          ambassadorWallet: verifiedPurchase.ambassadorWallet,
          reason: null
        },
        verification: {
          status: "already-finalized",
          purchase: verifiedPurchase,
          slug: verifiedPurchase.ambassadorSlug,
          slugHash,
          ambassadorWallet: verifiedPurchase.ambassadorWallet,
          reason: `Purchase already finalized with status: ${verifiedPurchase.status}`,
          canAllocate: false
        }
      };
    }

    if (!verifiedPurchase.ambassadorWallet) {
      return {
        stage: "verified-purchase",
        purchaseId: verifiedPurchase.purchaseId,
        attribution: {
          status: "matched-local-record",
          purchase: verifiedPurchase,
          slug: verifiedPurchase.ambassadorSlug,
          slugHash,
          ambassadorWallet: verifiedPurchase.ambassadorWallet,
          reason: null
        },
        verification: {
          status: "ignored",
          purchase: verifiedPurchase,
          slug: verifiedPurchase.ambassadorSlug,
          slugHash,
          ambassadorWallet: verifiedPurchase.ambassadorWallet,
          reason: "Ambassador wallet is missing for verified purchase",
          canAllocate: false
        }
      };
    }

    const allocationResult = await this.allocation.tryAllocateVerifiedPurchase(
      verifiedPurchase.purchaseId,
      {
        feeLimitSun: input.feeLimitSun,
        allocationMode
      }
    );

    return {
      stage: "verified-purchase",
      purchaseId: verifiedPurchase.purchaseId,
      attribution: {
        status: purchase.status === "received" ? "matched-local-record" : "duplicate-local-record",
        purchase: allocationResult.purchase,
        slug: allocationResult.purchase.ambassadorSlug,
        slugHash,
        ambassadorWallet: allocationResult.purchase.ambassadorWallet,
        reason: purchase.status === "received" ? null : "Purchase already exists in local store"
      },
      verification: {
        status: "ready-for-allocation",
        purchase: allocationResult.purchase,
        slug: allocationResult.purchase.ambassadorSlug,
        slugHash,
        ambassadorWallet: allocationResult.purchase.ambassadorWallet,
        reason: null,
        canAllocate: true
      },
      allocation: mapAllocationAttemptToApiResult(allocationResult)
    };
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<ReplayFailedAllocationApiResult> {
    return this.allocation.replayFailedAllocation(purchaseId, feeLimitSun, now);
  }
}

export function createAllocationWorker(
  options: CreateAllocationWorkerOptions
): AllocationWorker {
  const store = createPurchaseStore();

  const executorConfig: TronControllerAllocationExecutorConfig = {
    tronWeb: options.tronWeb,
    controllerContractAddress: options.controllerContractAddress
  };

  const executor = new TronControllerAllocationExecutor(executorConfig);

  const allocation = new AllocationService({
    store,
    executor,
    logger: options.logger
  });

  const processor = new AllocationWorkerProcessorImpl({
    store,
    allocation,
    logger: options.logger
  });

  return {
    store,
    allocation,
    processor
  };
}
