import { AllocationService, type AllocationDecision } from "./domain/allocation";
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
import {
  createGasStationClientFromEnv,
  type GasStationClient
} from "./services/gasStation";

export interface CreateAllocationWorkerOptions {
  tronWeb: any;
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

export interface ProcessVerifiedPurchaseAndAllocateInput {
  txHash: string;
  buyerWallet: string;
  slug?: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  feeLimitSun?: number;
  now?: number;
  allocationMode?: AllocationMode;
}

export interface ProcessVerifiedPurchaseAndAllocateResult {
  stage: "verified-purchase";
  purchaseId: string | null;
  attribution: ProcessChainEventResult["attribution"] | null;
  verification: ProcessChainEventResult["verification"];
  allocation: AllocationDecision | null;
}

export interface AllocationWorkerProcessor {
  attributionService: any;
  allocationService: AllocationService;

  processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<FrontendAttributionResult>;

  processVerifiedChainEvent(
    input: ProcessChainEventInput
  ): Promise<ProcessChainEventResult>;

  processVerifiedPurchaseAndAllocate(
    input: ProcessVerifiedPurchaseAndAllocateInput
  ): Promise<ProcessVerifiedPurchaseAndAllocateResult>;

  replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<ReplayFailedAllocationApiResult>;

  prepareWithdrawBatch(
    input: {
      ambassadorWallet: string;
      limit?: number;
    }
  ): Promise<{
    ambassadorWallet: string;
    purchases: PurchaseRecord[];
  }>;

  allocatePendingBatch(
    input: {
      ambassadorWallet: string;
      feeLimitSun?: number;
      limit?: number;
      allocationMode?: AllocationMode;
      stopOnFirstDeferred?: boolean;
    }
  ): Promise<{
    ambassadorWallet: string;
    processed: Awaited<ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>>[];
    stoppedEarly: boolean;
    stopReason: string | null;
  }>;
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
    status === "withdraw_included" ||
    status === "withdraw_completed" ||
    status === "ignored" ||
    status === "allocation_failed_final"
  );
}

function parseBooleanEnv(value: string | undefined, fallback = false): boolean {
  if (value == null || String(value).trim() === "") {
    return fallback;
  }

  const normalized = String(value).trim().toLowerCase();

  return (
    normalized === "1" ||
    normalized === "true" ||
    normalized === "yes" ||
    normalized === "on"
  );
}

function parseNonNegativeIntegerEnv(
  value: string | undefined,
  fallback: number
): number {
  if (value == null || String(value).trim() === "") {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`Invalid non-negative integer env value: ${value}`);
  }

  return Math.floor(parsed);
}

function parseStringEnv(value: string | undefined, fallback: string): string {
  const normalized = String(value || "").trim();
  return normalized || fallback;
}

function safeNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "bigint") {
    return Number(value);
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return fallback;
}

function pickTupleValue(source: any, index: number, ...keys: string[]): any {
  if (Array.isArray(source)) {
    if (source[index] !== undefined) {
      return source[index];
    }
  }

  if (source && typeof source === "object") {
    for (const key of keys) {
      if (key && key in source) {
        return source[key];
      }
    }

    const numericKey = String(index);
    if (numericKey in source) {
      return source[numericKey];
    }

    const values = Object.values(source);
    if (values[index] !== undefined) {
      return values[index];
    }
  }

  return undefined;
}

function dividePercentFloor(amountSun: string, percent: number): string {
  if (percent <= 0) {
    return "0";
  }

  if (percent >= 100) {
    return amountSun;
  }

  return ((BigInt(amountSun) * BigInt(percent)) / 100n).toString();
}

function subtractSun(left: string, right: string): string {
  const result = BigInt(left) - BigInt(right);
  return result > 0n ? result.toString() : "0";
}

async function readAmbassadorRewardPercent(input: {
  tronWeb: any;
  controllerContractAddress?: string;
  ambassadorWallet: string;
}): Promise<number> {
  const tronWeb = input.tronWeb;

  if (!tronWeb) {
    throw new Error("tronWeb is required to read ambassador reward percent");
  }

  const controllerContractAddress = assertNonEmpty(
    input.controllerContractAddress,
    "controllerContractAddress"
  );

  const contract = await tronWeb.contract().at(controllerContractAddress);

  if (typeof contract.getDashboardCore !== "function") {
    throw new Error("Controller contract does not expose getDashboardCore()");
  }

  const coreRaw = await contract.getDashboardCore(input.ambassadorWallet).call();
  const rewardPercent = safeNumber(
    pickTupleValue(coreRaw, 3, "rewardPercent"),
    0
  );

  if (!Number.isFinite(rewardPercent) || rewardPercent < 0) {
    return 0;
  }

  return Math.floor(rewardPercent);
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

  if (
    result.status === "deferred" ||
    result.status === "stopped-on-resource-shortage"
  ) {
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

function mapApiAllocationToDecision(
  purchase: PurchaseRecord,
  allocation:
    | {
        status: "allocated" | "deferred" | "failed" | "skipped";
        purchase: PurchaseRecord;
        ambassadorWallet: string | null;
        txid: string | null;
        reason: string | null;
      }
    | undefined
): AllocationDecision | null {
  if (!allocation) {
    return null;
  }

  if (allocation.status === "allocated") {
    return {
      status: "allocated",
      purchase,
      txid: allocation.txid,
      reason: null,
      errorCode: null,
      errorMessage: null
    };
  }

  if (allocation.status === "deferred") {
    return {
      status: "deferred",
      purchase,
      txid: null,
      reason: allocation.reason,
      errorCode: null,
      errorMessage: allocation.reason
    };
  }

  if (allocation.status === "skipped") {
    return {
      status: "skipped-no-ambassador-wallet",
      purchase,
      txid: null,
      reason: allocation.reason,
      errorCode: null,
      errorMessage: allocation.reason
    };
  }

  return {
    status: "retryable-failed",
    purchase,
    txid: null,
    reason: allocation.reason,
    errorCode: null,
    errorMessage: allocation.reason
  };
}

class AllocationWorkerProcessorImpl implements AllocationWorkerProcessor {
  public readonly attributionService: any = null;
  public readonly allocationService: AllocationService;

  private readonly store: PurchaseStore;
  private readonly allocation: AllocationService;
  private readonly logger?: WorkerLogger;
  private readonly tronWeb: any;
  private readonly controllerContractAddress?: string;

  constructor(options: {
    store: PurchaseStore;
    allocation: AllocationService;
    tronWeb: any;
    controllerContractAddress?: string;
    logger?: WorkerLogger;
  }) {
    this.store = options.store;
    this.allocation = options.allocation;
    this.allocationService = options.allocation;
    this.tronWeb = options.tronWeb;
    this.controllerContractAddress = options.controllerContractAddress;
    this.logger = options.logger;
  }

  async processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<FrontendAttributionResult> {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const slug = assertNonEmpty(input.slug, "slug");
    const now = input.now;

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

      receivedPurchase.purchase = await this.store.attachAmbassadorToPurchase({
        purchaseId: receivedPurchase.purchase.purchaseId,
        ambassadorSlug: slug,
        ambassadorWallet: ambassador.wallet,
        purchaseAmountSun: "0",
        ownerShareSun: "0",
        ambassadorRewardSun: "0",
        ownerPayoutSun: "0",
        now
      });
    }

    const currentPurchase = await this.store.getByPurchaseId(
      receivedPurchase.purchase.purchaseId
    );

    if (!currentPurchase) {
      throw new Error("Failed to reload purchase after frontend attribution");
    }

    this.logger?.info?.({
      scope: "frontend-attribution",
      stage: "received",
      txHash,
      purchaseId: currentPurchase.purchaseId,
      slug,
      ambassadorWallet,
      attributionStatus
    });

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
        reason:
          !ambassador || ambassador.status !== "active" || !ambassador.wallet
            ? attributionReason
            : null,
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

    if (isFinalPurchaseStatus(purchase.status)) {
      const ambassador =
        purchase.ambassadorSlug
          ? await this.store.getAmbassadorBySlug(purchase.ambassadorSlug)
          : null;

      return {
        stage: "verified-purchase",
        purchaseId: purchase.purchaseId,
        attribution: {
          status: "duplicate-local-record",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: ambassador?.slugHash ?? null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Purchase already finalized"
        },
        verification: {
          status: "already-finalized",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: ambassador?.slugHash ?? null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: `Purchase already finalized with status: ${purchase.status}`,
          canAllocate: false
        }
      };
    }

    let ambassadorRewardSun = "0";
    let ownerPayoutSun = ownerShareSun;

    if (purchase.ambassadorWallet) {
      const rewardPercent = await readAmbassadorRewardPercent({
        tronWeb: this.tronWeb,
        controllerContractAddress: this.controllerContractAddress,
        ambassadorWallet: purchase.ambassadorWallet
      });

      ambassadorRewardSun = dividePercentFloor(ownerShareSun, rewardPercent);
      ownerPayoutSun = subtractSun(ownerShareSun, ambassadorRewardSun);

      this.logger?.info?.({
        scope: "allocation",
        stage: "reward-share-calculated",
        purchaseId: purchase.purchaseId,
        txHash,
        ambassadorWallet: purchase.ambassadorWallet,
        ownerShareSun,
        rewardPercent,
        ambassadorRewardSun,
        ownerPayoutSun
      });
    }

    const verifiedPurchase = await this.store.markVerifiedPurchase({
      purchaseId: purchase.purchaseId,
      txHash,
      buyerWallet,
      purchaseAmountSun,
      ownerShareSun,
      ambassadorRewardSun,
      ownerPayoutSun,
      now: blockTimestamp
    });

    const ambassador =
      verifiedPurchase.ambassadorSlug
        ? await this.store.getAmbassadorBySlug(verifiedPurchase.ambassadorSlug)
        : null;

    const slugHash = ambassador?.slugHash ?? null;

    if (!verifiedPurchase.ambassadorWallet) {
      return {
        stage: "verified-purchase",
        purchaseId: verifiedPurchase.purchaseId,
        attribution: {
          status:
            purchase.status === "received"
              ? "matched-local-record"
              : "duplicate-local-record",
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
        status:
          purchase.status === "received"
            ? "matched-local-record"
            : "duplicate-local-record",
        purchase: allocationResult.purchase,
        slug: allocationResult.purchase.ambassadorSlug,
        slugHash,
        ambassadorWallet: allocationResult.purchase.ambassadorWallet,
        reason:
          purchase.status === "received"
            ? null
            : "Purchase already exists in local store"
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

  async processVerifiedPurchaseAndAllocate(
    input: ProcessVerifiedPurchaseAndAllocateInput
  ): Promise<ProcessVerifiedPurchaseAndAllocateResult> {
    const result = await this.processVerifiedChainEvent({
      txHash: input.txHash,
      buyerWallet: input.buyerWallet,
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      blockTimestamp: input.now ?? Date.now(),
      allocationMode: input.allocationMode ?? "eager",
      feeLimitSun: input.feeLimitSun
    });

    let allocationDecision: AllocationDecision | null = null;

    if (result.purchaseId) {
      const purchase = await this.store.getByPurchaseId(result.purchaseId);

      if (purchase) {
        allocationDecision = mapApiAllocationToDecision(purchase, result.allocation);
      }
    }

    return {
      stage: "verified-purchase",
      purchaseId: result.purchaseId,
      attribution: result.attribution,
      verification: result.verification,
      allocation: allocationDecision
    };
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<ReplayFailedAllocationApiResult> {
    return this.allocation.replayFailedAllocation(purchaseId, feeLimitSun, now);
  }

  async prepareWithdrawBatch(
    input: {
      ambassadorWallet: string;
      limit?: number;
    }
  ): Promise<{
    ambassadorWallet: string;
    purchases: PurchaseRecord[];
  }> {
    return this.allocation.prepareWithdrawBatch(input);
  }

  async allocatePendingBatch(
    input: {
      ambassadorWallet: string;
      feeLimitSun?: number;
      limit?: number;
      allocationMode?: AllocationMode;
      stopOnFirstDeferred?: boolean;
    }
  ): Promise<{
    ambassadorWallet: string;
    processed: Awaited<ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>>[];
    stoppedEarly: boolean;
    stopReason: string | null;
  }> {
    const result = await this.allocation.allocatePendingBatch({
      ambassadorWallet: input.ambassadorWallet,
      feeLimitSun: input.feeLimitSun,
      limit: input.limit,
      allocationMode: input.allocationMode,
      stopOnFirstDeferred: input.stopOnFirstDeferred
    });

    return {
      ambassadorWallet: result.ambassadorWallet,
      processed: result.processed as Awaited<
        ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>
      >[],
      stoppedEarly: result.stoppedEarly,
      stopReason: result.stopReason
    };
  }
}

export function createAllocationWorker(
  options: CreateAllocationWorkerOptions
): AllocationWorker {
  const store = createPurchaseStore();

  const gasStationEnabled = parseBooleanEnv(process.env.GASSTATION_ENABLED, false);
  const gasStationMinBandwidth = parseNonNegativeIntegerEnv(
    process.env.GASSTATION_MIN_BANDWIDTH,
    0
  );
  const gasStationMinEnergy = parseNonNegativeIntegerEnv(
    process.env.GASSTATION_MIN_ENERGY,
    0
  );
  const allocationMinBandwidth = parseNonNegativeIntegerEnv(
    process.env.ALLOCATION_MIN_BANDWIDTH,
    0
  );
  const allocationMinEnergy = parseNonNegativeIntegerEnv(
    process.env.ALLOCATION_MIN_ENERGY,
    0
  );
  const gasStationServiceChargeType = parseStringEnv(
    process.env.GASSTATION_SERVICE_CHARGE_TYPE,
    "10010"
  );

  let gasStationClient: GasStationClient | null = null;

  if (gasStationEnabled) {
    gasStationClient = createGasStationClientFromEnv();
  }

  options.logger?.info?.({
    scope: "gasstation",
    stage: "configured",
    enabled: gasStationEnabled,
    gasStationMinBandwidth,
    gasStationMinEnergy,
    allocationMinBandwidth,
    allocationMinEnergy,
    gasStationServiceChargeType
  });

  const executorConfig: TronControllerAllocationExecutorConfig = {
    tronWeb: options.tronWeb,
    controllerContractAddress: options.controllerContractAddress,
    gasStationClient,
    gasStationEnabled,
    gasStationMinBandwidth,
    gasStationMinEnergy,
    allocationMinBandwidth,
    allocationMinEnergy,
    gasStationServiceChargeType
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
    tronWeb: options.tronWeb,
    controllerContractAddress: options.controllerContractAddress,
    logger: options.logger
  });

  return {
    store,
    allocation,
    processor
  };
}
