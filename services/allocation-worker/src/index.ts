import { AllocationService, type AllocationDecision } from "./domain/allocation";
import { AttributionService } from "./domain/attribution";
import { AttributionProcessor } from "./app/processAttribution";
import {
  createPurchaseStore,
  type AllocationMode,
  type PurchaseRecord,
  type PurchaseStore
} from "./db/purchases";
import {
  TronControllerAllocationExecutor,
  TronControllerClient,
  type TronControllerAllocationExecutorConfig
} from "./tron/controller";
import { derivePurchaseId, hashSlugToBytes32Hex } from "./tron/hashing";
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
  stage: "frontend-attribution";
  attribution: Awaited<
    ReturnType<AttributionProcessor["processFrontendAttribution"]>
  >["attribution"];
}

export interface ReplayFailedAllocationApiResult {
  status: "allocated" | "failed" | "skipped";
  purchase: PurchaseRecord;
  ambassadorWallet: string | null;
  txid: string | null;
  reason: string | null;
  errorCode: string | null;
  errorMessage: string | null;
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
    errorCode: string | null;
    errorMessage: string | null;
  };
}

export interface ProcessVerifiedPurchaseAndAllocateInput {
  txHash: string;
  buyerWallet: string;
  slug: string;
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
  attributionService: AttributionService;
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
    processed: Awaited<
      ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>
    >[];
    stoppedEarly: boolean;
    stopReason: string | null;
  }>;
}

export interface AllocationWorker {
  store: PurchaseStore;
  allocation: AllocationService;
  processor: AllocationWorkerProcessor;
  attributionService: AttributionService;
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

class AllocationWorkerProcessorImpl implements AllocationWorkerProcessor {
  public readonly attributionService: AttributionService;
  public readonly allocationService: AllocationService;

  private readonly allocation: AllocationService;
  private readonly attributionProcessor: AttributionProcessor;

  constructor(options: {
    allocation: AllocationService;
    attributionService: AttributionService;
    attributionProcessor: AttributionProcessor;
  }) {
    this.allocation = options.allocation;
    this.allocationService = options.allocation;
    this.attributionService = options.attributionService;
    this.attributionProcessor = options.attributionProcessor;
  }

  async processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<FrontendAttributionResult> {
    return this.attributionProcessor.processFrontendAttribution({
      txHash: normalizeTxHash(input.txHash),
      buyerWallet: normalizeWallet(input.buyerWallet, "buyerWallet"),
      slug: assertNonEmpty(input.slug, "slug"),
      now: input.now
    });
  }

  async processVerifiedChainEvent(
    input: ProcessChainEventInput
  ): Promise<ProcessChainEventResult> {
    return this.attributionProcessor.processVerifiedChainEvent({
      txHash: normalizeTxHash(input.txHash),
      buyerWallet: normalizeWallet(input.buyerWallet, "buyerWallet"),
      purchaseAmountSun: parseAmountAsString(
        input.purchaseAmountSun,
        "purchaseAmountSun"
      ),
      ownerShareSun: parseAmountAsString(input.ownerShareSun, "ownerShareSun"),
      blockTimestamp: Number(input.blockTimestamp),
      allocationMode: input.allocationMode,
      feeLimitSun: input.feeLimitSun
    });
  }

  async processVerifiedPurchaseAndAllocate(
    input: ProcessVerifiedPurchaseAndAllocateInput
  ): Promise<ProcessVerifiedPurchaseAndAllocateResult> {
    const result = await this.attributionProcessor.processVerifiedPurchaseAndAllocate({
      txHash: normalizeTxHash(input.txHash),
      buyerWallet: normalizeWallet(input.buyerWallet, "buyerWallet"),
      slug: assertNonEmpty(input.slug, "slug"),
      purchaseAmountSun: parseAmountAsString(
        input.purchaseAmountSun,
        "purchaseAmountSun"
      ),
      ownerShareSun: parseAmountAsString(input.ownerShareSun, "ownerShareSun"),
      feeLimitSun: input.feeLimitSun,
      now: input.now ?? Date.now(),
      allocationMode: input.allocationMode
    });

    return {
      stage: "verified-purchase",
      purchaseId: result.purchaseId,
      attribution: result.attribution
        ? {
            status:
              result.attribution.status === "duplicate-local-record"
                ? "duplicate-local-record"
                : result.attribution.status === "no-local-record"
                  ? "no-local-record"
                  : result.attribution.status === "wallet-mismatch"
                    ? "wallet-mismatch"
                    : "matched-local-record",
            purchase: result.attribution.purchase ?? null,
            slug: result.attribution.slug ?? null,
            slugHash: result.attribution.slugHash ?? null,
            ambassadorWallet: result.attribution.ambassadorWallet ?? null,
            reason: result.attribution.reason ?? null
          }
        : null,
      verification: {
        status:
          result.verification.status === "already-processed-on-chain"
            ? "already-finalized"
            : result.verification.status === "no-attribution"
              ? "no-attribution"
              : result.verification.canAllocate
                ? "ready-for-allocation"
                : "ignored",
        purchase: result.verification.purchase,
        slug: result.verification.slug,
        slugHash: result.verification.slugHash,
        ambassadorWallet: result.verification.ambassadorWallet,
        reason: result.verification.reason,
        canAllocate: result.verification.canAllocate
      },
      allocation: result.allocation
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
    processed: Awaited<
      ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>
    >[];
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
  const ownerAutoWithdrawEnabled = parseBooleanEnv(
    process.env.OWNER_AUTO_WITHDRAW_ENABLED,
    false
  );
  const ownerWithdrawMinSun = parseNonNegativeIntegerEnv(
    process.env.OWNER_WITHDRAW_MIN_SUN,
    1
  );
  const ownerWithdrawFeeLimitSun = parseNonNegativeIntegerEnv(
    process.env.OWNER_WITHDRAW_FEE_LIMIT_SUN,
    300_000_000
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
    gasStationServiceChargeType,
    ownerAutoWithdrawEnabled,
    ownerWithdrawMinSun,
    ownerWithdrawFeeLimitSun
  });

  const controllerClient = new TronControllerClient({
    tronWeb: options.tronWeb,
    contractAddress: options.controllerContractAddress,
    gasStationClient,
    gasStationEnabled,
    gasStationMinBandwidth,
    gasStationMinEnergy,
    allocationMinBandwidth,
    allocationMinEnergy,
    gasStationServiceChargeType,
    ownerAutoWithdrawEnabled,
    ownerWithdrawMinSun,
    ownerWithdrawFeeLimitSun
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
    gasStationServiceChargeType,
    ownerAutoWithdrawEnabled,
    ownerWithdrawMinSun,
    ownerWithdrawFeeLimitSun
  };

  const executor = new TronControllerAllocationExecutor(executorConfig);

  const allocation = new AllocationService({
    store,
    executor,
    logger: options.logger
  });

  const attributionService = new AttributionService({
    store,
    controllerClient,
    hashing: {
      hashSlugToBytes32Hex,
      derivePurchaseId
    }
  });

  const attributionProcessor = new AttributionProcessor({
    attributionService,
    allocationService: allocation,
    store,
    controllerClient,
    tronWeb: options.tronWeb,
    controllerContractAddress: options.controllerContractAddress,
    logger: options.logger
  });

  const processor = new AllocationWorkerProcessorImpl({
    allocation,
    attributionService,
    attributionProcessor
  });

  return {
    store,
    allocation,
    processor,
    attributionService
  };
}
