import crypto from "node:crypto";
import { keccak_256 } from "@noble/hashes/sha3";
import { utf8ToBytes } from "@noble/hashes/utils";
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
  attribution: Awaited<ReturnType<AttributionProcessor["processFrontendAttribution"]>>["attribution"];
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
    processed: Awaited<ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>>[];
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
  if (Array.isArray(source) && source[index] !== undefined) {
    return source[index];
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

function isFinalPurchaseStatus(status: PurchaseRecord["status"]): boolean {
  return (
    status === "allocated" ||
    status === "withdraw_included" ||
    status === "withdraw_completed" ||
    status === "ignored" ||
    status === "allocation_failed_final"
  );
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function hashSlugToBytes32Hex(slug: string): string {
  const bytes = utf8ToBytes(String(slug || "").trim().toLowerCase());
  return `0x${bytesToHex(keccak_256(bytes))}`;
}

function derivePurchaseId(input: { txHash: string; buyerWallet: string }): string {
  const payload = `${normalizeTxHash(input.txHash)}:${normalizeWallet(
    input.buyerWallet,
    "buyerWallet"
  ).toLowerCase()}`;

  return `0x${crypto.createHash("sha256").update(payload).digest("hex")}`;
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
    return 0;
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
  errorCode: string | null;
  errorMessage: string | null;
} {
  if (result.status === "allocated") {
    return {
      status: "allocated",
      purchase: result.purchase,
      ambassadorWallet: result.purchase.ambassadorWallet,
      txid: result.txid,
      reason: null,
      errorCode: null,
      errorMessage: null
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
      reason: result.reason,
      errorCode: result.errorCode,
      errorMessage: result.errorMessage
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
      reason: result.reason,
      errorCode: result.errorCode,
      errorMessage: result.errorMessage
    };
  }

  return {
    status: "failed",
    purchase: result.purchase,
    ambassadorWallet: result.purchase.ambassadorWallet,
    txid: null,
    reason: result.reason,
    errorCode: result.errorCode,
    errorMessage: result.errorMessage
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
        errorCode: string | null;
        errorMessage: string | null;
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
      errorCode: allocation.errorCode,
      errorMessage: allocation.errorMessage
    };
  }

  if (allocation.status === "skipped") {
    return {
      status: "skipped-no-ambassador-wallet",
      purchase,
      txid: null,
      reason: allocation.reason,
      errorCode: allocation.errorCode,
      errorMessage: allocation.errorMessage
    };
  }

  return {
    status: "retryable-failed",
    purchase,
    txid: null,
    reason: allocation.reason,
    errorCode: allocation.errorCode,
    errorMessage: allocation.errorMessage
  };
}

class AllocationWorkerProcessorImpl implements AllocationWorkerProcessor {
  public readonly attributionService: AttributionService;
  public readonly allocationService: AllocationService;

  private readonly store: PurchaseStore;
  private readonly allocation: AllocationService;
  private readonly attributionProcessor: AttributionProcessor;
  private readonly logger?: WorkerLogger;
  private readonly tronWeb: any;
  private readonly controllerContractAddress?: string;

  constructor(options: {
    store: PurchaseStore;
    allocation: AllocationService;
    attributionService: AttributionService;
    attributionProcessor: AttributionProcessor;
    tronWeb: any;
    controllerContractAddress?: string;
    logger?: WorkerLogger;
  }) {
    this.store = options.store;
    this.allocation = options.allocation;
    this.allocationService = options.allocation;
    this.attributionService = options.attributionService;
    this.attributionProcessor = options.attributionProcessor;
    this.tronWeb = options.tronWeb;
    this.controllerContractAddress = options.controllerContractAddress;
    this.logger = options.logger;
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
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const purchaseAmountSun = parseAmountAsString(input.purchaseAmountSun, "purchaseAmountSun");
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
          slugHash: purchase.ambassadorSlug ? hashSlugToBytes32Hex(purchase.ambassadorSlug) : null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Buyer wallet mismatch for txHash"
        },
        verification: {
          status: "ignored",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: purchase.ambassadorSlug ? hashSlugToBytes32Hex(purchase.ambassadorSlug) : null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Buyer wallet mismatch for txHash",
          canAllocate: false
        }
      };
    }

    if (isFinalPurchaseStatus(purchase.status)) {
      return {
        stage: "verified-purchase",
        purchaseId: purchase.purchaseId,
        attribution: {
          status: "duplicate-local-record",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: purchase.ambassadorSlug ? hashSlugToBytes32Hex(purchase.ambassadorSlug) : null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Purchase already finalized"
        },
        verification: {
          status: "already-finalized",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: purchase.ambassadorSlug ? hashSlugToBytes32Hex(purchase.ambassadorSlug) : null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: `Purchase already finalized with status: ${purchase.status}`,
          canAllocate: false
        }
      };
    }

    if (!purchase.ambassadorSlug) {
      return {
        stage: "verified-purchase",
        purchaseId: purchase.purchaseId,
        attribution: {
          status: "matched-local-record",
          purchase,
          slug: null,
          slugHash: null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Purchase exists locally but ambassador slug is missing"
        },
        verification: {
          status: "ignored",
          purchase,
          slug: null,
          slugHash: null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Purchase exists locally but ambassador slug is missing",
          canAllocate: false
        }
      };
    }

    const verification = await this.attributionService.prepareVerifiedPurchase({
      purchaseId: purchase.purchaseId,
      txHash,
      buyerWallet,
      slug: purchase.ambassadorSlug,
      purchaseAmountSun,
      ownerShareSun,
      now: blockTimestamp
    });

    let verifiedPurchase = verification.purchase;

    if (verification.canAllocate && verifiedPurchase.ambassadorWallet) {
      const rewardPercent = await readAmbassadorRewardPercent({
        tronWeb: this.tronWeb,
        controllerContractAddress: this.controllerContractAddress,
        ambassadorWallet: verifiedPurchase.ambassadorWallet
      });

      const ambassadorRewardSun = dividePercentFloor(ownerShareSun, rewardPercent);
      const ownerPayoutSun = subtractSun(ownerShareSun, ambassadorRewardSun);

      verifiedPurchase = await this.store.markVerifiedPurchase({
        purchaseId: verifiedPurchase.purchaseId,
        txHash,
        buyerWallet,
        purchaseAmountSun,
        ownerShareSun,
        ambassadorRewardSun,
        ownerPayoutSun,
        now: blockTimestamp
      });

      this.logger?.info?.({
        scope: "allocation",
        stage: "reward-share-calculated",
        purchaseId: verifiedPurchase.purchaseId,
        txHash,
        ambassadorWallet: verifiedPurchase.ambassadorWallet,
        ownerShareSun,
        rewardPercent,
        ambassadorRewardSun,
        ownerPayoutSun
      });
    }

    if (!verification.canAllocate) {
      return {
        stage: "verified-purchase",
        purchaseId: verifiedPurchase.purchaseId,
        attribution: {
          status:
            purchase.status === "received"
              ? "matched-local-record"
              : "duplicate-local-record",
          purchase: verifiedPurchase,
          slug: verification.slug,
          slugHash: verification.slugHash,
          ambassadorWallet: verification.ambassadorWallet,
          reason:
            purchase.status === "received"
              ? null
              : "Purchase already exists in local store"
        },
        verification: {
          status:
            verification.status === "already-processed-on-chain"
              ? "already-finalized"
              : "ignored",
          purchase: verifiedPurchase,
          slug: verification.slug,
          slugHash: verification.slugHash,
          ambassadorWallet: verification.ambassadorWallet,
          reason: verification.reason,
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
        slug: verification.slug,
        slugHash: verification.slugHash,
        ambassadorWallet: allocationResult.purchase.ambassadorWallet,
        reason:
          purchase.status === "received"
            ? null
            : "Purchase already exists in local store"
      },
      verification: {
        status: "ready-for-allocation",
        purchase: allocationResult.purchase,
        slug: verification.slug,
        slugHash: verification.slugHash,
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
    const result = await this.attributionProcessor.processVerifiedPurchaseAndAllocate({
      txHash: normalizeTxHash(input.txHash),
      buyerWallet: normalizeWallet(input.buyerWallet, "buyerWallet"),
      slug: assertNonEmpty(input.slug, "slug"),
      purchaseAmountSun: parseAmountAsString(input.purchaseAmountSun, "purchaseAmountSun"),
      ownerShareSun: parseAmountAsString(input.ownerShareSun, "ownerShareSun"),
      feeLimitSun: input.feeLimitSun,
      now: input.now ?? Date.now()
    });

    return {
      stage: "verified-purchase",
      purchaseId: result.purchaseId,
      attribution: {
        status:
          result.attribution?.status === "duplicate-local-record"
            ? "duplicate-local-record"
            : "matched-local-record",
        purchase: result.attribution?.purchase ?? null,
        slug: result.attribution?.slug ?? null,
        slugHash: result.attribution?.slugHash ?? null,
        ambassadorWallet: result.attribution?.ambassadorWallet ?? null,
        reason: result.attribution?.reason ?? null
      },
      verification: {
        status:
          result.verification.status === "already-processed-on-chain"
            ? "already-finalized"
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
    allocationService: allocation
  });

  const processor = new AllocationWorkerProcessorImpl({
    store,
    allocation,
    attributionService,
    attributionProcessor,
    tronWeb: options.tronWeb,
    controllerContractAddress: options.controllerContractAddress,
    logger: options.logger
  });

  return {
    store,
    allocation,
    processor,
    attributionService
  };
}
