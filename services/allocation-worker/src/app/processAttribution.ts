import {
  AttributionDecision,
  AttributionService,
  FrontendAttributionInput,
  PrepareVerifiedPurchaseResult
} from "../domain/attribution";
import {
  AllocationDecision,
  AllocationService
} from "../domain/allocation";
import type {
  AllocationMode,
  PurchaseRecord,
  PurchaseStore
} from "../db/purchases";
import type { ControllerClient } from "../tron/controller";

export interface WorkerLogger {
  info?(payload: Record<string, unknown>): void;
  warn?(payload: Record<string, unknown>): void;
  error?(payload: Record<string, unknown>): void;
}

export interface ProcessAttributionConfig {
  attributionService: AttributionService;
  allocationService: AllocationService;
  store: PurchaseStore;
  controllerClient: ControllerClient;
  tronWeb: any;
  controllerContractAddress?: string;
  logger?: WorkerLogger;
}

export interface ProcessFrontendAttributionResult {
  stage: "frontend-attribution";
  attribution: AttributionDecision;
}

export interface ProcessVerifiedChainEventInput {
  txHash: string;
  buyerWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  blockTimestamp: number;
  allocationMode?: AllocationMode;
  feeLimitSun?: number;
}

export interface ProcessVerifiedChainEventResult {
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
  purchaseId: string;
  attribution: AttributionDecision | null;
  verification: PrepareVerifiedPurchaseResult;
  allocation: AllocationDecision | null;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeTxHash(value: string): string {
  return assertNonEmpty(value, "txHash").toLowerCase();
}

function normalizeSunAmount(
  value: string | number | bigint,
  fieldName: string
): string {
  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${fieldName} must be a non-negative integer string`);
  }

  return normalized;
}

function safeNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "bigint") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  if (value && typeof value === "object") {
    const asAny = value as Record<string, unknown>;

    const nestedCandidates = [
      asAny._hex,
      asAny.hex,
      asAny.value,
      asAny.amount,
      asAny.toString && typeof asAny.toString === "function" ? asAny.toString() : undefined
    ];

    for (const candidate of nestedCandidates) {
      if (typeof candidate === "string" && candidate.trim()) {
        const trimmed = candidate.trim();

        if (/^0x[0-9a-f]+$/i.test(trimmed)) {
          const parsed = Number(BigInt(trimmed));
          if (Number.isFinite(parsed)) {
            return parsed;
          }
        }

        const parsed = Number(trimmed);
        if (Number.isFinite(parsed)) {
          return parsed;
        }
      }
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

function parsePercentStrict(value: unknown, fieldName: string): number {
  const parsed = safeNumber(value, Number.NaN);

  if (!Number.isFinite(parsed)) {
    throw new Error(`${fieldName} is not a finite number`);
  }

  const normalized = Math.floor(parsed);

  if (normalized < 0) {
    throw new Error(`${fieldName} must be >= 0`);
  }

  if (normalized > 100) {
    throw new Error(`${fieldName} must be <= 100`);
  }

  return normalized;
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

function hasPositiveSunAmount(value: string | null | undefined): boolean {
  try {
    return BigInt(String(value || "0")) > 0n;
  } catch {
    return false;
  }
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

function getReadableErrorMessage(error: unknown, fallback: string): string {
  if (
    error &&
    typeof error === "object" &&
    "message" in error &&
    typeof (error as { message?: unknown }).message === "string"
  ) {
    const message = String((error as { message: string }).message || "").trim();

    if (message) {
      return message;
    }
  }

  if (typeof error === "string" && error.trim()) {
    return error.trim();
  }

  return fallback;
}

function toJsonSafe<T>(value: T): T {
  return JSON.parse(
    JSON.stringify(value, (_key, currentValue) =>
      typeof currentValue === "bigint" ? currentValue.toString() : currentValue
    )
  ) as T;
}

async function getControllerContractInstance(input: {
  tronWeb: any;
  controllerContractAddress?: string;
}): Promise<any> {
  if (!input.tronWeb) {
    throw new Error("tronWeb is required");
  }

  const controllerContractAddress = assertNonEmpty(
    input.controllerContractAddress || "",
    "controllerContractAddress"
  );

  return input.tronWeb.contract().at(controllerContractAddress);
}

async function callIfExists(contract: any, methodName: string, ...args: any[]): Promise<any> {
  if (!contract || typeof contract[methodName] !== "function") {
    throw new Error(`${methodName} is not available on controller contract`);
  }

  return contract[methodName](...args).call();
}

async function readAmbassadorRewardData(input: {
  tronWeb: any;
  controllerContractAddress?: string;
  ambassadorWallet: string;
  logger?: WorkerLogger;
}): Promise<{
  rewardPercent: number;
  effectiveLevel: number | null;
  source:
    | "getEffectiveLevel+getRewardPercentByLevel"
    | "getDashboardCore"
    | "getRewardPercent";
  raw: Record<string, unknown>;
}> {
  const contract = await getControllerContractInstance({
    tronWeb: input.tronWeb,
    controllerContractAddress: input.controllerContractAddress
  });

  const ambassadorWallet = assertNonEmpty(input.ambassadorWallet, "ambassadorWallet");
  const raw: Record<string, unknown> = {};

  if (
    typeof contract.getEffectiveLevel === "function" &&
    typeof contract.getRewardPercentByLevel === "function"
  ) {
    try {
      const effectiveLevelRaw = await callIfExists(
        contract,
        "getEffectiveLevel",
        ambassadorWallet
      );
      raw.getEffectiveLevel = effectiveLevelRaw;

      const effectiveLevel = Math.floor(
        safeNumber(
          pickTupleValue(effectiveLevelRaw, 0, "effectiveLevel"),
          Number.NaN
        )
      );

      if (!Number.isFinite(effectiveLevel) || effectiveLevel < 0) {
        throw new Error("Invalid effective level");
      }

      const rewardPercentByLevelRaw = await callIfExists(
        contract,
        "getRewardPercentByLevel",
        effectiveLevel
      );
      raw.getRewardPercentByLevel = rewardPercentByLevelRaw;

      const rewardPercent = parsePercentStrict(
        pickTupleValue(
          rewardPercentByLevelRaw,
          0,
          "rewardPercent",
          "percent"
        ),
        "getRewardPercentByLevel"
      );

      return {
        rewardPercent,
        effectiveLevel,
        source: "getEffectiveLevel+getRewardPercentByLevel",
        raw
      };
    } catch (error) {
      raw.getEffectiveLevelPlusByLevelError =
        error instanceof Error ? error.message : String(error);
    }
  }

  if (typeof contract.getDashboardCore === "function") {
    try {
      const coreRaw = await callIfExists(contract, "getDashboardCore", ambassadorWallet);
      raw.getDashboardCore = coreRaw;

      const effectiveLevel = Math.floor(
        safeNumber(
          pickTupleValue(coreRaw, 2, "effectiveLevel"),
          Number.NaN
        )
      );

      const rewardPercent = parsePercentStrict(
        pickTupleValue(coreRaw, 3, "rewardPercent"),
        "getDashboardCore.rewardPercent"
      );

      return {
        rewardPercent,
        effectiveLevel: Number.isFinite(effectiveLevel) ? effectiveLevel : null,
        source: "getDashboardCore",
        raw
      };
    } catch (error) {
      raw.getDashboardCoreError = error instanceof Error ? error.message : String(error);
    }
  }

  if (typeof contract.getRewardPercent === "function") {
    try {
      const rewardPercentRaw = await callIfExists(contract, "getRewardPercent", ambassadorWallet);
      raw.getRewardPercent = rewardPercentRaw;

      const rewardPercent = parsePercentStrict(
        pickTupleValue(rewardPercentRaw, 0, "rewardPercent", "percent"),
        "getRewardPercent"
      );

      let effectiveLevel: number | null = null;

      if (typeof contract.getEffectiveLevel === "function") {
        try {
          const effectiveLevelRaw = await callIfExists(
            contract,
            "getEffectiveLevel",
            ambassadorWallet
          );
          raw.getEffectiveLevelFallback = effectiveLevelRaw;

          const parsedLevel = Math.floor(
            safeNumber(
              pickTupleValue(effectiveLevelRaw, 0, "effectiveLevel"),
              Number.NaN
            )
          );

          if (Number.isFinite(parsedLevel) && parsedLevel >= 0) {
            effectiveLevel = parsedLevel;
          }
        } catch (error) {
          raw.getEffectiveLevelFallbackError =
            error instanceof Error ? error.message : String(error);
        }
      }

      return {
        rewardPercent,
        effectiveLevel,
        source: "getRewardPercent",
        raw
      };
    } catch (error) {
      raw.getRewardPercentError = error instanceof Error ? error.message : String(error);
    }
  }

  input.logger?.error?.({
    scope: "allocation",
    stage: "reward-percent-read-failed",
    ambassadorWallet,
    raw: toJsonSafe(raw)
  });

  throw new Error("Unable to read ambassador reward percent from controller");
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

export class AttributionProcessor {
  private readonly attributionService: AttributionService;
  private readonly allocationService: AllocationService;
  private readonly store: PurchaseStore;
  private readonly controllerClient: ControllerClient;
  private readonly tronWeb: any;
  private readonly controllerContractAddress?: string;
  private readonly logger?: WorkerLogger;

  constructor(config: ProcessAttributionConfig) {
    if (!config?.attributionService) {
      throw new Error("attributionService is required");
    }

    if (!config?.allocationService) {
      throw new Error("allocationService is required");
    }

    if (!config?.store) {
      throw new Error("store is required");
    }

    if (!config?.controllerClient) {
      throw new Error("controllerClient is required");
    }

    if (!config?.tronWeb) {
      throw new Error("tronWeb is required");
    }

    this.attributionService = config.attributionService;
    this.allocationService = config.allocationService;
    this.store = config.store;
    this.controllerClient = config.controllerClient;
    this.tronWeb = config.tronWeb;
    this.controllerContractAddress = config.controllerContractAddress;
    this.logger = config.logger;
  }

  async processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<ProcessFrontendAttributionResult> {
    const attribution = await this.attributionService.captureFrontendAttribution(input);

    return {
      stage: "frontend-attribution",
      attribution
    };
  }

  private async ensureVerifiedPurchaseRewardSplit(input: {
    purchase: PurchaseRecord;
    txHash: string;
    buyerWallet: string;
    purchaseAmountSun: string;
    ownerShareSun: string;
    now: number;
  }): Promise<PurchaseRecord> {
    const purchase = input.purchase;

    if (!purchase.ambassadorWallet) {
      return purchase;
    }

    const rewardData = await readAmbassadorRewardData({
      tronWeb: this.tronWeb,
      controllerContractAddress: this.controllerContractAddress,
      ambassadorWallet: purchase.ambassadorWallet,
      logger: this.logger
    });

    const ambassadorRewardSun = dividePercentFloor(
      input.ownerShareSun,
      rewardData.rewardPercent
    );
    const ownerPayoutSun = subtractSun(input.ownerShareSun, ambassadorRewardSun);

    const updatedPurchase = await this.store.markVerifiedPurchase({
      purchaseId: purchase.purchaseId,
      txHash: input.txHash,
      buyerWallet: input.buyerWallet,
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorRewardSun,
      ownerPayoutSun,
      now: input.now
    });

    this.logger?.info?.({
      scope: "allocation",
      stage: "reward-share-calculated",
      purchaseId: updatedPurchase.purchaseId,
      txHash: input.txHash,
      ambassadorWallet: updatedPurchase.ambassadorWallet,
      ownerShareSun: input.ownerShareSun,
      rewardPercent: rewardData.rewardPercent,
      effectiveLevel: rewardData.effectiveLevel,
      rewardSource: rewardData.source,
      ambassadorRewardSun,
      ownerPayoutSun,
      rawRewardData: toJsonSafe(rewardData.raw)
    });

    return updatedPurchase;
  }

  private async repairRewardSplitIfMissing(
    purchase: PurchaseRecord,
    now: number = Date.now()
  ): Promise<PurchaseRecord> {
    if (!purchase.ambassadorWallet) {
      return purchase;
    }

    if (!hasPositiveSunAmount(purchase.ownerShareSun)) {
      return purchase;
    }

    if (hasPositiveSunAmount(purchase.ambassadorRewardSun)) {
      return purchase;
    }

    const rewardData = await readAmbassadorRewardData({
      tronWeb: this.tronWeb,
      controllerContractAddress: this.controllerContractAddress,
      ambassadorWallet: purchase.ambassadorWallet,
      logger: this.logger
    });

    const ambassadorRewardSun = dividePercentFloor(
      purchase.ownerShareSun,
      rewardData.rewardPercent
    );
    const ownerPayoutSun = subtractSun(purchase.ownerShareSun, ambassadorRewardSun);

    const updatedPurchase = await this.store.update(purchase.purchaseId, {
      ambassadorRewardSun,
      ownerPayoutSun,
      now
    });

    this.logger?.info?.({
      scope: "allocation",
      stage: "reward-split-repaired-before-replay",
      purchaseId: updatedPurchase.purchaseId,
      ambassadorWallet: updatedPurchase.ambassadorWallet,
      ownerShareSun: updatedPurchase.ownerShareSun,
      rewardPercent: rewardData.rewardPercent,
      effectiveLevel: rewardData.effectiveLevel,
      rewardSource: rewardData.source,
      ambassadorRewardSun,
      ownerPayoutSun,
      rawRewardData: toJsonSafe(rewardData.raw)
    });

    return updatedPurchase;
  }

  async processVerifiedChainEvent(
    input: ProcessVerifiedChainEventInput
  ): Promise<ProcessVerifiedChainEventResult> {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = assertNonEmpty(input.buyerWallet, "buyerWallet");
    const purchaseAmountSun = normalizeSunAmount(
      input.purchaseAmountSun,
      "purchaseAmountSun"
    );
    const ownerShareSun = normalizeSunAmount(input.ownerShareSun, "ownerShareSun");
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
      return {
        stage: "verified-purchase",
        purchaseId: purchase.purchaseId,
        attribution: {
          status: "duplicate-local-record",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Purchase already finalized"
        },
        verification: {
          status: "already-finalized",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: null,
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

    if (verification.canAllocate) {
      verifiedPurchase = await this.ensureVerifiedPurchaseRewardSplit({
        purchase: verification.purchase,
        txHash,
        buyerWallet,
        purchaseAmountSun,
        ownerShareSun,
        now: blockTimestamp
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

    const allocationResult = await this.allocationService.tryAllocateVerifiedPurchase(
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
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = assertNonEmpty(input.buyerWallet, "buyerWallet");
    const slug = assertNonEmpty(input.slug, "slug");
    const purchaseAmountSun = normalizeSunAmount(
      input.purchaseAmountSun,
      "purchaseAmountSun"
    );
    const ownerShareSun = normalizeSunAmount(input.ownerShareSun, "ownerShareSun");
    const now = input.now ?? Date.now();
    const allocationMode = input.allocationMode ?? "eager";

    let attribution: AttributionDecision | null = null;

    try {
      attribution = await this.attributionService.captureFrontendAttribution({
        txHash,
        buyerWallet,
        slug,
        now
      });
    } catch (error) {
      throw new Error(
        getReadableErrorMessage(error, "Failed to capture frontend attribution")
      );
    }

    const purchaseId = attribution.purchase.purchaseId;

    const verification = await this.attributionService.prepareVerifiedPurchase({
      purchaseId,
      txHash,
      buyerWallet,
      slug,
      purchaseAmountSun,
      ownerShareSun,
      now
    });

    if (!verification.canAllocate) {
      return {
        stage: "verified-purchase",
        purchaseId,
        attribution,
        verification,
        allocation: null
      };
    }

    const verifiedPurchaseWithSplit = await this.ensureVerifiedPurchaseRewardSplit({
      purchase: verification.purchase,
      txHash,
      buyerWallet,
      purchaseAmountSun,
      ownerShareSun,
      now
    });

    const allocation = await this.allocationService.executeAllocation({
      purchaseId: verifiedPurchaseWithSplit.purchaseId,
      feeLimitSun: input.feeLimitSun,
      allocationMode,
      now
    });

    return {
      stage: "verified-purchase",
      purchaseId,
      attribution,
      verification: {
        ...verification,
        purchase: verifiedPurchaseWithSplit
      },
      allocation
    };
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<AllocationDecision> {
    const replayNow = now ?? Date.now();
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");

    const existingPurchase = await this.store.getByPurchaseId(normalizedPurchaseId);

    if (!existingPurchase) {
      throw new Error(`Purchase not found: ${normalizedPurchaseId}`);
    }

    await this.repairRewardSplitIfMissing(existingPurchase, replayNow);

    const replayResult = await this.allocationService.replayFailedAllocation(
      normalizedPurchaseId,
      feeLimitSun,
      replayNow
    );

    return {
      status:
        replayResult.status === "allocated"
          ? "allocated"
          : replayResult.status === "skipped"
            ? "skipped-already-final"
            : "retryable-failed",
      purchase: replayResult.purchase,
      txid: replayResult.txid,
      reason: replayResult.reason,
      errorCode: replayResult.errorCode,
      errorMessage: replayResult.errorMessage
    };
  }
}
