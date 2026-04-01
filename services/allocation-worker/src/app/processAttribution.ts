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
import type { AllocationMode, PurchaseStore } from "../db/purchases";
import type { ControllerClient } from "../tron/controller";

export interface ProcessAttributionConfig {
  attributionService: AttributionService;
  allocationService: AllocationService;
  store: PurchaseStore;
  controllerClient: ControllerClient;
  logger?: {
    info?(payload: Record<string, unknown>): void;
    warn?(payload: Record<string, unknown>): void;
    error?(payload: Record<string, unknown>): void;
  };
}

export interface ProcessFrontendAttributionResult {
  stage: "frontend-attribution";
  attribution: AttributionDecision;
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

function normalizeSunAmount(value: string | number | bigint, fieldName: string): string {
  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${fieldName} must be a non-negative integer string`);
  }

  return normalized;
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

async function readAmbassadorRewardData(input: {
  controllerClient: ControllerClient;
  ambassadorWallet: string;
}): Promise<{
  rewardPercent: number;
  effectiveLevel: number | null;
  raw: Record<string, unknown>;
}> {
  const ambassadorWallet = assertNonEmpty(input.ambassadorWallet, "ambassadorWallet");
  const contract = await input.controllerClient.contract();

  const raw: Record<string, unknown> = {};

  if (typeof contract.getRewardPercent === "function") {
    const rewardPercentRaw = await contract.getRewardPercent(ambassadorWallet).call();
    raw.getRewardPercent = rewardPercentRaw;

    const rewardPercent = parsePercentStrict(
      pickTupleValue(rewardPercentRaw, 0),
      "getRewardPercent"
    );

    let effectiveLevel: number | null = null;

    if (typeof contract.getEffectiveLevel === "function") {
      try {
        const effectiveLevelRaw = await contract.getEffectiveLevel(ambassadorWallet).call();
        raw.getEffectiveLevel = effectiveLevelRaw;
        effectiveLevel = safeNumber(pickTupleValue(effectiveLevelRaw, 0), 0);
      } catch (error) {
        raw.getEffectiveLevelError =
          error instanceof Error ? error.message : String(error);
      }
    }

    return {
      rewardPercent,
      effectiveLevel,
      raw
    };
  }

  if (typeof contract.getDashboardCore === "function") {
    const coreRaw = await contract.getDashboardCore(ambassadorWallet).call();
    raw.getDashboardCore = coreRaw;

    const rewardPercent = parsePercentStrict(
      pickTupleValue(coreRaw, 3, "rewardPercent"),
      "getDashboardCore.rewardPercent"
    );

    const effectiveLevel = safeNumber(
      pickTupleValue(coreRaw, 2, "effectiveLevel"),
      0
    );

    return {
      rewardPercent,
      effectiveLevel,
      raw
    };
  }

  throw new Error("Unable to read ambassador reward percent from controller");
}

export class AttributionProcessor {
  private readonly attributionService: AttributionService;
  private readonly allocationService: AllocationService;
  private readonly store: PurchaseStore;
  private readonly controllerClient: ControllerClient;
  private readonly logger?: ProcessAttributionConfig["logger"];

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

    this.attributionService = config.attributionService;
    this.allocationService = config.allocationService;
    this.store = config.store;
    this.controllerClient = config.controllerClient;
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

  async processVerifiedPurchaseAndAllocate(
    input: ProcessVerifiedPurchaseAndAllocateInput
  ): Promise<ProcessVerifiedPurchaseAndAllocateResult> {
    const txHash = assertNonEmpty(input.txHash, "txHash");
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

    let verifiedPurchase = verification.purchase;

    if (verification.canAllocate && verifiedPurchase.ambassadorWallet) {
      const rewardData = await readAmbassadorRewardData({
        controllerClient: this.controllerClient,
        ambassadorWallet: verifiedPurchase.ambassadorWallet
      });

      const ambassadorRewardSun = dividePercentFloor(
        ownerShareSun,
        rewardData.rewardPercent
      );
      const ownerPayoutSun = subtractSun(ownerShareSun, ambassadorRewardSun);

      verifiedPurchase = await this.store.markVerifiedPurchase({
        purchaseId: verifiedPurchase.purchaseId,
        txHash,
        buyerWallet,
        purchaseAmountSun,
        ownerShareSun,
        ambassadorRewardSun,
        ownerPayoutSun,
        now
      });

      this.logger?.info?.({
        scope: "allocation",
        stage: "reward-share-calculated",
        purchaseId: verifiedPurchase.purchaseId,
        txHash,
        ambassadorWallet: verifiedPurchase.ambassadorWallet,
        ownerShareSun,
        rewardPercent: rewardData.rewardPercent,
        effectiveLevel: rewardData.effectiveLevel,
        ambassadorRewardSun,
        ownerPayoutSun,
        rawRewardData: rewardData.raw
      });
    }

    const patchedVerification: PrepareVerifiedPurchaseResult = {
      ...verification,
      purchase: verifiedPurchase,
      ambassadorWallet: verifiedPurchase.ambassadorWallet
    };

    if (!patchedVerification.canAllocate) {
      return {
        stage: "verified-purchase",
        purchaseId,
        attribution,
        verification: patchedVerification,
        allocation: null
      };
    }

    const allocation = await this.allocationService.executeAllocation({
      purchaseId,
      feeLimitSun: input.feeLimitSun,
      allocationMode,
      now
    });

    return {
      stage: "verified-purchase",
      purchaseId,
      attribution,
      verification: patchedVerification,
      allocation
    };
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<AllocationDecision> {
    const replayResult = await this.allocationService.replayFailedAllocation(
      assertNonEmpty(purchaseId, "purchaseId"),
      feeLimitSun,
      now
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
