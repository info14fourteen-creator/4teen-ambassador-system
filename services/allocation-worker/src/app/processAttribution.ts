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

export interface ProcessAttributionConfig {
  attributionService: AttributionService;
  allocationService: AllocationService;
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

export class AttributionProcessor {
  private readonly attributionService: AttributionService;
  private readonly allocationService: AllocationService;

  constructor(config: ProcessAttributionConfig) {
    if (!config?.attributionService) {
      throw new Error("attributionService is required");
    }

    if (!config?.allocationService) {
      throw new Error("allocationService is required");
    }

    this.attributionService = config.attributionService;
    this.allocationService = config.allocationService;
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
    const purchaseAmountSun = assertNonEmpty(input.purchaseAmountSun, "purchaseAmountSun");
    const ownerShareSun = assertNonEmpty(input.ownerShareSun, "ownerShareSun");
    const now = input.now ?? Date.now();

    let attribution: AttributionDecision | null = null;

    try {
      attribution = await this.attributionService.captureFrontendAttribution({
        txHash,
        buyerWallet,
        slug,
        now
      });
    } catch (error) {
      throw new Error(getReadableErrorMessage(error, "Failed to capture frontend attribution"));
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

    const allocation = await this.allocationService.executeAllocation({
      purchaseId,
      feeLimitSun: input.feeLimitSun,
      allocationMode: "eager",
      now
    });

    return {
      stage: "verified-purchase",
      purchaseId,
      attribution,
      verification,
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
