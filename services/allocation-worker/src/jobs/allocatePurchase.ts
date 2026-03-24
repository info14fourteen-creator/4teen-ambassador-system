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
