import { getStoredReferral } from "../referral/storage";
import { submitAttribution, SubmitAttributionOptions, SubmitAttributionResponse } from "./submitAttribution";

export interface AfterBuyInput {
  txHash: string;
  buyerWallet: string;
}

export interface AfterBuyOptions extends SubmitAttributionOptions {
  now?: number;
}

export interface AfterBuyResult<T = unknown> {
  status: "submitted" | "skipped-no-referral";
  referralSlug: string | null;
  response: SubmitAttributionResponse<T> | null;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

export async function handleAfterBuy<T = unknown>(
  input: AfterBuyInput,
  options: AfterBuyOptions
): Promise<AfterBuyResult<T>> {
  const txHash = assertNonEmpty(input.txHash, "txHash");
  const buyerWallet = assertNonEmpty(input.buyerWallet, "buyerWallet");
  const now = options.now ?? Date.now();

  const referral = getStoredReferral(now);

  if (!referral) {
    return {
      status: "skipped-no-referral",
      referralSlug: null,
      response: null
    };
  }

  const response = await submitAttribution<T>(
    {
      txHash,
      buyerWallet,
      slug: referral.slug
    },
    options
  );

  return {
    status: "submitted",
    referralSlug: referral.slug,
    response
  };
}
