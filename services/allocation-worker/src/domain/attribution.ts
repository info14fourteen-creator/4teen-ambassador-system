import { assertValidSlug, normalizeSlug } from "../../../../shared/utils/slug";
import { query } from "../db/postgres";
import { PurchaseRecord, PurchaseStore } from "../db/purchases";
import { ControllerClient } from "../tron/controller";

export interface FrontendAttributionInput {
  txHash: string;
  buyerWallet: string;
  slug: string;
  now?: number;
}

export interface VerifiedPurchaseInput {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  slug: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  now?: number;
}

export interface AttributionHashing {
  hashSlugToBytes32Hex(slug: string): string;
  derivePurchaseId(input: {
    txHash: string;
    buyerWallet: string;
  }): string;
}

export type AttributionDecisionStatus =
  | "ready-for-allocation"
  | "already-processed-on-chain"
  | "duplicate-local-record"
  | "ambassador-not-found"
  | "binding-not-allowed"
  | "ignored";

export interface AttributionDecision {
  status: AttributionDecisionStatus;
  purchase: PurchaseRecord;
  slug: string;
  slugHash: string;
  ambassadorWallet: string | null;
  reason: string | null;
}

export interface PrepareVerifiedPurchaseResult extends AttributionDecision {
  canAllocate: boolean;
}

export interface AttributionServiceConfig {
  store: PurchaseStore;
  controllerClient: ControllerClient;
  hashing: AttributionHashing;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeWallet(value: string, fieldName: string): string {
  return assertNonEmpty(value, fieldName);
}

function normalizeTxHash(value: string): string {
  return assertNonEmpty(value, "txHash").toLowerCase();
}

function normalizeSunAmount(value: string | number | bigint, fieldName: string): string {
  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${fieldName} must be a non-negative integer string`);
  }

  return normalized;
}

function normalizeIncomingSlug(value: string): string {
  return assertValidSlug(normalizeSlug(value));
}

async function getLocalAmbassadorWalletBySlug(slug: string): Promise<string | null> {
  const normalizedSlug = normalizeIncomingSlug(slug);

  const result = await query<{ wallet: string }>(
    `
      SELECT i.wallet
      FROM ambassador_public_profiles p
      INNER JOIN ambassador_private_identities i
        ON i.ambassador_id = p.id
      WHERE p.slug = $1
      LIMIT 1
    `,
    [normalizedSlug]
  );

  const wallet = String(result.rows[0]?.wallet || "").trim();
  return wallet || null;
}

async function resolveAmbassadorWallet(params: {
  slug: string;
  slugHash: string;
  existingPurchase?: PurchaseRecord | null;
  controllerClient: ControllerClient;
}): Promise<{ ambassadorWallet: string | null; source: "existing" | "local" | "chain" | "none" }> {
  const existingWallet = String(params.existingPurchase?.ambassadorWallet || "").trim();

  if (existingWallet) {
    return {
      ambassadorWallet: existingWallet,
      source: "existing"
    };
  }

  const localWallet = await getLocalAmbassadorWalletBySlug(params.slug);

  if (localWallet) {
    return {
      ambassadorWallet: localWallet,
      source: "local"
    };
  }

  const chainResolved = await params.controllerClient.getAmbassadorBySlugHash(params.slugHash);

  if (chainResolved.ambassadorWallet) {
    return {
      ambassadorWallet: chainResolved.ambassadorWallet,
      source: "chain"
    };
  }

  return {
    ambassadorWallet: null,
    source: "none"
  };
}

export class AttributionService {
  private readonly store: PurchaseStore;
  private readonly controllerClient: ControllerClient;
  private readonly hashing: AttributionHashing;

  constructor(config: AttributionServiceConfig) {
    if (!config?.store) {
      throw new Error("store is required");
    }

    if (!config?.controllerClient) {
      throw new Error("controllerClient is required");
    }

    if (!config?.hashing) {
      throw new Error("hashing is required");
    }

    this.store = config.store;
    this.controllerClient = config.controllerClient;
    this.hashing = config.hashing;
  }

  async captureFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<AttributionDecision> {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const slug = normalizeIncomingSlug(input.slug);
    const now = input.now ?? Date.now();

    const purchaseId = this.hashing.derivePurchaseId({
      txHash,
      buyerWallet
    });

    const slugHash = this.hashing.hashSlugToBytes32Hex(slug);

    const existingByPurchaseId = await this.store.getByPurchaseId(purchaseId);
    if (existingByPurchaseId) {
      return {
        status: "duplicate-local-record",
        purchase: existingByPurchaseId,
        slug,
        slugHash,
        ambassadorWallet: existingByPurchaseId.ambassadorWallet,
        reason: "Purchase already exists in local store"
      };
    }

    const existingByTxHash = await this.store.getByTxHash(txHash);
    if (existingByTxHash) {
      return {
        status: "duplicate-local-record",
        purchase: existingByTxHash,
        slug,
        slugHash,
        ambassadorWallet: existingByTxHash.ambassadorWallet,
        reason: "Transaction already exists in local store"
      };
    }

    const resolved = await resolveAmbassadorWallet({
      slug,
      slugHash,
      controllerClient: this.controllerClient
    });

    if (!resolved.ambassadorWallet) {
      const ignoredPurchase = await this.store.create({
        purchaseId,
        txHash,
        buyerWallet,
        ambassadorSlug: slug,
        ambassadorWallet: null,
        purchaseAmountSun: "0",
        ownerShareSun: "0",
        ambassadorRewardSun: "0",
        ownerPayoutSun: "0",
        source: "frontend-attribution",
        status: "ignored",
        failureReason: "Ambassador wallet not found",
        now
      });

      return {
        status: "ambassador-not-found",
        purchase: ignoredPurchase,
        slug,
        slugHash,
        ambassadorWallet: null,
        reason: "Ambassador wallet not found"
      };
    }

    const existingBuyerAmbassador = await this.controllerClient.getBuyerAmbassador(buyerWallet);

    if (
      existingBuyerAmbassador &&
      existingBuyerAmbassador !== resolved.ambassadorWallet
    ) {
      const ignoredPurchase = await this.store.create({
        purchaseId,
        txHash,
        buyerWallet,
        ambassadorSlug: slug,
        ambassadorWallet: resolved.ambassadorWallet,
        purchaseAmountSun: "0",
        ownerShareSun: "0",
        ambassadorRewardSun: "0",
        ownerPayoutSun: "0",
        source: "frontend-attribution",
        status: "ignored",
        failureReason: "Buyer is already bound to another ambassador on-chain",
        now
      });

      return {
        status: "binding-not-allowed",
        purchase: ignoredPurchase,
        slug,
        slugHash,
        ambassadorWallet: resolved.ambassadorWallet,
        reason: "Buyer is already bound to another ambassador on-chain"
      };
    }

    const purchase = await this.store.create({
      purchaseId,
      txHash,
      buyerWallet,
      ambassadorSlug: slug,
      ambassadorWallet: resolved.ambassadorWallet,
      purchaseAmountSun: "0",
      ownerShareSun: "0",
      ambassadorRewardSun: "0",
      ownerPayoutSun: "0",
      source: "frontend-attribution",
      status: "received",
      failureReason: null,
      now
    });

    return {
      status: "ready-for-allocation",
      purchase,
      slug,
      slugHash,
      ambassadorWallet: resolved.ambassadorWallet,
      reason: null
    };
  }

  async prepareVerifiedPurchase(
    input: VerifiedPurchaseInput
  ): Promise<PrepareVerifiedPurchaseResult> {
    const purchaseId = assertNonEmpty(input.purchaseId, "purchaseId");
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const slug = normalizeIncomingSlug(input.slug);
    const purchaseAmountSun = normalizeSunAmount(input.purchaseAmountSun, "purchaseAmountSun");
    const ownerShareSun = normalizeSunAmount(input.ownerShareSun, "ownerShareSun");
    const now = input.now ?? Date.now();

    const slugHash = this.hashing.hashSlugToBytes32Hex(slug);

    const existing = await this.store.getByPurchaseId(purchaseId);
    if (!existing) {
      throw new Error(`Purchase not found: ${purchaseId}`);
    }

    if (existing.txHash.toLowerCase() !== txHash) {
      throw new Error("Purchase txHash does not match existing record");
    }

    if (existing.buyerWallet !== buyerWallet) {
      throw new Error("Purchase buyerWallet does not match existing record");
    }

    const alreadyProcessedOnChain = await this.controllerClient.isPurchaseProcessed(purchaseId);

    if (alreadyProcessedOnChain) {
      const allocated = await this.store.markAllocated(purchaseId, {
        ambassadorWallet: existing.ambassadorWallet,
        allocationMode: existing.allocationMode ?? "manual-replay",
        now
      });

      return {
        status: "already-processed-on-chain",
        purchase: allocated,
        slug,
        slugHash,
        ambassadorWallet: allocated.ambassadorWallet,
        reason: "Purchase is already processed on-chain",
        canAllocate: false
      };
    }

    const resolved = await resolveAmbassadorWallet({
      slug,
      slugHash,
      existingPurchase: existing,
      controllerClient: this.controllerClient
    });

    if (!resolved.ambassadorWallet) {
      const failed = await this.store.markAllocationFinalFailed(purchaseId, {
        reason: "Ambassador wallet not found",
        allocationMode: existing.allocationMode ?? "manual-replay",
        errorCode: "AMBASSADOR_NOT_FOUND",
        errorMessage: "Ambassador wallet not found",
        now
      });

      return {
        status: "ambassador-not-found",
        purchase: failed,
        slug,
        slugHash,
        ambassadorWallet: null,
        reason: "Ambassador wallet not found",
        canAllocate: false
      };
    }

    const existingBuyerAmbassador = await this.controllerClient.getBuyerAmbassador(buyerWallet);

    if (
      existingBuyerAmbassador &&
      existingBuyerAmbassador !== resolved.ambassadorWallet
    ) {
      const ignored = await this.store.markIgnored(
        purchaseId,
        "Buyer is already bound to another ambassador on-chain",
        now
      );

      return {
        status: "binding-not-allowed",
        purchase: ignored,
        slug,
        slugHash,
        ambassadorWallet: resolved.ambassadorWallet,
        reason: "Buyer is already bound to another ambassador on-chain",
        canAllocate: false
      };
    }

    if (!existingBuyerAmbassador) {
      const canBind = await this.controllerClient.canBindBuyerToAmbassador(
        buyerWallet,
        resolved.ambassadorWallet
      );

      if (!canBind) {
        const ignored = await this.store.markIgnored(
          purchaseId,
          "Binding buyer to ambassador is not allowed by controller",
          now
        );

        return {
          status: "binding-not-allowed",
          purchase: ignored,
          slug,
          slugHash,
          ambassadorWallet: resolved.ambassadorWallet,
          reason: "Binding buyer to ambassador is not allowed by controller",
          canAllocate: false
        };
      }
    }

    const verified = await this.store.markVerified(purchaseId, {
      purchaseAmountSun,
      ownerShareSun,
      ambassadorRewardSun: existing.ambassadorRewardSun,
      ownerPayoutSun: existing.ownerPayoutSun === "0" ? ownerShareSun : existing.ownerPayoutSun,
      ambassadorSlug: slug,
      ambassadorWallet: resolved.ambassadorWallet,
      allocationMode: existing.allocationMode,
      now
    });

    return {
      status: "ready-for-allocation",
      purchase: verified,
      slug,
      slugHash,
      ambassadorWallet: resolved.ambassadorWallet,
      reason: null,
      canAllocate: true
    };
  }

  async markAllocationSuccess(
    purchaseId: string,
    ambassadorWallet?: string | null,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.store.markAllocated(purchaseId, {
      ambassadorWallet: ambassadorWallet ?? undefined,
      now
    });
  }

  async markAllocationFailure(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.store.markFailed(
      assertNonEmpty(purchaseId, "purchaseId"),
      assertNonEmpty(reason, "reason"),
      now
    );
  }
}
