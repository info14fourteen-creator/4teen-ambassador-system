import { PurchaseRecord, PurchaseStore } from "../db/purchases";
import { ControllerClient } from "../tron/controller";

export type AllocationDecisionStatus =
  | "allocated"
  | "already-processed-on-chain"
  | "missing-purchase"
  | "invalid-purchase-status"
  | "missing-ambassador"
  | "failed";

export interface AllocationDecision {
  status: AllocationDecisionStatus;
  purchase: PurchaseRecord | null;
  ambassadorWallet: string | null;
  txid: string | null;
  reason: string | null;
}

export interface ExecuteAllocationInput {
  purchaseId: string;
  feeLimitSun?: number;
  now?: number;
}

export interface AllocationServiceConfig {
  store: PurchaseStore;
  controllerClient: ControllerClient;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function isAllocatableStatus(status: PurchaseRecord["status"]): boolean {
  return status === "verified" || status === "received";
}

export class AllocationService {
  private readonly store: PurchaseStore;
  private readonly controllerClient: ControllerClient;

  constructor(config: AllocationServiceConfig) {
    if (!config?.store) {
      throw new Error("store is required");
    }

    if (!config?.controllerClient) {
      throw new Error("controllerClient is required");
    }

    this.store = config.store;
    this.controllerClient = config.controllerClient;
  }

  async executeAllocation(input: ExecuteAllocationInput): Promise<AllocationDecision> {
    const purchaseId = assertNonEmpty(input.purchaseId, "purchaseId");
    const now = input.now ?? Date.now();

    const purchase = await this.store.getByPurchaseId(purchaseId);

    if (!purchase) {
      return {
        status: "missing-purchase",
        purchase: null,
        ambassadorWallet: null,
        txid: null,
        reason: "Purchase not found in local store"
      };
    }

    if (!isAllocatableStatus(purchase.status)) {
      return {
        status: "invalid-purchase-status",
        purchase,
        ambassadorWallet: purchase.ambassadorWallet,
        txid: null,
        reason: `Purchase status is not allocatable: ${purchase.status}`
      };
    }

    const onChainProcessed = await this.controllerClient.isPurchaseProcessed(purchase.purchaseId);

    if (onChainProcessed) {
      const ignoredPurchase = await this.store.markIgnored(
        purchase.purchaseId,
        "Purchase already processed on-chain",
        now
      );

      return {
        status: "already-processed-on-chain",
        purchase: ignoredPurchase,
        ambassadorWallet: ignoredPurchase.ambassadorWallet,
        txid: null,
        reason: "Purchase already processed on-chain"
      };
    }

    const alreadyBoundAmbassador = await this.controllerClient.getBuyerAmbassador(
      purchase.buyerWallet
    );

    const ambassadorWallet =
      alreadyBoundAmbassador || purchase.ambassadorWallet || null;

    if (!ambassadorWallet) {
      const failedPurchase = await this.store.markFailed(
        purchase.purchaseId,
        "Ambassador wallet is missing for allocation",
        now
      );

      return {
        status: "missing-ambassador",
        purchase: failedPurchase,
        ambassadorWallet: null,
        txid: null,
        reason: "Ambassador wallet is missing for allocation"
      };
    }

    try {
      const result = await this.controllerClient.recordVerifiedPurchase({
        purchaseId: purchase.purchaseId,
        buyerWallet: purchase.buyerWallet,
        ambassadorWallet,
        purchaseAmountSun: purchase.purchaseAmountSun,
        ownerShareSun: purchase.ownerShareSun,
        feeLimitSun: input.feeLimitSun
      });

      const allocatedPurchase = await this.store.markAllocated(purchase.purchaseId, {
        ambassadorWallet,
        now
      });

      return {
        status: "allocated",
        purchase: allocatedPurchase,
        ambassadorWallet,
        txid: result.txid,
        reason: null
      };
    } catch (error) {
      const message =
        error && typeof error === "object" && "message" in error
          ? String((error as { message?: unknown }).message || "").trim() || "Allocation failed"
          : typeof error === "string" && error.trim()
            ? error.trim()
            : "Allocation failed";

      const failedPurchase = await this.store.markFailed(
        purchase.purchaseId,
        message,
        now
      );

      return {
        status: "failed",
        purchase: failedPurchase,
        ambassadorWallet,
        txid: null,
        reason: message
      };
    }
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<AllocationDecision> {
    const purchase = await this.store.getByPurchaseId(assertNonEmpty(purchaseId, "purchaseId"));

    if (!purchase) {
      return {
        status: "missing-purchase",
        purchase: null,
        ambassadorWallet: null,
        txid: null,
        reason: "Purchase not found in local store"
      };
    }

    if (purchase.status !== "failed") {
      return {
        status: "invalid-purchase-status",
        purchase,
        ambassadorWallet: purchase.ambassadorWallet,
        txid: null,
        reason: `Replay is allowed only for failed purchases, got: ${purchase.status}`
      };
    }

    await this.store.update(purchase.purchaseId, {
      status: "verified",
      failureReason: null,
      now
    });

    return this.executeAllocation({
      purchaseId: purchase.purchaseId,
      feeLimitSun,
      now
    });
  }
}
