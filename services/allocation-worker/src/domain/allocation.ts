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

const RETRY_DELAY_MS = 60 * 60 * 1000;

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function isAllocatableStatus(status: PurchaseRecord["status"]): boolean {
  return status === "verified" || status === "received" || status === "failed";
}

function toErrorMessage(error: unknown): string {
  if (typeof error === "string" && error.trim()) {
    return error.trim();
  }

  if (
    error &&
    typeof error === "object" &&
    "message" in error &&
    typeof (error as { message?: unknown }).message === "string"
  ) {
    const message = (error as { message: string }).message.trim();
    if (message) {
      return message;
    }
  }

  return "Allocation failed";
}

function isRetryableInfrastructureFailure(message: string): boolean {
  const normalized = message.toLowerCase();

  return (
    normalized.includes("status code 429") ||
    normalized.includes("too many requests") ||
    normalized.includes("out_of_energy") ||
    normalized.includes("out of energy") ||
    normalized.includes("server is busy") ||
    normalized.includes("timeout") ||
    normalized.includes("etimedout") ||
    normalized.includes("econnreset") ||
    normalized.includes("failed to fetch") ||
    normalized.includes("temporary") ||
    normalized.includes("node busy") ||
    normalized.includes("bandwidth")
  );
}

function isAlreadyProcessedFailure(message: string): boolean {
  const normalized = message.toLowerCase();

  return (
    normalized.includes("already processed") ||
    normalized.includes("purchase already processed") ||
    normalized.includes("already allocated") ||
    normalized.includes("duplicate purchase")
  );
}

function withRetryAfter(reason: string, retryAt: number): string {
  return `[RETRY_AFTER=${retryAt}] ${reason}`;
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

    const ambassadorWallet = purchase.ambassadorWallet || null;

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
      const message = toErrorMessage(error);

      if (isAlreadyProcessedFailure(message)) {
        const ignoredPurchase = await this.store.markIgnored(
          purchase.purchaseId,
          "Purchase already processed on-chain",
          now
        );

        return {
          status: "already-processed-on-chain",
          purchase: ignoredPurchase,
          ambassadorWallet,
          txid: null,
          reason: "Purchase already processed on-chain"
        };
      }

      if (isRetryableInfrastructureFailure(message)) {
        const retryAt = now + RETRY_DELAY_MS;

        const failedPurchase = await this.store.markFailed(
          purchase.purchaseId,
          withRetryAfter(message, retryAt),
          now
        );

        return {
          status: "failed",
          purchase: failedPurchase,
          ambassadorWallet,
          txid: null,
          reason: `Retry scheduled after ${new Date(retryAt).toISOString()}`
        };
      }

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
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");
    const purchase = await this.store.getByPurchaseId(normalizedPurchaseId);

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

    const updateNow = now ?? Date.now();

    await this.store.update(purchase.purchaseId, {
      status: "verified",
      failureReason: null,
      now: updateNow
    });

    const executeInput: ExecuteAllocationInput = {
      purchaseId: purchase.purchaseId
    };

    if (feeLimitSun !== undefined) {
      executeInput.feeLimitSun = feeLimitSun;
    }

    if (now !== undefined) {
      executeInput.now = now;
    }

    return this.executeAllocation(executeInput);
  }
}
