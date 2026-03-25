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

function normalizeOptionalWallet(value: string | null | undefined): string | null {
  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized || null;
}

function isAllocatableStatus(status: PurchaseRecord["status"]): boolean {
  return status === "verified" || status === "received";
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

function readResultStatus(value: unknown): string {
  if (!value || typeof value !== "object") {
    return "";
  }

  if ("status" in value && typeof (value as { status?: unknown }).status === "string") {
    return String((value as { status: string }).status).trim();
  }

  return "";
}

function readResultReason(value: unknown): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  if ("reason" in value && typeof (value as { reason?: unknown }).reason === "string") {
    const reason = String((value as { reason: string }).reason).trim();
    return reason || null;
  }

  if ("message" in value && typeof (value as { message?: unknown }).message === "string") {
    const message = String((value as { message: string }).message).trim();
    return message || null;
  }

  return null;
}

function readResultTxid(value: unknown): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const candidateKeys = ["txid", "txId", "transactionId", "hash"];

  for (const key of candidateKeys) {
    if (key in value && typeof (value as Record<string, unknown>)[key] === "string") {
      const normalized = String((value as Record<string, unknown>)[key]).trim();

      if (normalized) {
        return normalized;
      }
    }
  }

  return null;
}

function readAmbassadorWalletFromLookup(value: unknown): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const candidateKeys = [
    "ambassadorWallet",
    "wallet",
    "boundAmbassadorWallet",
    "address"
  ];

  for (const key of candidateKeys) {
    if (key in value && typeof (value as Record<string, unknown>)[key] === "string") {
      const normalized = String((value as Record<string, unknown>)[key]).trim();

      if (normalized) {
        return normalized;
      }
    }
  }

  return null;
}

function isLookupMissing(value: unknown): boolean {
  const status = readResultStatus(value).toLowerCase();

  if (
    status === "not-found" ||
    status === "missing" ||
    status === "unbound" ||
    status === "none"
  ) {
    return true;
  }

  const wallet = readAmbassadorWalletFromLookup(value);
  return !wallet;
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

  async executeAllocation(
    input: ExecuteAllocationInput
  ): Promise<AllocationDecision> {
    const purchaseId = assertNonEmpty(input.purchaseId, "purchaseId");
    const now = input.now ?? Date.now();

    const purchase = await this.store.getByPurchaseId(purchaseId);

    if (!purchase) {
      return {
        status: "missing-purchase",
        purchase: null,
        ambassadorWallet: null,
        txid: null,
        reason: "Purchase record not found"
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

    let ambassadorWallet = normalizeOptionalWallet(purchase.ambassadorWallet);

    /**
     * Primary source of truth for allocation is the verified local purchase record.
     * We only hit the controller read endpoint if the local record still has no
     * ambassador wallet. This avoids unnecessary TronGrid constant-call traffic
     * and prevents 429 failures during scan replay.
     */
    if (!ambassadorWallet) {
      try {
        const lookupResult = await (this.controllerClient as any).getBuyerAmbassador(
          purchase.buyerWallet
        );

        if (!isLookupMissing(lookupResult)) {
          ambassadorWallet = readAmbassadorWalletFromLookup(lookupResult);
        }
      } catch (error) {
        const reason = toErrorMessage(error);

        await this.store.markFailed(purchase.purchaseId, reason, now);

        return {
          status: "failed",
          purchase: await this.store.getByPurchaseId(purchase.purchaseId),
          ambassadorWallet: null,
          txid: null,
          reason
        };
      }
    }

    if (!ambassadorWallet) {
      await this.store.markFailed(
        purchase.purchaseId,
        "Ambassador wallet is missing for verified purchase",
        now
      );

      return {
        status: "missing-ambassador",
        purchase: await this.store.getByPurchaseId(purchase.purchaseId),
        ambassadorWallet: null,
        txid: null,
        reason: "Ambassador wallet is missing for verified purchase"
      };
    }

    if (purchase.ambassadorWallet !== ambassadorWallet) {
      await this.store.update(purchase.purchaseId, {
        ambassadorWallet,
        now
      });
    }

    try {
      const recordInput: any = {
        purchaseId: purchase.purchaseId,
        txHash: purchase.txHash,
        buyerWallet: purchase.buyerWallet,
        purchaseAmountSun: purchase.purchaseAmountSun,
        ownerShareSun: purchase.ownerShareSun,
        ambassadorWallet
      };

      if (input.feeLimitSun !== undefined) {
        recordInput.feeLimitSun = input.feeLimitSun;
      }

      const result = await (this.controllerClient as any).recordVerifiedPurchase(recordInput);
      const resultStatus = readResultStatus(result).toLowerCase();
      const txid = readResultTxid(result);
      const reason = readResultReason(result);

      if (resultStatus === "already-processed-on-chain") {
        await this.store.markAllocated(purchase.purchaseId, {
          ambassadorWallet,
          now
        });

        return {
          status: "already-processed-on-chain",
          purchase: await this.store.getByPurchaseId(purchase.purchaseId),
          ambassadorWallet,
          txid,
          reason: reason ?? "Purchase was already processed on-chain"
        };
      }

      if (resultStatus && resultStatus !== "allocated" && resultStatus !== "success") {
        const failureReason = reason ?? `Unexpected controller result status: ${resultStatus}`;

        await this.store.markFailed(purchase.purchaseId, failureReason, now);

        return {
          status: "failed",
          purchase: await this.store.getByPurchaseId(purchase.purchaseId),
          ambassadorWallet,
          txid,
          reason: failureReason
        };
      }

      await this.store.markAllocated(purchase.purchaseId, {
        ambassadorWallet,
        now
      });

      return {
        status: "allocated",
        purchase: await this.store.getByPurchaseId(purchase.purchaseId),
        ambassadorWallet,
        txid,
        reason: null
      };
    } catch (error) {
      const reason = toErrorMessage(error);

      await this.store.markFailed(purchase.purchaseId, reason, now);

      return {
        status: "failed",
        purchase: await this.store.getByPurchaseId(purchase.purchaseId),
        ambassadorWallet,
        txid: null,
        reason
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
        reason: "Purchase record not found"
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
