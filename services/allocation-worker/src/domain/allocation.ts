import TronWeb from "tronweb";
import { PurchaseRecord, PurchaseStore } from "../db/purchases";
import { ControllerClient } from "../tron/controller";

export type AllocationDecisionStatus =
  | "allocated"
  | "already-processed-on-chain"
  | "missing-purchase"
  | "invalid-purchase-status"
  | "missing-ambassador"
  | "deferred-insufficient-energy"
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

interface AccountResourceResponse {
  freeNetLimit?: number;
  freeNetUsed?: number;
  NetLimit?: number;
  NetUsed?: number;
  EnergyLimit?: number;
  EnergyUsed?: number;
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

function getTronResourceApiUrl(): string {
  return String(process.env.TRON_RESOURCE_API_URL || "https://api.trongrid.io").replace(/\/+$/, "");
}

function getTronHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json"
  };

  const apiKey = String(process.env.TRONGRID_API_KEY || "").trim();
  if (apiKey) {
    headers["TRON-PRO-API-KEY"] = apiKey;
  }

  return headers;
}

function getResourceAddressFromEnv(): string {
  const candidates = [
    process.env.TRON_RESOURCE_ADDRESS,
    process.env.CONTROLLER_OWNER_WALLET,
    process.env.CONTROLLER_WALLET,
    process.env.OWNER_WALLET
  ];

  for (const candidate of candidates) {
    const normalized = String(candidate || "").trim();
    if (normalized) {
      return normalized;
    }
  }

  throw new Error(
    "Resource wallet is not configured. Set TRON_RESOURCE_ADDRESS or CONTROLLER_OWNER_WALLET"
  );
}

function getRequiredEnergyThreshold(): number {
  const raw = Number(process.env.TRON_MIN_REQUIRED_ENERGY || "185000");
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 185000;
}

function getReserveEnergyThreshold(): number {
  const raw = Number(process.env.TRON_RESERVE_ENERGY || "15000");
  return Number.isFinite(raw) && raw >= 0 ? Math.floor(raw) : 15000;
}

async function getAvailableEnergy(addressBase58: string): Promise<number> {
  const addressHex = TronWeb.address.toHex(addressBase58);
  const url = `${getTronResourceApiUrl()}/wallet/getaccountresource`;

  const response = await fetch(url, {
    method: "POST",
    headers: getTronHeaders(),
    body: JSON.stringify({
      address: addressHex,
      visible: false
    })
  });

  const text = await response.text();
  let data: AccountResourceResponse = {};

  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    throw new Error(`Invalid TRON resource response: ${text || "empty response"}`);
  }

  if (!response.ok) {
    throw new Error(`TRON resource API HTTP ${response.status}`);
  }

  const energyLimit = Number(data.EnergyLimit || 0);
  const energyUsed = Number(data.EnergyUsed || 0);
  const available = Math.max(0, energyLimit - energyUsed);

  return Number.isFinite(available) ? available : 0;
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

    const resourceAddress = getResourceAddressFromEnv();
    const minRequiredEnergy = getRequiredEnergyThreshold();
    const reserveEnergy = getReserveEnergyThreshold();
    const totalRequiredEnergy = minRequiredEnergy + reserveEnergy;
    const availableEnergy = await getAvailableEnergy(resourceAddress);

    if (availableEnergy < totalRequiredEnergy) {
      const deferredPurchase = await this.store.update(purchase.purchaseId, {
        status: "verified",
        failureReason: `Awaiting energy: available=${availableEnergy}, required=${totalRequiredEnergy}`,
        now
      });

      return {
        status: "deferred-insufficient-energy",
        purchase: deferredPurchase,
        ambassadorWallet: deferredPurchase.ambassadorWallet,
        txid: null,
        reason: `Insufficient energy on ${resourceAddress}: available ${availableEnergy}, required ${totalRequiredEnergy}`
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

    const ambassadorWallet = alreadyBoundAmbassador || purchase.ambassadorWallet || null;

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
      const txid = await this.controllerClient.allocatePurchase({
        purchaseId: purchase.purchaseId,
        buyerWallet: purchase.buyerWallet,
        ambassadorWallet,
        amountSun: purchase.ownerShareSun,
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
        txid,
        reason: null
      };
    } catch (error) {
      const message = toErrorMessage(error);

      const lowered = message.toLowerCase();
      const looksLikeEnergyProblem =
        lowered.includes("out_of_energy") ||
        lowered.includes("insufficient energy") ||
        lowered.includes("energy");

      if (looksLikeEnergyProblem) {
        const deferredPurchase = await this.store.update(purchase.purchaseId, {
          status: "verified",
          failureReason: message,
          now
        });

        return {
          status: "deferred-insufficient-energy",
          purchase: deferredPurchase,
          ambassadorWallet,
          txid: null,
          reason: message
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
