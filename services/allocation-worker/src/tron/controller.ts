import { FOURTEEN_CONTROLLER_CONTRACT } from "../../../../shared/config/contracts";
import type {
  AllocationExecutor,
  AllocationExecutorInput,
  AllocationExecutorResult
} from "../domain/allocation";
import type { GasStationClient } from "../services/gasStation";

export interface ControllerClientConfig {
  tronWeb: any;
  contractAddress?: string;
  gasStation?: GasStationClient | null;
  logger?: {
    info?(payload: Record<string, unknown>): void;
    warn?(payload: Record<string, unknown>): void;
    error?(payload: Record<string, unknown>): void;
  };
}

export interface TronControllerAllocationExecutorConfig {
  tronWeb: any;
  controllerContractAddress?: string;
  gasStation?: GasStationClient | null;
  logger?: {
    info?(payload: Record<string, unknown>): void;
    warn?(payload: Record<string, unknown>): void;
    error?(payload: Record<string, unknown>): void;
  };
}

export interface ResolveAmbassadorBySlugHashResult {
  slugHash: string;
  ambassadorWallet: string | null;
}

export interface RecordVerifiedPurchaseInput {
  purchaseId: string;
  buyerWallet: string;
  ambassadorWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  feeLimitSun?: number;
}

export interface RecordVerifiedPurchaseResult {
  txid: string;
}

export interface ControllerClient {
  getAmbassadorBySlugHash(slugHash: string): Promise<ResolveAmbassadorBySlugHashResult>;
  getBuyerAmbassador(buyerWallet: string): Promise<string | null>;
  isPurchaseProcessed(purchaseId: string): Promise<boolean>;
  canBindBuyerToAmbassador(buyerWallet: string, ambassadorWallet: string): Promise<boolean>;
  recordVerifiedPurchase(input: RecordVerifiedPurchaseInput): Promise<RecordVerifiedPurchaseResult>;
}

const TRON_HEX_ZERO_ADDRESS = "410000000000000000000000000000000000000000";

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

function normalizeBytes32Hex(value: string, fieldName: string): string {
  const normalized = assertNonEmpty(value, fieldName).toLowerCase();

  if (!/^0x[0-9a-f]{64}$/.test(normalized)) {
    throw new Error(`${fieldName} must be a 32-byte hex string`);
  }

  return normalized;
}

function normalizeFeeLimitSun(value: number | undefined): number {
  const resolved = value ?? 300_000_000;

  if (!Number.isInteger(resolved) || resolved <= 0) {
    throw new Error("feeLimitSun must be a positive integer");
  }

  return resolved;
}

function isHexAddress(value: string): boolean {
  return /^41[0-9a-fA-F]{40}$/.test(value);
}

function isBase58Address(value: string): boolean {
  return /^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(value);
}

function normalizeAddress(value: string, fieldName: string): string {
  const normalized = assertNonEmpty(value, fieldName);

  if (!isBase58Address(normalized) && !isHexAddress(normalized)) {
    throw new Error(`${fieldName} must be a valid TRON address`);
  }

  return normalized;
}

function isZeroHexAddress(value: string): boolean {
  return value.toLowerCase() === TRON_HEX_ZERO_ADDRESS.toLowerCase();
}

function normalizeReturnedAddress(tronWeb: any, value: unknown): string | null {
  const raw = String(value || "").trim();

  if (!raw) {
    return null;
  }

  if (isHexAddress(raw)) {
    if (isZeroHexAddress(raw)) {
      return null;
    }

    if (tronWeb?.address?.fromHex) {
      return tronWeb.address.fromHex(raw);
    }

    return raw;
  }

  if (isBase58Address(raw)) {
    return raw;
  }

  return raw || null;
}

function normalizeTxid(value: unknown): string {
  const raw = String(value || "").trim();

  if (!raw) {
    return "";
  }

  if (typeof value === "string") {
    return raw;
  }

  return raw;
}

function extractTxid(result: unknown): string {
  if (!result) return "";

  if (typeof result === "string") {
    return result.trim();
  }

  if (typeof result === "object") {
    const candidate =
      (result as any).txid ||
      (result as any).txID ||
      (result as any).transaction?.txID ||
      (result as any).transaction?.txid ||
      (result as any).result?.txid ||
      (result as any).result?.txID ||
      "";

    return String(candidate || "").trim();
  }

  return "";
}

function isResourceError(error: unknown): boolean {
  const text = String(
    (error as any)?.message ||
      (error as any)?.error ||
      (error as any)?.data?.message ||
      (error as any)?.response?.data?.message ||
      (error as any)?.response?.message ||
      ""
  ).toLowerCase();

  return (
    text.includes("out_of_energy") ||
    text.includes("out of energy") ||
    text.includes("bandwidth") ||
    text.includes("net_usage") ||
    text.includes("resource") ||
    text.includes("contract validate error") ||
    text.includes("account resource insufficient")
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getContract(tronWeb: any, contractAddress: string): Promise<any> {
  if (!tronWeb || typeof tronWeb.contract !== "function") {
    throw new Error("Valid tronWeb instance is required");
  }

  return await tronWeb.contract().at(contractAddress);
}

export class TronControllerClient implements ControllerClient {
  private readonly tronWeb: any;
  private readonly contractAddress: string;
  private readonly gasStation: GasStationClient | null;
  private readonly logger?: {
    info?(payload: Record<string, unknown>): void;
    warn?(payload: Record<string, unknown>): void;
    error?(payload: Record<string, unknown>): void;
  };

  private contractInstance: any | null = null;

  constructor(config: ControllerClientConfig) {
    if (!config?.tronWeb) {
      throw new Error("tronWeb is required");
    }

    this.tronWeb = config.tronWeb;
    this.contractAddress = normalizeAddress(
      config.contractAddress ?? FOURTEEN_CONTROLLER_CONTRACT,
      "contractAddress"
    );
    this.gasStation = config.gasStation ?? null;
    this.logger = config.logger;
  }

  private async contract(): Promise<any> {
    if (!this.contractInstance) {
      this.contractInstance = await getContract(this.tronWeb, this.contractAddress);
    }

    return this.contractInstance;
  }

  private getSignerAddress(): string {
    const address =
      this.tronWeb?.defaultAddress?.base58 ||
      this.tronWeb?.address?.fromPrivateKey?.(this.tronWeb?.defaultPrivateKey || "");

    return normalizeAddress(address, "signerAddress");
  }

  private async sendRecordVerifiedPurchase(
    input: RecordVerifiedPurchaseInput
  ): Promise<RecordVerifiedPurchaseResult> {
    const purchaseId = normalizeBytes32Hex(input.purchaseId, "purchaseId");
    const buyerWallet = normalizeAddress(input.buyerWallet, "buyerWallet");
    const ambassadorWallet = normalizeAddress(input.ambassadorWallet, "ambassadorWallet");
    const purchaseAmountSun = normalizeSunAmount(input.purchaseAmountSun, "purchaseAmountSun");
    const ownerShareSun = normalizeSunAmount(input.ownerShareSun, "ownerShareSun");
    const feeLimitSun = normalizeFeeLimitSun(input.feeLimitSun);

    const contract = await this.contract();

    const result = await contract
      .recordVerifiedPurchase(
        purchaseId,
        buyerWallet,
        ambassadorWallet,
        purchaseAmountSun,
        ownerShareSun
      )
      .send({
        feeLimit: feeLimitSun
      });

    const txid = extractTxid(result);

    return {
      txid: assertNonEmpty(txid, "txid")
    };
  }

  private async ensureGasStationResources(
    input: RecordVerifiedPurchaseInput
  ): Promise<void> {
    if (!this.gasStation) {
      throw new Error("GasStation client is not configured");
    }

    const signerAddress = this.getSignerAddress();

    this.logger?.info?.({
      scope: "allocation",
      step: "gasstation-estimate-start",
      signerAddress,
      controllerContractAddress: this.contractAddress,
      purchaseId: input.purchaseId
    });

    const estimate = await this.gasStation.estimateEnergyOrder({
      receiveAddress: signerAddress,
      addressTo: this.contractAddress,
      contractAddress: this.contractAddress
    });

    const estimatedEnergy = Number(estimate.energy_num || 0);

    if (!Number.isFinite(estimatedEnergy) || estimatedEnergy < 64400) {
      throw new Error("GasStation returned invalid energy estimate");
    }

    const requestId = `alloc-${String(input.purchaseId).replace(/^0x/, "").slice(0, 24)}-${Date.now()}`;

    this.logger?.info?.({
      scope: "allocation",
      step: "gasstation-create-order-start",
      signerAddress,
      controllerContractAddress: this.contractAddress,
      purchaseId: input.purchaseId,
      estimatedEnergy
    });

    const order = await this.gasStation.createEnergyOrder({
      requestId,
      receiveAddress: signerAddress,
      energyNum: estimatedEnergy,
      serviceChargeType: estimate.service_charge_type || "10010"
    });

    this.logger?.info?.({
      scope: "allocation",
      step: "gasstation-create-order-success",
      purchaseId: input.purchaseId,
      tradeNo: order.trade_no,
      estimatedEnergy
    });

    await sleep(5000);
  }

  async getAmbassadorBySlugHash(slugHash: string): Promise<ResolveAmbassadorBySlugHashResult> {
    const normalizedSlugHash = normalizeBytes32Hex(slugHash, "slugHash");
    const contract = await this.contract();

    const result = await contract.getAmbassadorBySlugHash(normalizedSlugHash).call();
    const ambassadorWallet = normalizeReturnedAddress(this.tronWeb, result);

    return {
      slugHash: normalizedSlugHash,
      ambassadorWallet
    };
  }

  async getBuyerAmbassador(buyerWallet: string): Promise<string | null> {
    const normalizedBuyerWallet = normalizeAddress(buyerWallet, "buyerWallet");
    const contract = await this.contract();

    const result = await contract.getBuyerAmbassador(normalizedBuyerWallet).call();
    return normalizeReturnedAddress(this.tronWeb, result);
  }

  async isPurchaseProcessed(purchaseId: string): Promise<boolean> {
    const normalizedPurchaseId = normalizeBytes32Hex(purchaseId, "purchaseId");
    const contract = await this.contract();

    const result = await contract.isPurchaseProcessed(normalizedPurchaseId).call();
    return Boolean(result);
  }

  async canBindBuyerToAmbassador(
    buyerWallet: string,
    ambassadorWallet: string
  ): Promise<boolean> {
    const normalizedBuyerWallet = normalizeAddress(buyerWallet, "buyerWallet");
    const normalizedAmbassadorWallet = normalizeAddress(ambassadorWallet, "ambassadorWallet");
    const contract = await this.contract();

    const result = await contract
      .canBindBuyerToAmbassador(normalizedBuyerWallet, normalizedAmbassadorWallet)
      .call();

    return Boolean(result);
  }

  async recordVerifiedPurchase(
    input: RecordVerifiedPurchaseInput
  ): Promise<RecordVerifiedPurchaseResult> {
    try {
      return await this.sendRecordVerifiedPurchase(input);
    } catch (error) {
      if (!isResourceError(error) || !this.gasStation) {
        throw error;
      }

      this.logger?.warn?.({
        scope: "allocation",
        step: "record-verified-purchase-resource-error",
        purchaseId: input.purchaseId,
        message: String((error as any)?.message || error || "Unknown resource error")
      });

      await this.ensureGasStationResources(input);

      this.logger?.info?.({
        scope: "allocation",
        step: "record-verified-purchase-retry-after-gasstation",
        purchaseId: input.purchaseId
      });

      return await this.sendRecordVerifiedPurchase(input);
    }
  }
}

export class TronControllerAllocationExecutor implements AllocationExecutor {
  private readonly client: TronControllerClient;

  constructor(config: TronControllerAllocationExecutorConfig) {
    this.client = new TronControllerClient({
      tronWeb: config.tronWeb,
      contractAddress: config.controllerContractAddress,
      gasStation: config.gasStation ?? null,
      logger: config.logger
    });
  }

  async allocate(
    input: AllocationExecutorInput
  ): Promise<AllocationExecutorResult> {
    const purchase = input.purchase;

    if (!purchase.ambassadorWallet) {
      throw new Error("Ambassador wallet is required for allocation");
    }

    return this.client.recordVerifiedPurchase({
      purchaseId: purchase.purchaseId,
      buyerWallet: purchase.buyerWallet,
      ambassadorWallet: purchase.ambassadorWallet,
      purchaseAmountSun: purchase.purchaseAmountSun,
      ownerShareSun: purchase.ownerShareSun,
      feeLimitSun: input.feeLimitSun
    });
  }
}
