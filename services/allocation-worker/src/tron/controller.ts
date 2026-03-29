import crypto from "node:crypto";
import { FOURTEEN_CONTROLLER_CONTRACT } from "../../../../shared/config/contracts";
import { GasStationClient } from "../services/gasStation";
import type {
  AllocationExecutor,
  AllocationExecutorInput,
  AllocationExecutorResult
} from "../domain/allocation";

export interface ControllerClientConfig {
  tronWeb: any;
  contractAddress?: string;
  gasStationClient?: GasStationClient | null;
  gasStationEnabled?: boolean;
  gasStationMinEnergy?: number;
  gasStationMinBandwidth?: number;
  allocationMinEnergy?: number;
  allocationMinBandwidth?: number;
  gasStationServiceChargeType?: string;
}

export interface TronControllerAllocationExecutorConfig {
  tronWeb: any;
  controllerContractAddress?: string;
  gasStationClient?: GasStationClient | null;
  gasStationEnabled?: boolean;
  gasStationMinEnergy?: number;
  gasStationMinBandwidth?: number;
  allocationMinEnergy?: number;
  allocationMinBandwidth?: number;
  gasStationServiceChargeType?: string;
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

interface AccountResourceSnapshot {
  address: string;
  energyAvailable: number;
  bandwidthAvailable: number;
  trxBalanceSun: number;
}

const TRON_HEX_ZERO_ADDRESS = "410000000000000000000000000000000000000000";
const DEFAULT_SERVICE_CHARGE_TYPE = "10010";
const DEFAULT_WAIT_AFTER_ORDER_MS = 2500;

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

function normalizeNonNegativeInteger(value: number | undefined, fallback: number): number {
  if (value == null) {
    return fallback;
  }

  if (!Number.isFinite(value) || value < 0) {
    throw new Error("Resource threshold must be a non-negative integer");
  }

  return Math.floor(value);
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

function toNumberSafe(value: unknown): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function buildGasRequestId(prefix: string, purchaseId: string, suffix: string): string {
  const hash = crypto
    .createHash("sha256")
    .update(`${prefix}:${purchaseId}:${suffix}:${Date.now()}:${Math.random()}`)
    .digest("hex");

  return hash.slice(0, 32);
}

function ceilPositive(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return 0;
  }

  return Math.ceil(value);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getContract(tronWeb: any, contractAddress: string): Promise<any> {
  if (!tronWeb || typeof tronWeb.contract !== "function") {
    throw new Error("Valid tronWeb instance is required");
  }

  return await tronWeb.contract().at(contractAddress);
}

async function getAccountResourceSnapshot(
  tronWeb: any,
  address: string
): Promise<AccountResourceSnapshot> {
  const normalizedAddress = normalizeAddress(address, "address");

  const [resources, account, balanceSunRaw] = await Promise.all([
    tronWeb.trx.getAccountResources(normalizedAddress),
    tronWeb.trx.getAccount(normalizedAddress),
    tronWeb.trx.getBalance(normalizedAddress)
  ]);

  const energyLimit = toNumberSafe(resources?.EnergyLimit);
  const energyUsed = toNumberSafe(resources?.EnergyUsed);
  const energyAvailable = Math.max(0, energyLimit - energyUsed);

  const freeNetLimit = toNumberSafe(account?.freeNetLimit);
  const freeNetUsed = toNumberSafe(account?.freeNetUsed);
  const netLimit = toNumberSafe(account?.NetLimit);
  const netUsed = toNumberSafe(account?.NetUsed);

  const freeBandwidthAvailable = Math.max(0, freeNetLimit - freeNetUsed);
  const paidBandwidthAvailable = Math.max(0, netLimit - netUsed);
  const bandwidthAvailable = freeBandwidthAvailable + paidBandwidthAvailable;

  return {
    address: normalizedAddress,
    energyAvailable,
    bandwidthAvailable,
    trxBalanceSun: toNumberSafe(balanceSunRaw)
  };
}

export class TronControllerClient implements ControllerClient {
  private readonly tronWeb: any;
  private readonly contractAddress: string;
  private readonly gasStationClient: GasStationClient | null;
  private readonly gasStationEnabled: boolean;
  private readonly gasStationMinEnergy: number;
  private readonly gasStationMinBandwidth: number;
  private readonly allocationMinEnergy: number;
  private readonly allocationMinBandwidth: number;
  private readonly gasStationServiceChargeType: string;
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
    this.gasStationClient = config.gasStationClient ?? null;
    this.gasStationEnabled = Boolean(config.gasStationEnabled);
    this.gasStationMinEnergy = normalizeNonNegativeInteger(config.gasStationMinEnergy, 0);
    this.gasStationMinBandwidth = normalizeNonNegativeInteger(config.gasStationMinBandwidth, 0);
    this.allocationMinEnergy = normalizeNonNegativeInteger(config.allocationMinEnergy, 0);
    this.allocationMinBandwidth = normalizeNonNegativeInteger(config.allocationMinBandwidth, 0);
    this.gasStationServiceChargeType = assertNonEmpty(
      config.gasStationServiceChargeType ?? DEFAULT_SERVICE_CHARGE_TYPE,
      "gasStationServiceChargeType"
    );
  }

  private async contract(): Promise<any> {
    if (!this.contractInstance) {
      this.contractInstance = await getContract(this.tronWeb, this.contractAddress);
    }

    return this.contractInstance;
  }

  private getOperatorAddress(): string {
    const operatorAddress =
      this.tronWeb?.defaultAddress?.base58 ||
      this.tronWeb?.defaultAddress?.hex ||
      "";

    return normalizeAddress(operatorAddress, "operatorAddress");
  }

  private async estimateRentalCostSun(input: {
    energyToBuy: number;
    bandwidthToBuy: number;
  }): Promise<number> {
    if (!this.gasStationClient) {
      throw new Error("GasStation client is not configured");
    }

    let totalTrx = 0;

    if (input.energyToBuy > 0) {
      const energyEstimate = await this.gasStationClient.estimateEnergyOrder({
        receiveAddress: this.getOperatorAddress(),
        addressTo: this.getOperatorAddress(),
        contractAddress: this.contractAddress,
        serviceChargeType: this.gasStationServiceChargeType
      });

      totalTrx += toNumberSafe(energyEstimate.amount);
    }

    if (input.bandwidthToBuy > 0) {
      const bandwidthPrice = await this.gasStationClient.getPrice({
        serviceChargeType: this.gasStationServiceChargeType,
        resourceValue: input.bandwidthToBuy
      });

      const matched =
        bandwidthPrice.list?.find(
          (item) => item.service_charge_type === this.gasStationServiceChargeType
        ) ?? bandwidthPrice.list?.[0];

      if (!matched) {
        throw new Error("GasStation bandwidth price is unavailable");
      }

      totalTrx += toNumberSafe(matched.price);
    }

    return Math.ceil(totalTrx * 1_000_000);
  }

  private async ensureResourcesForAllocation(purchaseId: string): Promise<void> {
    const operatorAddress = this.getOperatorAddress();

    const before = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);

    const missingEnergy = Math.max(0, this.allocationMinEnergy - before.energyAvailable);
    const missingBandwidth = Math.max(0, this.allocationMinBandwidth - before.bandwidthAvailable);

    if (missingEnergy <= 0 && missingBandwidth <= 0) {
      return;
    }

    if (!this.gasStationEnabled || !this.gasStationClient) {
      throw new Error(
        `Account resource insufficient. Energy=${before.energyAvailable}, Bandwidth=${before.bandwidthAvailable}, RequiredEnergy=${this.allocationMinEnergy}, RequiredBandwidth=${this.allocationMinBandwidth}`
      );
    }

    const energyToBuy =
      missingEnergy > 0
        ? Math.max(this.gasStationMinEnergy, missingEnergy, 64400)
        : 0;

    const bandwidthToBuy =
      missingBandwidth > 0
        ? Math.max(this.gasStationMinBandwidth, missingBandwidth, 5000)
        : 0;

    const estimatedRentalCostSun = await this.estimateRentalCostSun({
      energyToBuy,
      bandwidthToBuy
    });

    if (before.trxBalanceSun < estimatedRentalCostSun) {
      throw new Error(
        `Insufficient TRX balance for resource rental. BalanceSun=${before.trxBalanceSun}, RequiredSun=${estimatedRentalCostSun}, MissingEnergy=${missingEnergy}, MissingBandwidth=${missingBandwidth}`
      );
    }

    if (energyToBuy > 0) {
      await this.gasStationClient.createEnergyOrder({
        requestId: buildGasRequestId("allocation", purchaseId, "energy"),
        receiveAddress: operatorAddress,
        energyNum: energyToBuy,
        serviceChargeType: this.gasStationServiceChargeType
      });
    }

    if (bandwidthToBuy > 0) {
      await this.gasStationClient.createBandwidthOrder({
        requestId: buildGasRequestId("allocation", purchaseId, "bandwidth"),
        receiveAddress: operatorAddress,
        netNum: bandwidthToBuy,
        serviceChargeType: this.gasStationServiceChargeType
      });
    }

    await delay(DEFAULT_WAIT_AFTER_ORDER_MS);

    const after = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);

    if (
      after.energyAvailable < this.allocationMinEnergy ||
      after.bandwidthAvailable < this.allocationMinBandwidth
    ) {
      throw new Error(
        `Account resource insufficient after rental. Energy=${after.energyAvailable}, Bandwidth=${after.bandwidthAvailable}, RequiredEnergy=${this.allocationMinEnergy}, RequiredBandwidth=${this.allocationMinBandwidth}`
      );
    }
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
    const purchaseId = normalizeBytes32Hex(input.purchaseId, "purchaseId");
    const buyerWallet = normalizeAddress(input.buyerWallet, "buyerWallet");
    const ambassadorWallet = normalizeAddress(input.ambassadorWallet, "ambassadorWallet");
    const purchaseAmountSun = normalizeSunAmount(input.purchaseAmountSun, "purchaseAmountSun");
    const ownerShareSun = normalizeSunAmount(input.ownerShareSun, "ownerShareSun");
    const feeLimitSun = normalizeFeeLimitSun(input.feeLimitSun);

    await this.ensureResourcesForAllocation(purchaseId);

    const contract = await this.contract();

    const txid = await contract
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

    return {
      txid: assertNonEmpty(txid, "txid")
    };
  }
}

export class TronControllerAllocationExecutor implements AllocationExecutor {
  private readonly client: TronControllerClient;

  constructor(config: TronControllerAllocationExecutorConfig) {
    this.client = new TronControllerClient({
      tronWeb: config.tronWeb,
      contractAddress: config.controllerContractAddress,
      gasStationClient: config.gasStationClient ?? null,
      gasStationEnabled: config.gasStationEnabled,
      gasStationMinEnergy: config.gasStationMinEnergy,
      gasStationMinBandwidth: config.gasStationMinBandwidth,
      allocationMinEnergy: config.allocationMinEnergy,
      allocationMinBandwidth: config.allocationMinBandwidth,
      gasStationServiceChargeType: config.gasStationServiceChargeType
    });
  }

  async allocate(input: AllocationExecutorInput): Promise<AllocationExecutorResult> {
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
