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

interface GasStationBalanceSnapshot {
  balanceSun: number;
  depositAddress: string;
}

const TRON_HEX_ZERO_ADDRESS = "410000000000000000000000000000000000000000";
const DEFAULT_SERVICE_CHARGE_TYPE = "10010";
const DEFAULT_TRON_RETRY_ATTEMPTS = 4;

const SUN_PER_TRX = 1_000_000;

const GASSTATION_LOW_BALANCE_SUN = 8_500_000;
const OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN = 11_000_000;
const OPERATOR_REMAINING_RESERVE_SUN = 2_000_000;

const GASSTATION_TOPUP_POLL_INTERVAL_MS = 4000;
const GASSTATION_TOPUP_POLL_ATTEMPTS = 12;

const RESOURCE_DELIVERY_POLL_INTERVAL_MS = 5000;
const RESOURCE_DELIVERY_POLL_ATTEMPTS = 18;

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

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getErrorMessage(error: unknown): string {
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

  return "Unknown error";
}

function extractErrorCode(error: unknown): string | null {
  if (!error || typeof error !== "object") {
    return null;
  }

  const candidates = [
    (error as any).code,
    (error as any).errorCode,
    (error as any).response?.status,
    (error as any).statusCode,
    (error as any).status
  ];

  for (const candidate of candidates) {
    if (candidate == null) {
      continue;
    }

    const normalized = String(candidate).trim();
    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function parseRetryAfterMs(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return Math.floor(value);
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed >= 0) {
      return Math.floor(parsed);
    }
  }

  return null;
}

function isRateLimitError(error: unknown): boolean {
  const message = getErrorMessage(error).toLowerCase();
  const code = String(extractErrorCode(error) || "").toUpperCase();

  return (
    message.includes("status code 429") ||
    message.includes("http 429") ||
    message.includes("too many requests") ||
    message.includes("rate limit") ||
    message.includes("rate limited") ||
    code === "429" ||
    code === "ERR_BAD_REQUEST" ||
    code === "TRON_RATE_LIMIT" ||
    code === "GASSTATION_RATE_LIMIT"
  );
}

function createTaggedError(
  message: string,
  extras?: {
    code?: string | null;
    retryAfterMs?: number | null;
    cause?: unknown;
  }
): Error {
  const error = new Error(message) as Error & {
    code?: string;
    retryAfterMs?: number | null;
    cause?: unknown;
  };

  if (extras?.code) {
    error.code = extras.code;
  }

  if (extras?.retryAfterMs != null) {
    error.retryAfterMs = extras.retryAfterMs;
  }

  if (extras?.cause !== undefined) {
    error.cause = extras.cause;
  }

  return error;
}

function wrapAsRateLimitError(error: unknown, defaultCode = "TRON_RATE_LIMIT"): Error {
  const retryAfterMs =
    parseRetryAfterMs((error as any)?.retryAfterMs) ??
    parseRetryAfterMs((error as any)?.response?.headers?.["retry-after"]) ??
    null;

  return createTaggedError(getErrorMessage(error), {
    code: extractErrorCode(error) ?? defaultCode,
    retryAfterMs,
    cause: error
  });
}

function computeBackoffMs(attemptIndex: number, retryAfterMs?: number | null): number {
  if (retryAfterMs != null && retryAfterMs >= 0) {
    return retryAfterMs;
  }

  if (attemptIndex <= 0) return 750;
  if (attemptIndex === 1) return 1500;
  if (attemptIndex === 2) return 3000;
  return 5000;
}

async function withRateLimitRetry<T>(
  operationName: string,
  fn: () => Promise<T>,
  maxAttempts = DEFAULT_TRON_RETRY_ATTEMPTS
): Promise<T> {
  let lastError: unknown = null;

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      if (!isRateLimitError(error) || attempt >= maxAttempts - 1) {
        break;
      }

      const waitMs = computeBackoffMs(
        attempt,
        parseRetryAfterMs((error as any)?.retryAfterMs)
      );

      await delay(waitMs);
    }
  }

  if (isRateLimitError(lastError)) {
    throw wrapAsRateLimitError(
      lastError,
      `${operationName.toUpperCase().replace(/[^A-Z0-9]+/g, "_")}_RATE_LIMIT`
    );
  }

  throw lastError instanceof Error ? lastError : new Error(`${operationName} failed`);
}

function parseTrxAmountToSun(value: unknown, fieldName: string): number {
  const raw = String(value ?? "").trim();

  if (!raw) {
    throw new Error(`${fieldName} is required`);
  }

  if (!/^\d+(\.\d+)?$/.test(raw)) {
    throw new Error(`${fieldName} must be a numeric TRX amount`);
  }

  const [wholePart, fractionPart = ""] = raw.split(".");
  const normalizedFraction = `${fractionPart}000000`.slice(0, 6);

  const whole = BigInt(wholePart || "0");
  const fraction = BigInt(normalizedFraction || "0");
  const totalSun = whole * BigInt(SUN_PER_TRX) + fraction;

  if (totalSun > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`${fieldName} is too large`);
  }

  return Number(totalSun);
}

function extractTxidFromSendTransactionResult(result: unknown): string | null {
  if (!result || typeof result !== "object") {
    return null;
  }

  const candidates = [
    (result as any).txid,
    (result as any).transaction?.txID,
    (result as any).txID
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
    }
  }

  return null;
}

function normalizePriceItems(source: unknown): Array<{
  expire_min: string;
  service_charge_type: string;
  price: string;
  remaining_number: string;
}> {
  if (!Array.isArray(source)) {
    return [];
  }

  return source
    .filter((item) => item && typeof item === "object")
    .map((item) => {
      const value = item as Record<string, unknown>;
      return {
        expire_min: String(value.expire_min ?? "").trim(),
        service_charge_type: String(value.service_charge_type ?? "").trim(),
        price: String(value.price ?? "").trim(),
        remaining_number: String(value.remaining_number ?? "").trim()
      };
    })
    .filter((item) => item.service_charge_type && item.price);
}

async function getContract(tronWeb: any, contractAddress: string): Promise<any> {
  if (!tronWeb || typeof tronWeb.contract !== "function") {
    throw new Error("Valid tronWeb instance is required");
  }

  return withRateLimitRetry("contract.at", async () => {
    return await tronWeb.contract().at(contractAddress);
  });
}

async function getAccountResourceSnapshot(
  tronWeb: any,
  address: string
): Promise<AccountResourceSnapshot> {
  const normalizedAddress = normalizeAddress(address, "address");

  const [resources, account, balanceSunRaw] = await Promise.all([
    withRateLimitRetry("trx.getAccountResources", async () => {
      return await tronWeb.trx.getAccountResources(normalizedAddress);
    }),
    withRateLimitRetry("trx.getAccount", async () => {
      return await tronWeb.trx.getAccount(normalizedAddress);
    }),
    withRateLimitRetry("trx.getBalance", async () => {
      return await tronWeb.trx.getBalance(normalizedAddress);
    })
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
      try {
        const energyEstimate = await this.gasStationClient.estimateEnergyOrder({
          receiveAddress: this.getOperatorAddress(),
          addressTo: this.getOperatorAddress(),
          contractAddress: this.contractAddress,
          serviceChargeType: this.gasStationServiceChargeType
        });

        totalTrx += toNumberSafe(energyEstimate.amount);
      } catch (error) {
        const code = String(extractErrorCode(error) || "").toUpperCase();

        if (code.includes("GASSTATION_ERROR_100003")) {
          totalTrx += GASSTATION_LOW_BALANCE_SUN / SUN_PER_TRX;
        } else {
          throw error;
        }
      }
    }

    if (input.bandwidthToBuy > 0) {
      const bandwidthPrice = await this.gasStationClient.getPrice({
        serviceChargeType: this.gasStationServiceChargeType,
        resourceValue: input.bandwidthToBuy
      });

      const items = normalizePriceItems(
        (bandwidthPrice as any).list?.length
          ? (bandwidthPrice as any).list
          : (bandwidthPrice as any).price_builder_list
      );

      const matched =
        items.find(
          (item) => item.service_charge_type === this.gasStationServiceChargeType
        ) ?? items[0];

      if (!matched) {
        throw new Error("GasStation bandwidth price is unavailable");
      }

      totalTrx += toNumberSafe(matched.price);
    }

    return Math.ceil(totalTrx * SUN_PER_TRX);
  }

  private async getGasStationBalanceSnapshot(): Promise<GasStationBalanceSnapshot> {
    if (!this.gasStationClient) {
      throw new Error("GasStation client is not configured");
    }

    const result = await this.gasStationClient.getBalance();
    const depositAddress = normalizeAddress(
      String(result.deposit_address || "").trim(),
      "deposit_address"
    );
    const balanceSun = parseTrxAmountToSun(result.balance, "gasStation.balance");

    return {
      balanceSun,
      depositAddress
    };
  }

  private async waitForGasStationBalanceIncrease(input: {
    beforeBalanceSun: number;
  }): Promise<GasStationBalanceSnapshot> {
    let lastSnapshot: GasStationBalanceSnapshot | null = null;

    for (let attempt = 0; attempt < GASSTATION_TOPUP_POLL_ATTEMPTS; attempt += 1) {
      await delay(GASSTATION_TOPUP_POLL_INTERVAL_MS);

      const snapshot = await this.getGasStationBalanceSnapshot();
      lastSnapshot = snapshot;

      if (snapshot.balanceSun > input.beforeBalanceSun) {
        return snapshot;
      }
    }

    throw createTaggedError(
      `GasStation service balance was low, auto top-up transfer was sent but the balance did not update in time. LastBalanceSun=${lastSnapshot?.balanceSun ?? 0}`,
      {
        code: "GASSTATION_TOPUP_NOT_SETTLED"
      }
    );
  }

  private async topUpGasStationFromOperatorIfNeeded(requiredSun: number): Promise<void> {
    if (!this.gasStationClient) {
      throw new Error("GasStation client is not configured");
    }

    const requiredServiceBalanceSun = Math.max(requiredSun, GASSTATION_LOW_BALANCE_SUN);
    const beforeGasStation = await this.getGasStationBalanceSnapshot();

    if (beforeGasStation.balanceSun >= requiredServiceBalanceSun) {
      return;
    }

    const operatorAddress = this.getOperatorAddress();
    const operatorSnapshot = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);

    const availableToTopUpSun = Math.max(
      0,
      operatorSnapshot.trxBalanceSun - OPERATOR_REMAINING_RESERVE_SUN
    );

    if (
      operatorSnapshot.trxBalanceSun < OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN ||
      availableToTopUpSun < GASSTATION_LOW_BALANCE_SUN
    ) {
      throw createTaggedError(
        `GasStation service balance is low and auto top-up was skipped because operator wallet balance is below 11 TRX. OperatorBalanceSun=${operatorSnapshot.trxBalanceSun}, AvailableToTopUpSun=${availableToTopUpSun}, RequiredServiceBalanceSun=${requiredServiceBalanceSun}`,
        {
          code: "GASSTATION_OPERATOR_BALANCE_LOW"
        }
      );
    }

    let transferResult: unknown;

    try {
      transferResult = await withRateLimitRetry("trx.sendTransaction", async () => {
        return await this.tronWeb.trx.sendTransaction(
          beforeGasStation.depositAddress,
          availableToTopUpSun
        );
      });
    } catch (error) {
      throw createTaggedError(
        `GasStation service balance was low, auto top-up transfer failed. ${getErrorMessage(error)}`,
        {
          code: "GASSTATION_TOPUP_TRANSFER_FAILED",
          cause: error
        }
      );
    }

    const txid = extractTxidFromSendTransactionResult(transferResult);

    let afterTopUp: GasStationBalanceSnapshot;

    try {
      afterTopUp = await this.waitForGasStationBalanceIncrease({
        beforeBalanceSun: beforeGasStation.balanceSun
      });
    } catch (error) {
      if (
        error &&
        typeof error === "object" &&
        (error as any).code === "GASSTATION_TOPUP_NOT_SETTLED"
      ) {
        throw error instanceof Error
          ? error
          : createTaggedError("GasStation top-up settlement check failed", {
              code: "GASSTATION_TOPUP_NOT_SETTLED",
              cause: error
            });
      }

      throw createTaggedError(
        `GasStation service balance was low, auto top-up failed after transfer${txid ? ` (${txid})` : ""}. ${getErrorMessage(error)}`,
        {
          code: "GASSTATION_TOPUP_FAILED",
          cause: error
        }
      );
    }

    if (afterTopUp.balanceSun < requiredServiceBalanceSun) {
      throw createTaggedError(
        `GasStation service balance was topped up, but it is still not enough for resource order. BalanceSun=${afterTopUp.balanceSun}, RequiredSun=${requiredServiceBalanceSun}`,
        {
          code: "GASSTATION_SERVICE_BALANCE_LOW_AFTER_TOPUP"
        }
      );
    }
  }

  private async waitForRequiredResources(input: {
    requiredEnergy: number;
    requiredBandwidth: number;
  }): Promise<AccountResourceSnapshot> {
    let lastSnapshot: AccountResourceSnapshot | null = null;
    const operatorAddress = this.getOperatorAddress();

    for (let attempt = 0; attempt < RESOURCE_DELIVERY_POLL_ATTEMPTS; attempt += 1) {
      await delay(RESOURCE_DELIVERY_POLL_INTERVAL_MS);

      const snapshot = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);
      lastSnapshot = snapshot;

      if (
        snapshot.energyAvailable >= input.requiredEnergy &&
        snapshot.bandwidthAvailable >= input.requiredBandwidth
      ) {
        return snapshot;
      }
    }

    throw createTaggedError(
      `Account resource insufficient after rental. Energy=${lastSnapshot?.energyAvailable ?? 0}, Bandwidth=${lastSnapshot?.bandwidthAvailable ?? 0}, RequiredEnergy=${input.requiredEnergy}, RequiredBandwidth=${input.requiredBandwidth}`,
      {
        code: "ACCOUNT_RESOURCE_INSUFFICIENT_AFTER_RENTAL"
      }
    );
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
      throw createTaggedError(
        `Account resource insufficient. Energy=${before.energyAvailable}, Bandwidth=${before.bandwidthAvailable}, RequiredEnergy=${this.allocationMinEnergy}, RequiredBandwidth=${this.allocationMinBandwidth}`,
        {
          code: "ACCOUNT_RESOURCE_INSUFFICIENT"
        }
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

    await this.topUpGasStationFromOperatorIfNeeded(estimatedRentalCostSun);

    try {
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
    } catch (error) {
      if (isRateLimitError(error)) {
        throw wrapAsRateLimitError(error, "GASSTATION_RATE_LIMIT");
      }

      throw createTaggedError(
        `GasStation balance topped up or was already sufficient, but resource order failed. ${getErrorMessage(error)}`,
        {
          code: "GASSTATION_ORDER_FAILED",
          cause: error
        }
      );
    }

    await this.waitForRequiredResources({
      requiredEnergy: this.allocationMinEnergy,
      requiredBandwidth: this.allocationMinBandwidth
    });
  }

  async getAmbassadorBySlugHash(slugHash: string): Promise<ResolveAmbassadorBySlugHashResult> {
    const normalizedSlugHash = normalizeBytes32Hex(slugHash, "slugHash");
    const contract = await this.contract();

    const result = await withRateLimitRetry("getAmbassadorBySlugHash.call", async () => {
      return await contract.getAmbassadorBySlugHash(normalizedSlugHash).call();
    });

    const ambassadorWallet = normalizeReturnedAddress(this.tronWeb, result);

    return {
      slugHash: normalizedSlugHash,
      ambassadorWallet
    };
  }

  async getBuyerAmbassador(buyerWallet: string): Promise<string | null> {
    const normalizedBuyerWallet = normalizeAddress(buyerWallet, "buyerWallet");
    const contract = await this.contract();

    const result = await withRateLimitRetry("getBuyerAmbassador.call", async () => {
      return await contract.getBuyerAmbassador(normalizedBuyerWallet).call();
    });

    return normalizeReturnedAddress(this.tronWeb, result);
  }

  async isPurchaseProcessed(purchaseId: string): Promise<boolean> {
    const normalizedPurchaseId = normalizeBytes32Hex(purchaseId, "purchaseId");
    const contract = await this.contract();

    const result = await withRateLimitRetry("isPurchaseProcessed.call", async () => {
      return await contract.isPurchaseProcessed(normalizedPurchaseId).call();
    });

    return Boolean(result);
  }

  async canBindBuyerToAmbassador(
    buyerWallet: string,
    ambassadorWallet: string
  ): Promise<boolean> {
    const normalizedBuyerWallet = normalizeAddress(buyerWallet, "buyerWallet");
    const normalizedAmbassadorWallet = normalizeAddress(ambassadorWallet, "ambassadorWallet");
    const contract = await this.contract();

    const result = await withRateLimitRetry("canBindBuyerToAmbassador.call", async () => {
      return await contract
        .canBindBuyerToAmbassador(normalizedBuyerWallet, normalizedAmbassadorWallet)
        .call();
    });

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

    try {
      const txid = await withRateLimitRetry("recordVerifiedPurchase.send", async () => {
        return await contract
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
      });

      return {
        txid: assertNonEmpty(txid, "txid")
      };
    } catch (error) {
      if (isRateLimitError(error)) {
        throw wrapAsRateLimitError(error, "TRON_RATE_LIMIT");
      }
      throw error;
    }
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
