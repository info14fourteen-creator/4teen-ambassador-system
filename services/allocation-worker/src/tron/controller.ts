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
  ownerAutoWithdrawEnabled?: boolean;
  ownerWithdrawMinSun?: number;
  ownerWithdrawFeeLimitSun?: number;
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
  ownerAutoWithdrawEnabled?: boolean;
  ownerWithdrawMinSun?: number;
  ownerWithdrawFeeLimitSun?: number;
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

export interface WithdrawOwnerFundsResult {
  txid: string;
  amountSun: string;
}

export interface ControllerClient {
  getAmbassadorBySlugHash(slugHash: string): Promise<ResolveAmbassadorBySlugHashResult>;
  getBuyerAmbassador(buyerWallet: string): Promise<string | null>;
  isPurchaseProcessed(purchaseId: string): Promise<boolean>;
  canBindBuyerToAmbassador(buyerWallet: string, ambassadorWallet: string): Promise<boolean>;
  getOwnerAvailableBalance(): Promise<string>;
  isOperatorContractOwner(): Promise<boolean>;
  withdrawOwnerFunds(amountSun: string, feeLimitSun?: number): Promise<WithdrawOwnerFundsResult>;
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

interface ResourceRequirement {
  requiredEnergy: number;
  requiredBandwidth: number;
  targetEnergy: number;
  targetBandwidth: number;
}

type TaggedError = Error & {
  code?: string;
  retryAfterMs?: number | null;
  cause?: unknown;
};

const TRON_HEX_ZERO_ADDRESS = "410000000000000000000000000000000000000000";

const DEFAULT_SERVICE_CHARGE_TYPE = "10010";
const DEFAULT_TRON_RETRY_ATTEMPTS = 4;
const DEFAULT_FEE_LIMIT_SUN = 300_000_000;
const DEFAULT_OWNER_WITHDRAW_MIN_SUN = 1;
const DEFAULT_OWNER_WITHDRAW_FEE_LIMIT_SUN = 300_000_000;

const SUN_PER_TRX = 1_000_000;

const GASSTATION_LOW_BALANCE_SUN = 8_500_000;
const OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN = 11_000_000;
const OPERATOR_REMAINING_RESERVE_SUN = 2_000_000;

const GASSTATION_TOPUP_POLL_INTERVAL_MS = 4_000;
const GASSTATION_TOPUP_POLL_ATTEMPTS = 12;

const RESOURCE_DELIVERY_POLL_INTERVAL_MS = 5_000;
const RESOURCE_DELIVERY_POLL_ATTEMPTS = 18;

const RESOURCE_STABILIZATION_DELAY_MS = 2_500;
const RESOURCE_RECHECK_BEFORE_SEND_DELAY_MS = 1_500;

const MIN_ENERGY_ORDER_FLOOR = 64_400;
const MIN_BANDWIDTH_ORDER_FLOOR = 5_000;

const ENERGY_MARGIN_PERCENT = 20;
const BANDWIDTH_MARGIN_PERCENT = 20;
const MIN_ENERGY_MARGIN = 12_000;
const MIN_BANDWIDTH_MARGIN = 1_500;

/**
 * Extra service-balance safety buffer.
 * We do not want RequiredServiceBalanceSun to explode because of estimate endpoint noise,
 * but we still want a small buffer above pure price.
 */
const GASSTATION_PRICE_BUFFER_SUN = 1_000_000;

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

function normalizeFeeLimitSun(
  value: number | undefined,
  fallback = DEFAULT_FEE_LIMIT_SUN
): number {
  const resolved = value ?? fallback;

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

    if (typeof tronWeb?.address?.fromHex === "function") {
      return tronWeb.address.fromHex(raw);
    }

    return raw;
  }

  if (isBase58Address(raw)) {
    return raw;
  }

  return null;
}

function toComparableAddress(tronWeb: any, value: unknown): string {
  const raw = String(value || "").trim();

  if (!raw) {
    return "";
  }

  if (isHexAddress(raw)) {
    return raw.toLowerCase();
  }

  if (isBase58Address(raw) && typeof tronWeb?.address?.toHex === "function") {
    try {
      return String(tronWeb.address.toHex(raw) || "").trim().toLowerCase();
    } catch {
      return raw.toLowerCase();
    }
  }

  return raw.toLowerCase();
}

function toNumberSafe(value: unknown): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function buildGasRequestId(prefix: string, purchaseId: string, suffix: string): string {
  return crypto
    .createHash("sha256")
    .update(`${prefix}:${purchaseId}:${suffix}:${Date.now()}:${Math.random()}`)
    .digest("hex")
    .slice(0, 32);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function logJson(payload: Record<string, unknown>): void {
  try {
    console.log(JSON.stringify(payload));
  } catch {
    console.log(
      JSON.stringify({
        level: "warn",
        scope: "logger",
        stage: "log-json-failed"
      })
    );
  }
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
): TaggedError {
  const error = new Error(message) as TaggedError;

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

function wrapAsRateLimitError(error: unknown, defaultCode = "TRON_RATE_LIMIT"): TaggedError {
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
  if (attemptIndex === 1) return 1_500;
  if (attemptIndex === 2) return 3_000;
  return 5_000;
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
  if (typeof result === "string" && result.trim()) {
    return result.trim();
  }

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

function calculateMargin(baseValue: number, percent: number, minValue: number): number {
  if (baseValue <= 0) {
    return 0;
  }

  return Math.max(minValue, Math.ceil((baseValue * percent) / 100));
}

function buildResourceRequirement(
  requiredEnergy: number,
  requiredBandwidth: number
): ResourceRequirement {
  const energyMargin = calculateMargin(requiredEnergy, ENERGY_MARGIN_PERCENT, MIN_ENERGY_MARGIN);
  const bandwidthMargin = calculateMargin(
    requiredBandwidth,
    BANDWIDTH_MARGIN_PERCENT,
    MIN_BANDWIDTH_MARGIN
  );

  return {
    requiredEnergy,
    requiredBandwidth,
    targetEnergy: requiredEnergy > 0 ? requiredEnergy + energyMargin : 0,
    targetBandwidth: requiredBandwidth > 0 ? requiredBandwidth + bandwidthMargin : 0
  };
}

function isResourceSendError(error: unknown): boolean {
  const message = getErrorMessage(error).toLowerCase();
  const code = String(extractErrorCode(error) || "").toUpperCase();

  return (
    message.includes("out of energy") ||
    message.includes("account resource insufficient") ||
    message.includes("insufficient bandwidth") ||
    message.includes("insufficient energy") ||
    message.includes("bandwidth limit") ||
    message.includes("energy limit") ||
    message.includes("fee limit") ||
    code.includes("OUT_OF_ENERGY") ||
    code.includes("BANDWIDTH") ||
    code.includes("ENERGY") ||
    code.includes("ACCOUNT_RESOURCE")
  );
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

  const [resources, account, balanceSunRaw, bandwidthRaw] = await Promise.all([
    withRateLimitRetry("trx.getAccountResources", async () => {
      return await tronWeb.trx.getAccountResources(normalizedAddress);
    }),
    withRateLimitRetry("trx.getAccount", async () => {
      return await tronWeb.trx.getAccount(normalizedAddress);
    }),
    withRateLimitRetry("trx.getBalance", async () => {
      return await tronWeb.trx.getBalance(normalizedAddress);
    }),
    withRateLimitRetry("trx.getBandwidth", async () => {
      if (typeof tronWeb?.trx?.getBandwidth !== "function") {
        return 0;
      }

      return await tronWeb.trx.getBandwidth(normalizedAddress);
    })
  ]);

  const energyLimit = toNumberSafe(resources?.EnergyLimit ?? account?.EnergyLimit);
  const energyUsed = toNumberSafe(resources?.EnergyUsed ?? account?.EnergyUsed);
  const energyAvailable = Math.max(0, energyLimit - energyUsed);

  const freeNetLimit = Math.max(
    toNumberSafe(account?.freeNetLimit),
    toNumberSafe(resources?.freeNetLimit),
    toNumberSafe(account?.free_net_limit),
    toNumberSafe(account?.freeNetLimitV2),
    toNumberSafe(resources?.freeNetLimitV2)
  );

  const freeNetUsed = Math.max(
    toNumberSafe(account?.freeNetUsed),
    toNumberSafe(resources?.freeNetUsed),
    toNumberSafe(account?.free_net_used),
    toNumberSafe(account?.freeNetUsedV2),
    toNumberSafe(resources?.freeNetUsedV2)
  );

  const netLimit = Math.max(
    toNumberSafe(account?.NetLimit),
    toNumberSafe(resources?.NetLimit),
    toNumberSafe(account?.netLimit),
    toNumberSafe(resources?.netLimit),
    toNumberSafe(account?.net_limit)
  );

  const netUsed = Math.max(
    toNumberSafe(account?.NetUsed),
    toNumberSafe(resources?.NetUsed),
    toNumberSafe(account?.netUsed),
    toNumberSafe(resources?.netUsed),
    toNumberSafe(account?.net_used)
  );

  const calculatedFreeBandwidth = Math.max(0, freeNetLimit - freeNetUsed);
  const calculatedPaidBandwidth = Math.max(0, netLimit - netUsed);
  const calculatedBandwidth = calculatedFreeBandwidth + calculatedPaidBandwidth;
  const bandwidthAvailable = Math.max(0, toNumberSafe(bandwidthRaw), calculatedBandwidth);

  return {
    address: normalizedAddress,
    energyAvailable,
    bandwidthAvailable,
    trxBalanceSun: toNumberSafe(balanceSunRaw)
  };
}

export class TronControllerClient implements ControllerClient {
  private static readonly operatorLocks = new Map<string, Promise<unknown>>();

  private readonly tronWeb: any;
  private readonly contractAddress: string;
  private readonly gasStationClient: GasStationClient | null;
  private readonly gasStationEnabled: boolean;
  private readonly gasStationMinEnergy: number;
  private readonly gasStationMinBandwidth: number;
  private readonly allocationMinEnergy: number;
  private readonly allocationMinBandwidth: number;
  private readonly gasStationServiceChargeType: string;
  private readonly ownerAutoWithdrawEnabled: boolean;
  private readonly ownerWithdrawMinSun: number;
  private readonly ownerWithdrawFeeLimitSun: number;

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
    this.ownerAutoWithdrawEnabled = Boolean(config.ownerAutoWithdrawEnabled);
    this.ownerWithdrawMinSun = normalizeNonNegativeInteger(
      config.ownerWithdrawMinSun,
      DEFAULT_OWNER_WITHDRAW_MIN_SUN
    );
    this.ownerWithdrawFeeLimitSun = normalizeFeeLimitSun(
      config.ownerWithdrawFeeLimitSun,
      DEFAULT_OWNER_WITHDRAW_FEE_LIMIT_SUN
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

  private async runWithOperatorLock<T>(fn: () => Promise<T>): Promise<T> {
    const operatorAddress = this.getOperatorAddress();
    const previous = TronControllerClient.operatorLocks.get(operatorAddress) ?? Promise.resolve();

    const current = previous
      .catch(() => undefined)
      .then(async () => {
        return await fn();
      });

    TronControllerClient.operatorLocks.set(operatorAddress, current);

    try {
      return await current;
    } finally {
      const stored = TronControllerClient.operatorLocks.get(operatorAddress);

      if (stored === current) {
        TronControllerClient.operatorLocks.delete(operatorAddress);
      }
    }
  }

  private buildCurrentRequirement(): ResourceRequirement {
    return buildResourceRequirement(this.allocationMinEnergy, this.allocationMinBandwidth);
  }

  private async getGasStationPriceSun(resourceValue: number): Promise<number> {
    if (!this.gasStationClient) {
      throw new Error("GasStation client is not configured");
    }

    const priceResult = await this.gasStationClient.getPrice({
      serviceChargeType: this.gasStationServiceChargeType,
      resourceValue
    });

    const items = normalizePriceItems(
      (priceResult as any).list?.length
        ? (priceResult as any).list
        : (priceResult as any).price_builder_list
    );

    const matched =
      items.find(
        (item) => item.service_charge_type === this.gasStationServiceChargeType
      ) ?? items[0];

    if (!matched) {
      throw new Error("GasStation price is unavailable");
    }

    return parseTrxAmountToSun(matched.price, "gasStation.price");
  }

  private async estimateRentalCostSun(input: {
    energyToBuy: number;
    bandwidthToBuy: number;
  }): Promise<number> {
    if (!this.gasStationClient) {
      throw new Error("GasStation client is not configured");
    }

    let totalSun = 0;

    if (input.energyToBuy > 0) {
      const energyPriceSun = await this.getGasStationPriceSun(input.energyToBuy);
      totalSun += energyPriceSun;
    }

    if (input.bandwidthToBuy > 0) {
      const bandwidthPriceSun = await this.getGasStationPriceSun(input.bandwidthToBuy);
      totalSun += bandwidthPriceSun;
    }

    if (totalSun <= 0) {
      return 0;
    }

    return totalSun + GASSTATION_PRICE_BUFFER_SUN;
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

    logJson({
      level: "info",
      scope: "gasstation",
      stage: "topup-check",
      requiredSun,
      requiredServiceBalanceSun,
      currentServiceBalanceSun: beforeGasStation.balanceSun,
      depositAddress: beforeGasStation.depositAddress
    });

    if (beforeGasStation.balanceSun >= requiredServiceBalanceSun) {
      logJson({
        level: "info",
        scope: "gasstation",
        stage: "topup-skipped-enough-balance",
        requiredServiceBalanceSun,
        currentServiceBalanceSun: beforeGasStation.balanceSun
      });
      return;
    }

    const operatorAddress = this.getOperatorAddress();
    const operatorSnapshot = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);

    const availableToTopUpSun = Math.max(
      0,
      operatorSnapshot.trxBalanceSun - OPERATOR_REMAINING_RESERVE_SUN
    );

    const shortfallSun = Math.max(0, requiredServiceBalanceSun - beforeGasStation.balanceSun);

    logJson({
      level: "info",
      scope: "gasstation",
      stage: "topup-operator-balance",
      operatorAddress,
      operatorBalanceSun: operatorSnapshot.trxBalanceSun,
      availableToTopUpSun,
      requiredServiceBalanceSun,
      shortfallSun
    });

    if (
      operatorSnapshot.trxBalanceSun < OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN ||
      availableToTopUpSun < Math.max(shortfallSun, 1)
    ) {
      throw createTaggedError(
        `GasStation service balance is low and auto top-up was skipped because operator wallet balance is below safe threshold. OperatorBalanceSun=${operatorSnapshot.trxBalanceSun}, AvailableToTopUpSun=${availableToTopUpSun}, RequiredServiceBalanceSun=${requiredServiceBalanceSun}, CurrentServiceBalanceSun=${beforeGasStation.balanceSun}`,
        {
          code: "GASSTATION_OPERATOR_BALANCE_LOW"
        }
      );
    }

    const topupAmountSun = Math.min(
      availableToTopUpSun,
      Math.max(shortfallSun, GASSTATION_LOW_BALANCE_SUN)
    );

    let transferResult: unknown;

    try {
      transferResult = await withRateLimitRetry("trx.sendTransaction", async () => {
        return await this.tronWeb.trx.sendTransaction(
          beforeGasStation.depositAddress,
          topupAmountSun
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

    logJson({
      level: "info",
      scope: "gasstation",
      stage: "topup-transfer-sent",
      operatorAddress,
      depositAddress: beforeGasStation.depositAddress,
      topupAmountSun,
      txid
    });

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

    logJson({
      level: "info",
      scope: "gasstation",
      stage: "topup-settled",
      txid,
      beforeBalanceSun: beforeGasStation.balanceSun,
      afterBalanceSun: afterTopUp.balanceSun,
      requiredServiceBalanceSun
    });

    if (afterTopUp.balanceSun < requiredServiceBalanceSun) {
      throw createTaggedError(
        `GasStation service balance was topped up, but it is still not enough for resource order. BalanceSun=${afterTopUp.balanceSun}, RequiredSun=${requiredServiceBalanceSun}`,
        {
          code: "GASSTATION_SERVICE_BALANCE_LOW_AFTER_TOPUP"
        }
      );
    }
  }

  private async waitForRequiredResources(
    requirement: ResourceRequirement
  ): Promise<AccountResourceSnapshot> {
    let lastSnapshot: AccountResourceSnapshot | null = null;
    const operatorAddress = this.getOperatorAddress();

    for (let attempt = 0; attempt < RESOURCE_DELIVERY_POLL_ATTEMPTS; attempt += 1) {
      await delay(RESOURCE_DELIVERY_POLL_INTERVAL_MS);

      const snapshot = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);
      lastSnapshot = snapshot;

      const energyOk =
        requirement.targetEnergy <= 0 || snapshot.energyAvailable >= requirement.targetEnergy;
      const bandwidthOk =
        requirement.targetBandwidth <= 0 ||
        snapshot.bandwidthAvailable >= requirement.targetBandwidth;

      logJson({
        level: "info",
        scope: "resource",
        stage: "delivery-poll",
        attempt: attempt + 1,
        operatorAddress,
        energyAvailable: snapshot.energyAvailable,
        bandwidthAvailable: snapshot.bandwidthAvailable,
        targetEnergy: requirement.targetEnergy,
        targetBandwidth: requirement.targetBandwidth,
        energyOk,
        bandwidthOk
      });

      if (energyOk && bandwidthOk) {
        await delay(RESOURCE_STABILIZATION_DELAY_MS);

        const stableSnapshot = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);
        const stableEnergyOk =
          requirement.targetEnergy <= 0 ||
          stableSnapshot.energyAvailable >= requirement.targetEnergy;
        const stableBandwidthOk =
          requirement.targetBandwidth <= 0 ||
          stableSnapshot.bandwidthAvailable >= requirement.targetBandwidth;

        logJson({
          level: "info",
          scope: "resource",
          stage: "delivery-stable-check",
          operatorAddress,
          energyAvailable: stableSnapshot.energyAvailable,
          bandwidthAvailable: stableSnapshot.bandwidthAvailable,
          targetEnergy: requirement.targetEnergy,
          targetBandwidth: requirement.targetBandwidth,
          stableEnergyOk,
          stableBandwidthOk
        });

        if (stableEnergyOk && stableBandwidthOk) {
          return stableSnapshot;
        }

        lastSnapshot = stableSnapshot;
      }
    }

    throw createTaggedError(
      `Account resource insufficient after rental. Energy=${lastSnapshot?.energyAvailable ?? 0}, Bandwidth=${lastSnapshot?.bandwidthAvailable ?? 0}, RequiredEnergy=${requirement.requiredEnergy}, RequiredBandwidth=${requirement.requiredBandwidth}, TargetEnergy=${requirement.targetEnergy}, TargetBandwidth=${requirement.targetBandwidth}`,
      {
        code: "ACCOUNT_RESOURCE_INSUFFICIENT_AFTER_RENTAL"
      }
    );
  }

  private async ensureResourcesForOperation(operationId: string): Promise<void> {
    const operatorAddress = this.getOperatorAddress();
    const requirement = this.buildCurrentRequirement();
    const before = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);

    const missingEnergy = Math.max(0, requirement.targetEnergy - before.energyAvailable);
    const missingBandwidth = Math.max(0, requirement.targetBandwidth - before.bandwidthAvailable);

    logJson({
      level: "info",
      scope: "resource",
      stage: "before-rental",
      operationId,
      operatorAddress,
      contractAddress: this.contractAddress,
      gasStationEnabled: this.gasStationEnabled,
      gasStationMinEnergy: this.gasStationMinEnergy,
      gasStationMinBandwidth: this.gasStationMinBandwidth,
      allocationMinEnergy: this.allocationMinEnergy,
      allocationMinBandwidth: this.allocationMinBandwidth,
      energyAvailable: before.energyAvailable,
      bandwidthAvailable: before.bandwidthAvailable,
      trxBalanceSun: before.trxBalanceSun,
      requiredEnergy: requirement.requiredEnergy,
      requiredBandwidth: requirement.requiredBandwidth,
      targetEnergy: requirement.targetEnergy,
      targetBandwidth: requirement.targetBandwidth,
      missingEnergy,
      missingBandwidth
    });

    if (missingEnergy <= 0 && missingBandwidth <= 0) {
      logJson({
        level: "info",
        scope: "resource",
        stage: "rental-skipped-enough-resources",
        operationId,
        operatorAddress,
        energyAvailable: before.energyAvailable,
        bandwidthAvailable: before.bandwidthAvailable
      });
      return;
    }

    if (!this.gasStationEnabled || !this.gasStationClient) {
      throw createTaggedError(
        `Account resource insufficient. Energy=${before.energyAvailable}, Bandwidth=${before.bandwidthAvailable}, RequiredEnergy=${requirement.requiredEnergy}, RequiredBandwidth=${requirement.requiredBandwidth}, TargetEnergy=${requirement.targetEnergy}, TargetBandwidth=${requirement.targetBandwidth}`,
        {
          code: "ACCOUNT_RESOURCE_INSUFFICIENT"
        }
      );
    }

    const energyToBuy =
      missingEnergy > 0
        ? Math.max(this.gasStationMinEnergy, missingEnergy, MIN_ENERGY_ORDER_FLOOR)
        : 0;

    const bandwidthToBuy =
      missingBandwidth > 0
        ? Math.max(this.gasStationMinBandwidth, missingBandwidth, MIN_BANDWIDTH_ORDER_FLOOR)
        : 0;

    const estimatedRentalCostSun = await this.estimateRentalCostSun({
      energyToBuy,
      bandwidthToBuy
    });

    logJson({
      level: "info",
      scope: "resource",
      stage: "rental-plan",
      operationId,
      operatorAddress,
      energyToBuy,
      bandwidthToBuy,
      estimatedRentalCostSun
    });

    await this.topUpGasStationFromOperatorIfNeeded(estimatedRentalCostSun);

    try {
      if (energyToBuy > 0) {
        const energyRequestId = buildGasRequestId("allocation", operationId, "energy");

        logJson({
          level: "info",
          scope: "resource",
          stage: "energy-order-start",
          operationId,
          operatorAddress,
          requestId: energyRequestId,
          energyToBuy,
          serviceChargeType: this.gasStationServiceChargeType
        });

        const energyOrder = await this.gasStationClient.createEnergyOrder({
          requestId: energyRequestId,
          receiveAddress: operatorAddress,
          energyNum: energyToBuy,
          serviceChargeType: this.gasStationServiceChargeType
        });

        logJson({
          level: "info",
          scope: "resource",
          stage: "energy-order-created",
          operationId,
          operatorAddress,
          requestId: energyRequestId,
          tradeNo: energyOrder.trade_no,
          energyToBuy
        });
      }

      if (bandwidthToBuy > 0) {
        const bandwidthRequestId = buildGasRequestId("allocation", operationId, "bandwidth");

        logJson({
          level: "info",
          scope: "resource",
          stage: "bandwidth-order-start",
          operationId,
          operatorAddress,
          requestId: bandwidthRequestId,
          bandwidthToBuy,
          serviceChargeType: this.gasStationServiceChargeType
        });

        const bandwidthOrder = await this.gasStationClient.createBandwidthOrder({
          requestId: bandwidthRequestId,
          receiveAddress: operatorAddress,
          netNum: bandwidthToBuy,
          serviceChargeType: this.gasStationServiceChargeType
        });

        logJson({
          level: "info",
          scope: "resource",
          stage: "bandwidth-order-created",
          operationId,
          operatorAddress,
          requestId: bandwidthRequestId,
          tradeNo: bandwidthOrder.trade_no,
          bandwidthToBuy
        });
      }
    } catch (error) {
      if (isRateLimitError(error)) {
        throw wrapAsRateLimitError(error, "GASSTATION_RATE_LIMIT");
      }

      logJson({
        level: "error",
        scope: "resource",
        stage: "resource-order-failed",
        operationId,
        operatorAddress,
        energyToBuy,
        bandwidthToBuy,
        error: getErrorMessage(error),
        code: extractErrorCode(error)
      });

      throw createTaggedError(
        `GasStation balance topped up or was already sufficient, but resource order failed. ${getErrorMessage(error)}`,
        {
          code: "GASSTATION_ORDER_FAILED",
          cause: error
        }
      );
    }

    const after = await this.waitForRequiredResources(requirement);

    logJson({
      level: "info",
      scope: "resource",
      stage: "after-rental",
      operationId,
      operatorAddress,
      energyAvailable: after.energyAvailable,
      bandwidthAvailable: after.bandwidthAvailable,
      trxBalanceSun: after.trxBalanceSun,
      requiredEnergy: requirement.requiredEnergy,
      requiredBandwidth: requirement.requiredBandwidth,
      targetEnergy: requirement.targetEnergy,
      targetBandwidth: requirement.targetBandwidth
    });
  }

  private async verifyResourcesStillReadyBeforeSend(): Promise<void> {
    const requirement = this.buildCurrentRequirement();
    const operatorAddress = this.getOperatorAddress();

    await delay(RESOURCE_RECHECK_BEFORE_SEND_DELAY_MS);

    const snapshot = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);

    const energyOk =
      requirement.requiredEnergy <= 0 || snapshot.energyAvailable >= requirement.requiredEnergy;
    const bandwidthOk =
      requirement.requiredBandwidth <= 0 ||
      snapshot.bandwidthAvailable >= requirement.requiredBandwidth;

    logJson({
      level: "info",
      scope: "resource",
      stage: "pre-send-check",
      operatorAddress,
      energyAvailable: snapshot.energyAvailable,
      bandwidthAvailable: snapshot.bandwidthAvailable,
      trxBalanceSun: snapshot.trxBalanceSun,
      requiredEnergy: requirement.requiredEnergy,
      requiredBandwidth: requirement.requiredBandwidth,
      energyOk,
      bandwidthOk
    });

    if (!energyOk || !bandwidthOk) {
      throw createTaggedError(
        `Account resources dropped before contract send. Energy=${snapshot.energyAvailable}, Bandwidth=${snapshot.bandwidthAvailable}, RequiredEnergy=${requirement.requiredEnergy}, RequiredBandwidth=${requirement.requiredBandwidth}`,
        {
          code: "ACCOUNT_RESOURCE_CONSUMED_BEFORE_SEND"
        }
      );
    }
  }

  private async sendWithdrawOwnerFundsWithoutLock(
    amountSun: string,
    feeLimitSun?: number
  ): Promise<WithdrawOwnerFundsResult> {
    const normalizedAmountSun = normalizeSunAmount(amountSun, "amountSun");
    const resolvedFeeLimitSun = normalizeFeeLimitSun(
      feeLimitSun,
      this.ownerWithdrawFeeLimitSun
    );

    await this.ensureResourcesForOperation(`owner-withdraw:${normalizedAmountSun}`);
    await this.verifyResourcesStillReadyBeforeSend();

    const contract = await this.contract();

    try {
      const sendResult = await withRateLimitRetry("withdrawOwnerFunds.send", async () => {
        return await contract.withdrawOwnerFunds(normalizedAmountSun).send({
          feeLimit: resolvedFeeLimitSun
        });
      });

      const txid = extractTxidFromSendTransactionResult(sendResult);

      return {
        txid: assertNonEmpty(txid || "", "txid"),
        amountSun: normalizedAmountSun
      };
    } catch (error) {
      if (isRateLimitError(error)) {
        throw wrapAsRateLimitError(error, "TRON_RATE_LIMIT");
      }

      if (isResourceSendError(error)) {
        throw createTaggedError(
          `Owner withdraw send failed because resources were still not sufficient at execution time. ${getErrorMessage(error)}`,
          {
            code: "ACCOUNT_RESOURCE_INSUFFICIENT_DURING_SEND",
            cause: error
          }
        );
      }

      throw error;
    }
  }

  private async tryAutoWithdrawOwnerFundsAfterAllocation(purchaseId: string): Promise<void> {
    if (!this.ownerAutoWithdrawEnabled) {
      return;
    }

    const isOwner = await this.isOperatorContractOwner();

    if (!isOwner) {
      return;
    }

    const availableSun = await this.getOwnerAvailableBalance();
    const available = BigInt(availableSun || "0");
    const minAmount = BigInt(String(this.ownerWithdrawMinSun));

    if (available <= 0n || available < minAmount) {
      return;
    }

    try {
      await this.sendWithdrawOwnerFundsWithoutLock(
        available.toString(),
        this.ownerWithdrawFeeLimitSun
      );
    } catch (error) {
      console.warn(
        JSON.stringify({
          level: "warn",
          scope: "owner-withdraw",
          stage: "auto-withdraw-failed",
          purchaseId,
          amountSun: available.toString(),
          error: getErrorMessage(error),
          code: extractErrorCode(error)
        })
      );
    }
  }

  async getAmbassadorBySlugHash(slugHash: string): Promise<ResolveAmbassadorBySlugHashResult> {
    const normalizedSlugHash = normalizeBytes32Hex(slugHash, "slugHash");
    const contract = await this.contract();

    const result = await withRateLimitRetry("getAmbassadorBySlugHash.call", async () => {
      return await contract.getAmbassadorBySlugHash(normalizedSlugHash).call();
    });

    return {
      slugHash: normalizedSlugHash,
      ambassadorWallet: normalizeReturnedAddress(this.tronWeb, result)
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

  async getOwnerAvailableBalance(): Promise<string> {
    const contract = await this.contract();

    const result = await withRateLimitRetry("ownerAvailableBalance.call", async () => {
      return await contract.ownerAvailableBalance().call();
    });

    return normalizeSunAmount(String(result ?? "0"), "ownerAvailableBalance");
  }

  async isOperatorContractOwner(): Promise<boolean> {
    const contract = await this.contract();

    const ownerRaw = await withRateLimitRetry("owner.call", async () => {
      return await contract.owner().call();
    });

    const operatorAddress = this.getOperatorAddress();
    const ownerComparable = toComparableAddress(this.tronWeb, ownerRaw);
    const operatorComparable = toComparableAddress(this.tronWeb, operatorAddress);

    return Boolean(ownerComparable && operatorComparable && ownerComparable === operatorComparable);
  }

  async withdrawOwnerFunds(
    amountSun: string,
    feeLimitSun?: number
  ): Promise<WithdrawOwnerFundsResult> {
    return this.runWithOperatorLock(async () => {
      return await this.sendWithdrawOwnerFundsWithoutLock(amountSun, feeLimitSun);
    });
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

    return this.runWithOperatorLock(async () => {
      const operatorAddress = this.getOperatorAddress();

      logJson({
        level: "info",
        scope: "allocation-send",
        stage: "start",
        purchaseId,
        buyerWallet,
        ambassadorWallet,
        purchaseAmountSun,
        ownerShareSun,
        feeLimitSun,
        operatorAddress,
        contractAddress: this.contractAddress
      });

      await this.ensureResourcesForOperation(purchaseId);
      await this.verifyResourcesStillReadyBeforeSend();

      const contract = await this.contract();

      try {
        logJson({
          level: "info",
          scope: "allocation-send",
          stage: "contract-send-start",
          purchaseId,
          operatorAddress,
          ambassadorWallet,
          purchaseAmountSun,
          ownerShareSun,
          feeLimitSun
        });

        const sendResult = await withRateLimitRetry("recordVerifiedPurchase.send", async () => {
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

        const txid = extractTxidFromSendTransactionResult(sendResult);
        const result = {
          txid: assertNonEmpty(txid || "", "txid")
        };

        logJson({
          level: "info",
          scope: "allocation-send",
          stage: "contract-send-success",
          purchaseId,
          operatorAddress,
          txid: result.txid
        });

        await this.tryAutoWithdrawOwnerFundsAfterAllocation(purchaseId);

        return result;
      } catch (error) {
        logJson({
          level: "error",
          scope: "allocation-send",
          stage: "contract-send-failed",
          purchaseId,
          operatorAddress,
          error: getErrorMessage(error),
          code: extractErrorCode(error)
        });

        if (isRateLimitError(error)) {
          throw wrapAsRateLimitError(error, "TRON_RATE_LIMIT");
        }

        if (isResourceSendError(error)) {
          throw createTaggedError(
            `Contract send failed because resources were still not sufficient at execution time. ${getErrorMessage(error)}`,
            {
              code: "ACCOUNT_RESOURCE_INSUFFICIENT_DURING_SEND",
              cause: error
            }
          );
        }

        throw error;
      }
    });
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
      gasStationServiceChargeType: config.gasStationServiceChargeType,
      ownerAutoWithdrawEnabled: config.ownerAutoWithdrawEnabled,
      ownerWithdrawMinSun: config.ownerWithdrawMinSun,
      ownerWithdrawFeeLimitSun: config.ownerWithdrawFeeLimitSun
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
