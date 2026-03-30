import crypto from "node:crypto";

export interface GasStationConfig {
  appId: string;
  secretKey: string;
  baseUrl?: string;
}

export interface GasStationBalanceResult {
  symbol: string;
  balance: string;
}

export interface GasStationEstimateResult {
  contract_address: string;
  address_to: string;
  receive_address: string;
  amount: string;
  energy_amount: string;
  active_amount: string;
  energy_num: number;
  energy_price: string;
  service_charge_type: string;
}

export interface GasStationPriceItem {
  expire_min: string;
  service_charge_type: string;
  price: string;
  remaining_number: string;
}

export interface GasStationPriceResult {
  list: GasStationPriceItem[];
}

export interface GasStationCreateOrderResult {
  trade_no: string;
}

type GasStationEncodingMode = "base64url" | "base64";

function assertNonEmpty(value: string | undefined, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeBaseUrl(value?: string): string {
  return String(value || "https://openapi.gasstation.ai").trim().replace(/\/+$/, "");
}

function pkcs7Pad(buffer: Buffer): Buffer {
  const blockSize = 16;
  const remainder = buffer.length % blockSize;
  const padLength = remainder === 0 ? blockSize : blockSize - remainder;
  const padding = Buffer.alloc(padLength, padLength);
  return Buffer.concat([buffer, padding]);
}

function toBase64(buffer: Buffer): string {
  return buffer.toString("base64");
}

function toBase64UrlSafe(buffer: Buffer): string {
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function encryptAesEcbPkcs7(
  plainText: string,
  secretKey: string,
  mode: GasStationEncodingMode
): string {
  const key = Buffer.from(assertNonEmpty(secretKey, "secretKey"), "utf8");

  if (![16, 24, 32].includes(key.length)) {
    throw new Error("secretKey must be 16, 24, or 32 bytes long");
  }

  const plainBuffer = Buffer.from(plainText, "utf8");
  const padded = pkcs7Pad(plainBuffer);

  const cipher = crypto.createCipheriv(`aes-${key.length * 8}-ecb`, key, null);
  cipher.setAutoPadding(false);

  const encrypted = Buffer.concat([cipher.update(padded), cipher.final()]);

  return mode === "base64url" ? toBase64UrlSafe(encrypted) : toBase64(encrypted);
}

function createTaggedError(
  message: string,
  extras?: {
    code?: string;
    retryAfterMs?: number | null;
    cause?: unknown;
    status?: number | null;
  }
): Error {
  const error = new Error(message) as Error & {
    code?: string;
    retryAfterMs?: number | null;
    cause?: unknown;
    status?: number | null;
  };

  if (extras?.code) error.code = extras.code;
  if (extras?.retryAfterMs != null) error.retryAfterMs = extras.retryAfterMs;
  if (extras?.cause !== undefined) error.cause = extras.cause;
  if (extras?.status != null) error.status = extras.status;

  return error;
}

function parseRetryAfterMs(value: string | null): number | null {
  if (!value) return null;

  const trimmed = value.trim();
  if (!trimmed) return null;

  const seconds = Number(trimmed);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.ceil(seconds * 1000);
  }

  const dateMs = Date.parse(trimmed);
  if (Number.isFinite(dateMs)) {
    return Math.max(0, dateMs - Date.now());
  }

  return null;
}

async function requestJsonOnce<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  const text = await response.text();

  let parsed: any = null;

  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    throw createTaggedError(
      `GasStation returned non-JSON response: ${text || "empty response"}`,
      {
        code: "GASSTATION_INVALID_RESPONSE",
        status: response.status || null
      }
    );
  }

  if (!response.ok) {
    const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));
    const message = parsed?.msg
      ? `GasStation HTTP ${response.status}: ${parsed.msg}`
      : `GasStation HTTP ${response.status}`;

    if (response.status === 429) {
      throw createTaggedError(message, {
        code: "GASSTATION_RATE_LIMIT",
        retryAfterMs,
        cause: parsed,
        status: response.status
      });
    }

    throw createTaggedError(message, {
      code: `GASSTATION_HTTP_${response.status}`,
      retryAfterMs,
      cause: parsed,
      status: response.status
    });
  }

  if (!parsed || typeof parsed !== "object") {
    throw createTaggedError("GasStation returned invalid response", {
      code: "GASSTATION_INVALID_RESPONSE",
      status: response.status || null
    });
  }

  if (parsed.code !== 0) {
    const message = parsed.msg
      ? `GasStation error ${parsed.code}: ${parsed.msg}`
      : `GasStation error ${parsed.code}`;

    const normalizedMessage = String(parsed.msg || "").toLowerCase();
    const isRateLimited =
      parsed.code === 429 ||
      normalizedMessage.includes("too many requests") ||
      normalizedMessage.includes("rate limit") ||
      normalizedMessage.includes("429");

    throw createTaggedError(message, {
      code: isRateLimited ? "GASSTATION_RATE_LIMIT" : `GASSTATION_ERROR_${parsed.code}`,
      cause: parsed
    });
  }

  return parsed.data as T;
}

function normalizePositiveInteger(value: number, fieldName: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${fieldName} must be a positive number`);
  }

  return Math.ceil(value);
}

export class GasStationClient {
  private readonly appId: string;
  private readonly secretKey: string;
  private readonly baseUrl: string;

  constructor(config: GasStationConfig) {
    this.appId = assertNonEmpty(config.appId, "appId");
    this.secretKey = assertNonEmpty(config.secretKey, "secretKey");
    this.baseUrl = normalizeBaseUrl(config.baseUrl);
  }

  private buildEncryptedUrl(
    path: string,
    payload: Record<string, unknown>,
    mode: GasStationEncodingMode
  ): string {
    const plainText = JSON.stringify(payload);
    const encrypted = encryptAesEcbPkcs7(plainText, this.secretKey, mode);

    const url = new URL(`${this.baseUrl}${path}`);
    url.searchParams.set("app_id", this.appId);
    url.searchParams.set("data", encrypted);
    return url.toString();
  }

  private async requestWithFallback<T>(
    path: string,
    payload: Record<string, unknown>,
    init?: RequestInit
  ): Promise<T> {
    const firstUrl = this.buildEncryptedUrl(path, payload, "base64url");

    try {
      return await requestJsonOnce<T>(firstUrl, init);
    } catch (error) {
      const status =
        error && typeof error === "object" && "status" in error
          ? Number((error as { status?: unknown }).status)
          : null;

      if (status !== 403) {
        throw error;
      }
    }

    const fallbackUrl = this.buildEncryptedUrl(path, payload, "base64");

    return requestJsonOnce<T>(fallbackUrl, init);
  }

  async getBalance(time?: string): Promise<GasStationBalanceResult> {
    const payload = {
      time: time ?? String(Math.floor(Date.now() / 1000))
    };

    return this.requestWithFallback<GasStationBalanceResult>(
      "/api/mpc/tron/gas/balance",
      payload,
      { method: "GET" }
    );
  }

  async getPrice(input?: {
    serviceChargeType?: string;
    resourceValue?: number;
  }): Promise<GasStationPriceResult> {
    const payload: Record<string, unknown> = {};

    if (input?.serviceChargeType) {
      payload.service_charge_type = assertNonEmpty(
        input.serviceChargeType,
        "serviceChargeType"
      );
    }

    if (input?.resourceValue != null) {
      payload.value = normalizePositiveInteger(input.resourceValue, "resourceValue");
    }

    return this.requestWithFallback<GasStationPriceResult>(
      "/api/tron/gas/order/price",
      payload,
      { method: "GET" }
    );
  }

  async estimateEnergyOrder(input: {
    receiveAddress: string;
    addressTo: string;
    contractAddress: string;
    serviceChargeType?: string;
  }): Promise<GasStationEstimateResult> {
    const payload = {
      receive_address: assertNonEmpty(input.receiveAddress, "receiveAddress"),
      address_to: assertNonEmpty(input.addressTo, "addressTo"),
      contract_address: assertNonEmpty(input.contractAddress, "contractAddress"),
      service_charge_type: assertNonEmpty(
        input.serviceChargeType ?? "10010",
        "serviceChargeType"
      )
    };

    return this.requestWithFallback<GasStationEstimateResult>(
      "/api/tron/gas/estimate",
      payload,
      { method: "GET" }
    );
  }

  async createEnergyOrder(input: {
    requestId: string;
    receiveAddress: string;
    energyNum: number;
    serviceChargeType?: string;
  }): Promise<GasStationCreateOrderResult> {
    const energyNum = normalizePositiveInteger(input.energyNum, "energyNum");

    if (energyNum < 64400) {
      throw new Error("energyNum must be at least 64400");
    }

    const payload = {
      request_id: assertNonEmpty(input.requestId, "requestId"),
      receive_address: assertNonEmpty(input.receiveAddress, "receiveAddress"),
      buy_type: 0,
      service_charge_type: assertNonEmpty(
        input.serviceChargeType ?? "10010",
        "serviceChargeType"
      ),
      energy_num: energyNum
    };

    return this.requestWithFallback<GasStationCreateOrderResult>(
      "/api/tron/gas/create_order",
      payload,
      { method: "POST" }
    );
  }

  async createBandwidthOrder(input: {
    requestId: string;
    receiveAddress: string;
    netNum: number;
    serviceChargeType?: string;
  }): Promise<GasStationCreateOrderResult> {
    const netNum = normalizePositiveInteger(input.netNum, "netNum");

    if (netNum < 5000) {
      throw new Error("netNum must be at least 5000");
    }

    const payload = {
      request_id: assertNonEmpty(input.requestId, "requestId"),
      receive_address: assertNonEmpty(input.receiveAddress, "receiveAddress"),
      buy_type: 0,
      service_charge_type: assertNonEmpty(
        input.serviceChargeType ?? "10010",
        "serviceChargeType"
      ),
      net_num: netNum
    };

    return this.requestWithFallback<GasStationCreateOrderResult>(
      "/api/tron/gas/create_order",
      payload,
      { method: "POST" }
    );
  }
}

export function createGasStationClientFromEnv(): GasStationClient {
  return new GasStationClient({
    appId: assertNonEmpty(
      process.env.GASSTATION_API_KEY ?? process.env.GASSTATION_APP_ID,
      "GASSTATION_API_KEY"
    ),
    secretKey: assertNonEmpty(
      process.env.GASSTATION_API_SECRET ?? process.env.GASSTATION_SECRET_KEY,
      "GASSTATION_API_SECRET"
    ),
    baseUrl: process.env.GASSTATION_API_BASE_URL ?? process.env.GASSTATION_BASE_URL
  });
}
