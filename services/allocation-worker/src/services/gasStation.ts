import crypto from "node:crypto";
import { ProxyAgent } from "undici";

export interface GasStationConfig {
  appId: string;
  secretKey: string;
  baseUrl?: string;
  proxyUrl?: string;
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

function assertNonEmpty(value: string | undefined, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeOptionalString(value?: string): string | undefined {
  const normalized = String(value || "").trim();
  return normalized || undefined;
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

function toStandardBase64(buffer: Buffer): string {
  return buffer.toString("base64");
}

function encryptAesEcbPkcs7Base64(plainText: string, secretKey: string): string {
  const key = Buffer.from(assertNonEmpty(secretKey, "secretKey"), "utf8");

  if (![16, 24, 32].includes(key.length)) {
    throw new Error("secretKey must be 16, 24, or 32 bytes long");
  }

  const plainBuffer = Buffer.from(plainText, "utf8");
  const padded = pkcs7Pad(plainBuffer);

  const cipher = crypto.createCipheriv(`aes-${key.length * 8}-ecb`, key, null);
  cipher.setAutoPadding(false);

  const encrypted = Buffer.concat([cipher.update(padded), cipher.final()]);
  return toStandardBase64(encrypted);
}

function createTaggedError(
  message: string,
  extras?: {
    code?: string;
    retryAfterMs?: number | null;
    cause?: unknown;
    status?: number;
    rawBody?: string | null;
  }
): Error {
  const error = new Error(message) as Error & {
    code?: string;
    retryAfterMs?: number | null;
    cause?: unknown;
    status?: number;
    rawBody?: string | null;
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

  if (extras?.status != null) {
    error.status = extras.status;
  }

  if (extras?.rawBody != null) {
    error.rawBody = extras.rawBody;
  }

  return error;
}

function parseRetryAfterMs(value: string | null): number | null {
  if (!value) {
    return null;
  }

  const trimmed = value.trim();

  if (!trimmed) {
    return null;
  }

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

async function requestJson<T>(
  url: string,
  init?: RequestInit,
  proxyUrl?: string
): Promise<T> {
  const dispatcher = proxyUrl ? new ProxyAgent(proxyUrl) : undefined;

  try {
    const response = await fetch(url, {
      ...init,
      headers: {
        Accept: "application/json",
        ...(init?.headers || {})
      },
      dispatcher
    } as RequestInit & { dispatcher?: ProxyAgent });

    const text = await response.text();

    let parsed: any = null;

    try {
      parsed = text ? JSON.parse(text) : null;
    } catch {
      throw createTaggedError(
        `GasStation returned non-JSON response: ${text || "empty response"}`,
        {
          code: "GASSTATION_INVALID_RESPONSE",
          status: response.status,
          rawBody: text || null
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
          status: response.status,
          rawBody: text || null
        });
      }

      throw createTaggedError(message, {
        code: `GASSTATION_HTTP_${response.status}`,
        retryAfterMs,
        cause: parsed,
        status: response.status,
        rawBody: text || null
      });
    }

    if (!parsed || typeof parsed !== "object") {
      throw createTaggedError("GasStation returned invalid response", {
        code: "GASSTATION_INVALID_RESPONSE",
        status: response.status,
        rawBody: text || null
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
        cause: parsed,
        status: response.status,
        rawBody: text || null
      });
    }

    return parsed.data as T;
  } catch (error) {
    if (error instanceof Error && error.message) {
      throw error;
    }

    throw createTaggedError("GasStation fetch failed", {
      code: "GASSTATION_FETCH_FAILED",
      cause: error
    });
  } finally {
    await dispatcher?.close();
  }
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
  private readonly proxyUrl?: string;

  constructor(config: GasStationConfig) {
    this.appId = assertNonEmpty(config.appId, "appId");
    this.secretKey = assertNonEmpty(config.secretKey, "secretKey");
    this.baseUrl = normalizeBaseUrl(config.baseUrl);
    this.proxyUrl = normalizeOptionalString(config.proxyUrl);
  }

  private buildEncryptedUrl(path: string, payload: Record<string, unknown>): string {
    const plainText = JSON.stringify(payload);
    const encrypted = encryptAesEcbPkcs7Base64(plainText, this.secretKey);

    const url = new URL(`${this.baseUrl}${path}`);
    url.searchParams.set("app_id", this.appId);
    url.searchParams.set("data", encrypted);
    return url.toString();
  }

  async getBalance(time?: string): Promise<GasStationBalanceResult> {
    const payload = {
      time: time ?? String(Math.floor(Date.now() / 1000))
    };

    const url = this.buildEncryptedUrl("/api/mpc/tron/gas/balance", payload);

    return requestJson<GasStationBalanceResult>(
      url,
      {
        method: "GET"
      },
      this.proxyUrl
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

    const url = this.buildEncryptedUrl("/api/tron/gas/order/price", payload);

    return requestJson<GasStationPriceResult>(
      url,
      {
        method: "GET"
      },
      this.proxyUrl
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

    const url = this.buildEncryptedUrl("/api/tron/gas/estimate", payload);

    return requestJson<GasStationEstimateResult>(
      url,
      {
        method: "GET"
      },
      this.proxyUrl
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

    const url = this.buildEncryptedUrl("/api/tron/gas/create_order", payload);

    return requestJson<GasStationCreateOrderResult>(
      url,
      {
        method: "POST"
      },
      this.proxyUrl
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

    const url = this.buildEncryptedUrl("/api/tron/gas/create_order", payload);

    return requestJson<GasStationCreateOrderResult>(
      url,
      {
        method: "POST"
      },
      this.proxyUrl
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
    baseUrl: process.env.GASSTATION_API_BASE_URL ?? process.env.GASSTATION_BASE_URL,
    proxyUrl:
      process.env.QUOTAGUARDSTATIC_URL ??
      process.env.QUOTAGUARD_URL ??
      process.env.FIXIE_URL
  });
}
