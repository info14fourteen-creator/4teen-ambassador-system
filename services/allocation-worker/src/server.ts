import http from "node:http";
import crypto from "node:crypto";
import { URL } from "node:url";
import TronWebModule from "tronweb";
import { assertValidSlug, normalizeSlug } from "../../../shared/utils/slug";
import { createAllocationWorker } from "./index";
import { BuyTokensScanner } from "./run-scan";
import {
  completeAmbassadorRegistration,
  getAmbassadorPublicProfileBySlug,
  initAmbassadorRegistryTables,
  isSlugTaken
} from "./db/ambassadors";
import { initPurchaseTables } from "./db/purchases";
import { rentDailyEnergy } from "./jobs/rentEnergy";

interface EnvConfig {
  port: number;
  tronFullHost: string;
  tronPrivateKey: string;
  controllerContractAddress?: string;
  tokenContractAddress?: string;
  scanPageSize: number;
  allowedOrigins: string[];
  cronSecret?: string;
  tronResourceAddress?: string;
  tronResourceApiUrl: string;
  tronApiKey?: string;
}

type TronWebConstructor = new (config: {
  fullHost: string;
  privateKey: string;
}) => any;

interface AccountResourceResponse {
  freeNetLimit?: number;
  freeNetUsed?: number;
  NetLimit?: number;
  NetUsed?: number;
  EnergyLimit?: number;
  EnergyUsed?: number;
}

function getTronWebConstructor(): TronWebConstructor {
  const candidate =
    (TronWebModule as any)?.TronWeb ??
    (TronWebModule as any)?.default ??
    TronWebModule;

  if (typeof candidate !== "function") {
    throw new Error("Unable to resolve TronWeb constructor");
  }

  return candidate as TronWebConstructor;
}

function assertNonEmpty(value: string | undefined, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
}

function parsePositiveInteger(
  value: string | undefined,
  fallback: number,
  fieldName: string
): number {
  if (value == null || String(value).trim() === "") {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${fieldName} must be a positive integer`);
  }

  return parsed;
}

function parseAllowedOrigins(value: string | undefined): string[] {
  const defaults = [
    "https://4teen.me",
    "https://www.4teen.me",
    "http://localhost:3000",
    "http://127.0.0.1:3000"
  ];

  if (!value || !value.trim()) {
    return defaults;
  }

  const parsed = value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

  return parsed.length ? parsed : defaults;
}

function normalizeBaseUrl(value: string | undefined, fallback: string): string {
  const normalized = String(value || fallback).trim();
  return normalized.replace(/\/+$/, "");
}

function loadEnv(): EnvConfig {
  const config: EnvConfig = {
    port: parsePositiveInteger(process.env.PORT, 3000, "PORT"),
    tronFullHost: assertNonEmpty(process.env.TRON_FULL_HOST, "TRON_FULL_HOST"),
    tronPrivateKey: assertNonEmpty(process.env.TRON_PRIVATE_KEY, "TRON_PRIVATE_KEY"),
    scanPageSize: parsePositiveInteger(process.env.SCAN_PAGE_SIZE, 50, "SCAN_PAGE_SIZE"),
    allowedOrigins: parseAllowedOrigins(process.env.ALLOWED_ORIGINS),
    tronResourceApiUrl: normalizeBaseUrl(
      process.env.TRON_RESOURCE_API_URL,
      "https://api.trongrid.io"
    )
  };

  const controllerContractAddress = process.env.FOURTEEN_CONTROLLER_CONTRACT?.trim();
  const tokenContractAddress = process.env.FOURTEEN_TOKEN_CONTRACT?.trim();
  const cronSecret = process.env.CRON_SECRET?.trim();
  const tronResourceAddress =
    process.env.TRON_RESOURCE_ADDRESS?.trim() ||
    process.env.CONTROLLER_OWNER_WALLET?.trim() ||
    process.env.CONTROLLER_WALLET?.trim() ||
    process.env.OWNER_WALLET?.trim();
  const tronApiKey = process.env.TRONGRID_API_KEY?.trim();

  if (controllerContractAddress) {
    config.controllerContractAddress = controllerContractAddress;
  }

  if (tokenContractAddress) {
    config.tokenContractAddress = tokenContractAddress;
  }

  if (cronSecret) {
    config.cronSecret = cronSecret;
  }

  if (tronResourceAddress) {
    config.tronResourceAddress = tronResourceAddress;
  }

  if (tronApiKey) {
    config.tronApiKey = tronApiKey;
  }

  return config;
}

function getCorsOrigin(req: http.IncomingMessage, env: EnvConfig): string {
  const origin = typeof req.headers.origin === "string" ? req.headers.origin.trim() : "";

  if (origin && env.allowedOrigins.includes(origin)) {
    return origin;
  }

  return env.allowedOrigins[0] || "https://4teen.me";
}

function setCorsHeaders(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  env: EnvConfig
): void {
  res.setHeader("Access-Control-Allow-Origin", getCorsOrigin(req, env));
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Cron-Secret");
}

function sendJson(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  env: EnvConfig,
  statusCode: number,
  payload: unknown
): void {
  const body = JSON.stringify(payload, null, 2);

  setCorsHeaders(req, res, env);

  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body).toString()
  });

  res.end(body);
}

function readJsonBody(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];

    req.on("data", (chunk) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });

    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf-8").trim();

      if (!raw) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new Error("Invalid JSON body"));
      }
    });

    req.on("error", reject);
  });
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

  return "Unknown error";
}

function normalizeIncomingSlug(value: unknown): string {
  const raw = assertNonEmpty(normalizeOptionalString(value), "slug");
  return assertValidSlug(normalizeSlug(raw));
}

function normalizeSlugHash(value: unknown): string {
  const raw = assertNonEmpty(normalizeOptionalString(value), "slugHash").toLowerCase();

  if (!/^0x[0-9a-f]{64}$/.test(raw)) {
    throw new Error("slugHash must be a bytes32 hex string");
  }

  return raw;
}

function buildReferralLink(slug: string): string {
  return `?r=${encodeURIComponent(slug)}`;
}

function safeEqual(a: string, b: string): boolean {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);

  if (aBuf.length !== bBuf.length) {
    return false;
  }

  return crypto.timingSafeEqual(aBuf, bBuf);
}

function requireCronSecret(req: http.IncomingMessage, env: EnvConfig): void {
  if (!env.cronSecret) {
    throw new Error("CRON_SECRET is not configured");
  }

  const provided =
    (typeof req.headers["x-cron-secret"] === "string" && req.headers["x-cron-secret"].trim()) ||
    "";

  if (!provided || !safeEqual(provided, env.cronSecret)) {
    throw new Error("Unauthorized cron request");
  }
}

function getTronHeaders(env: EnvConfig): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json"
  };

  if (env.tronApiKey) {
    headers["TRON-PRO-API-KEY"] = env.tronApiKey;
  }

  return headers;
}

async function getAccountResources(env: EnvConfig, addressBase58: string) {
  const TronWeb = getTronWebConstructor();
  const tronWeb = new TronWeb({
    fullHost: env.tronFullHost,
    privateKey: env.tronPrivateKey
  });

  const addressHex = tronWeb.address.toHex(addressBase58);
  const response = await fetch(`${env.tronResourceApiUrl}/wallet/getaccountresource`, {
    method: "POST",
    headers: getTronHeaders(env),
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
  const freeNetLimit = Number(data.freeNetLimit || 0);
  const freeNetUsed = Number(data.freeNetUsed || 0);
  const netLimit = Number(data.NetLimit || 0);
  const netUsed = Number(data.NetUsed || 0);

  const availableEnergy = Math.max(0, energyLimit - energyUsed);
  const availableFreeBandwidth = Math.max(0, freeNetLimit - freeNetUsed);
  const availablePaidBandwidth = Math.max(0, netLimit - netUsed);
  const availableBandwidth = availableFreeBandwidth + availablePaidBandwidth;

  return {
    address: addressBase58,
    energy: {
      limit: energyLimit,
      used: energyUsed,
      available: availableEnergy
    },
    bandwidth: {
      freeLimit: freeNetLimit,
      freeUsed: freeNetUsed,
      paidLimit: netLimit,
      paidUsed: netUsed,
      availableFree: availableFreeBandwidth,
      availablePaid: availablePaidBandwidth,
      availableTotal: availableBandwidth
    }
  };
}

async function bootstrap() {
  const env = loadEnv();
  const TronWeb = getTronWebConstructor();

  await initAmbassadorRegistryTables();
  await initPurchaseTables();

  const tronWeb = new TronWeb({
    fullHost: env.tronFullHost,
    privateKey: env.tronPrivateKey
  });

  const worker = createAllocationWorker({
    tronWeb,
    controllerContractAddress: env.controllerContractAddress
  });

  const scanner = new BuyTokensScanner({
    tronWeb,
    processor: worker.processor,
    store: worker.store,
    tokenContractAddress: env.tokenContractAddress,
    pageSize: env.scanPageSize
  });

  const server = http.createServer(async (req, res) => {
    try {
      const method = req.method || "GET";
      const host = req.headers.host || "localhost";
      const requestUrl = new URL(req.url || "/", `http://${host}`);
      const pathname = requestUrl.pathname;

      if (method === "OPTIONS") {
        setCorsHeaders(req, res, env);
        res.writeHead(204);
        res.end();
        return;
      }

      if (method === "GET" && pathname === "/health") {
        sendJson(req, res, env, 200, {
          ok: true,
          service: "allocation-worker",
          timestamp: Date.now()
        });
        return;
      }

      if (method === "GET" && pathname === "/resources") {
        const address =
          normalizeOptionalString(requestUrl.searchParams.get("address")) ||
          env.tronResourceAddress;

        if (!address) {
          throw new Error(
            "Resource address is not configured. Set TRON_RESOURCE_ADDRESS or pass ?address="
          );
        }

        const result = await getAccountResources(env, address);

        sendJson(req, res, env, 200, {
          ok: true,
          result
        });
        return;
      }

      if (method === "GET" && pathname === "/slug/check") {
        const slug = normalizeIncomingSlug(requestUrl.searchParams.get("slug"));
        const taken = await isSlugTaken(slug);

        sendJson(req, res, env, 200, {
          ok: true,
          slug,
          available: !taken
        });
        return;
      }

      if (method === "POST" && pathname === "/ambassador/register-complete") {
        const body = await readJsonBody(req);

        const slug = normalizeIncomingSlug(body.slug);
        const slugHash = normalizeSlugHash(body.slugHash);
        const wallet = assertNonEmpty(normalizeOptionalString(body.wallet), "wallet");

        const created = await completeAmbassadorRegistration({
          slug,
          slugHash,
          wallet,
          now: Date.now()
        });

        sendJson(req, res, env, 200, {
          ok: true,
          result: {
            slug: created.publicProfile.slug,
            slugHash: created.publicProfile.slugHash,
            status: created.publicProfile.status,
            referralLink: buildReferralLink(created.publicProfile.slug)
          }
        });
        return;
      }

      if (method === "GET" && pathname === "/ambassador/profile") {
        const slug = normalizeIncomingSlug(requestUrl.searchParams.get("slug"));
        const profile = await getAmbassadorPublicProfileBySlug(slug);

        if (!profile) {
          sendJson(req, res, env, 404, {
            ok: false,
            error: "Ambassador profile not found"
          });
          return;
        }

        sendJson(req, res, env, 200, {
          ok: true,
          result: {
            slug: profile.slug,
            slugHash: profile.slugHash,
            status: profile.status,
            referralLink: buildReferralLink(profile.slug)
          }
        });
        return;
      }

      if (method === "POST" && pathname === "/attribution") {
        const body = await readJsonBody(req);

        const txHash = assertNonEmpty(normalizeOptionalString(body.txHash), "txHash");
        const buyerWallet = assertNonEmpty(
          normalizeOptionalString(body.buyerWallet),
          "buyerWallet"
        );
        const slug = normalizeIncomingSlug(body.slug);

        const result = await worker.processor.processFrontendAttribution({
          txHash,
          buyerWallet,
          slug,
          now: Date.now()
        });

        sendJson(req, res, env, 200, {
          ok: true,
          result
        });
        return;
      }

      if (method === "POST" && pathname === "/scan") {
        const body = await readJsonBody(req);

        const fingerprint =
          typeof body.fingerprint === "string" && body.fingerprint.trim()
            ? body.fingerprint.trim()
            : undefined;

        const result = await scanner.fetchEvents({
          fingerprint
        });

        sendJson(req, res, env, 200, {
          ok: true,
          result
        });
        return;
      }

      if (method === "POST" && pathname === "/replay-failed") {
        const body = await readJsonBody(req);
        const purchaseId = assertNonEmpty(
          normalizeOptionalString(body.purchaseId),
          "purchaseId"
        );

        const feeLimitSun =
          body.feeLimitSun !== undefined
            ? parsePositiveInteger(String(body.feeLimitSun), 0, "feeLimitSun")
            : undefined;

        const result = await worker.processor.replayFailedAllocation(
          purchaseId,
          feeLimitSun,
          Date.now()
        );

        sendJson(req, res, env, 200, {
          ok: true,
          result
        });
        return;
      }

      if (method === "GET" && pathname === "/failures") {
        const failures = await worker.store.listReplayableFailures();

        sendJson(req, res, env, 200, {
          ok: true,
          count: failures.length,
          failures
        });
        return;
      }

      if (method === "POST" && pathname === "/rent-energy") {
        const result = await rentDailyEnergy();

        sendJson(req, res, env, result.ok ? 200 : 400, {
          ok: result.ok,
          result
        });
        return;
      }

      if (method === "POST" && pathname === "/cron/daily-maintenance") {
        requireCronSecret(req, env);

        const rentResult = await rentDailyEnergy().catch((error) => ({
          ok: false,
          stage: "checked-balance" as const,
          gasBalance: null,
          tradeNo: null,
          reason: toErrorMessage(error)
        }));

        const scanResult = await scanner.fetchEvents({}).catch((error) => ({
          ok: false,
          error: toErrorMessage(error)
        }));

        sendJson(req, res, env, 200, {
          ok: true,
          result: {
            rentEnergy: rentResult,
            scan: scanResult,
            timestamp: Date.now()
          }
        });
        return;
      }

      sendJson(req, res, env, 404, {
        ok: false,
        error: "Not found"
      });
    } catch (error) {
      const message = toErrorMessage(error);
      const statusCode = message === "Unauthorized cron request" ? 401 : 500;

      sendJson(req, res, env, statusCode, {
        ok: false,
        error: message
      });
    }
  });

  server.listen(env.port, () => {
    console.log(
      JSON.stringify({
        ok: true,
        message: "allocation-worker started",
        port: env.port,
        allowedOrigins: env.allowedOrigins,
        resourceAddress: env.tronResourceAddress || null
      })
    );
  });
}

void bootstrap();
