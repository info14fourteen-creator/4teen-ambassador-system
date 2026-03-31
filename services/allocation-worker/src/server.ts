import http from "node:http";
import { URL } from "node:url";
import TronWebModule from "tronweb";
import { assertValidSlug, normalizeSlug } from "../../../shared/utils/slug";
import { createAllocationWorker } from "./index";
import { BuyTokensScanner } from "./run-scan";
import { createCabinetService } from "./services/cabinet";
import { createGasStationClientFromEnv } from "./services/gasStation";
import {
  completeAmbassadorRegistration,
  getAmbassadorPublicProfileBySlug,
  getAmbassadorRegistryRecordByWallet,
  initAmbassadorRegistryTables,
  isSlugTaken
} from "./db/ambassadors";
import { initPurchaseTables } from "./db/purchases";

interface EnvConfig {
  port: number;
  tronFullHost: string;
  tronPrivateKey: string;
  controllerContractAddress?: string;
  tokenContractAddress?: string;
  scanPageSize: number;
  allowedOrigins: string[];
  gasStationEnabled: boolean;
  gasStationApiBaseUrl?: string;
  gasStationApiKey?: string;
  gasStationApiSecret?: string;
  gasStationMinBandwidth: number;
  gasStationMinEnergy: number;
  allocationMinBandwidth: number;
  allocationMinEnergy: number;
  gasStationServiceChargeType: string;
}

type TronWebConstructor = new (config: {
  fullHost: string;
  privateKey: string;
}) => any;

const DEFAULT_CONTROLLER_CONTRACT = "TF8yhohRfMxsdVRr7fFrYLh5fxK8sAFkeZ";
const SUN_PER_TRX = 1_000_000;
const GASSTATION_LOW_BALANCE_SUN = 8_500_000;
const OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN = 11_000_000;
const OPERATOR_REMAINING_RESERVE_SUN = 2_000_000;

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

function parseBoolean(value: string | undefined, fallback = false): boolean {
  if (value == null || value.trim() === "") {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();

  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }

  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }

  throw new Error(`Invalid boolean value: ${value}`);
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

function loadEnv(): EnvConfig {
  const gasStationEnabled = parseBoolean(process.env.GASSTATION_ENABLED, false);

  const config: EnvConfig = {
    port: parsePositiveInteger(process.env.PORT, 3000, "PORT"),
    tronFullHost: assertNonEmpty(process.env.TRON_FULL_HOST, "TRON_FULL_HOST"),
    tronPrivateKey: assertNonEmpty(process.env.TRON_PRIVATE_KEY, "TRON_PRIVATE_KEY"),
    scanPageSize: parsePositiveInteger(process.env.SCAN_PAGE_SIZE, 50, "SCAN_PAGE_SIZE"),
    allowedOrigins: parseAllowedOrigins(process.env.ALLOWED_ORIGINS),

    gasStationEnabled,
    gasStationApiBaseUrl: normalizeOptionalString(
      process.env.GASSTATION_API_BASE_URL ?? process.env.GASSTATION_BASE_URL
    ),
    gasStationApiKey: normalizeOptionalString(
      process.env.GASSTATION_API_KEY ?? process.env.GASSTATION_APP_ID
    ),
    gasStationApiSecret: normalizeOptionalString(
      process.env.GASSTATION_API_SECRET ?? process.env.GASSTATION_SECRET_KEY
    ),

    gasStationMinBandwidth: parsePositiveInteger(
      process.env.GASSTATION_MIN_BANDWIDTH,
      5000,
      "GASSTATION_MIN_BANDWIDTH"
    ),
    gasStationMinEnergy: parsePositiveInteger(
      process.env.GASSTATION_MIN_ENERGY,
      64400,
      "GASSTATION_MIN_ENERGY"
    ),
    allocationMinBandwidth: parsePositiveInteger(
      process.env.ALLOCATION_MIN_BANDWIDTH,
      444,
      "ALLOCATION_MIN_BANDWIDTH"
    ),
    allocationMinEnergy: parsePositiveInteger(
      process.env.ALLOCATION_MIN_ENERGY,
      168502,
      "ALLOCATION_MIN_ENERGY"
    ),
    gasStationServiceChargeType: String(
      process.env.GASSTATION_SERVICE_CHARGE_TYPE || "10010"
    ).trim() || "10010"
  };

  const controllerContractAddress = process.env.FOURTEEN_CONTROLLER_CONTRACT?.trim();
  const tokenContractAddress = process.env.FOURTEEN_TOKEN_CONTRACT?.trim();

  if (controllerContractAddress) {
    config.controllerContractAddress = controllerContractAddress;
  }

  if (tokenContractAddress) {
    config.tokenContractAddress = tokenContractAddress;
  }

  if (config.gasStationEnabled) {
    config.gasStationApiBaseUrl = assertNonEmpty(
      config.gasStationApiBaseUrl,
      "GASSTATION_API_BASE_URL"
    );
    config.gasStationApiKey = assertNonEmpty(
      config.gasStationApiKey,
      "GASSTATION_API_KEY"
    );
    config.gasStationApiSecret = assertNonEmpty(
      config.gasStationApiSecret,
      "GASSTATION_API_SECRET"
    );
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
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
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

function normalizeIncomingWallet(value: unknown): string {
  return assertNonEmpty(normalizeOptionalString(value), "wallet");
}

function buildReferralLink(slug: string): string {
  return `?r=${encodeURIComponent(slug)}`;
}

function isBase58Address(value: string): boolean {
  return /^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(value);
}

function isHexAddress(value: string): boolean {
  return /^41[0-9a-fA-F]{40}$/.test(value);
}

function normalizeAddress(value: string, fieldName: string): string {
  const normalized = assertNonEmpty(value, fieldName);

  if (!isBase58Address(normalized) && !isHexAddress(normalized)) {
    throw new Error(`${fieldName} must be a valid TRON address`);
  }

  return normalized;
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

function sunToTrxString(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return "0";
  }

  const whole = Math.floor(value / SUN_PER_TRX);
  const fraction = String(value % SUN_PER_TRX).padStart(6, "0").replace(/0+$/, "");
  return fraction ? `${whole}.${fraction}` : String(whole);
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

  const resolvedControllerContractAddress =
    env.controllerContractAddress || DEFAULT_CONTROLLER_CONTRACT;

  const worker = createAllocationWorker({
    tronWeb,
    controllerContractAddress: resolvedControllerContractAddress,
    logger: {
      info(payload) {
        console.log(JSON.stringify({ level: "info", ...payload }));
      },
      warn(payload) {
        console.warn(JSON.stringify({ level: "warn", ...payload }));
      },
      error(payload) {
        console.error(JSON.stringify({ level: "error", ...payload }));
      }
    }
  });

  const cabinetService = createCabinetService({
    store: worker.store,
    tronWeb,
    controllerContractAddress: resolvedControllerContractAddress,
    processor: worker.processor
  });

  const scanner = new BuyTokensScanner({
    tronWeb,
    processor: worker.processor as any,
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
          timestamp: Date.now(),
          controllerContractAddress: resolvedControllerContractAddress,
          gasStation: {
            enabled: env.gasStationEnabled,
            apiBaseUrl: env.gasStationApiBaseUrl || null,
            minBandwidth: env.gasStationMinBandwidth,
            minEnergy: env.gasStationMinEnergy,
            serviceChargeType: env.gasStationServiceChargeType
          },
          allocationThresholds: {
            minBandwidth: env.allocationMinBandwidth,
            minEnergy: env.allocationMinEnergy
          }
        });
        return;
      }

      if (method === "GET" && pathname === "/debug/gasstation/balance") {
        const client = createGasStationClientFromEnv();
        const gasBalance = await client.getBalance();

        const operatorAddress = normalizeAddress(
          tronWeb?.defaultAddress?.base58 || tronWeb?.defaultAddress?.hex || "",
          "operatorAddress"
        );

        const operatorBalanceSun = Number(
          await tronWeb.trx.getBalance(operatorAddress)
        );

        const serviceBalanceSun = parseTrxAmountToSun(
          gasBalance.balance,
          "gasStation.balance"
        );

        const depositAddress = normalizeAddress(
          (gasBalance as any).deposit_address,
          "deposit_address"
        );

        const availableForTopUpSun = Math.max(
          0,
          operatorBalanceSun - OPERATOR_REMAINING_RESERVE_SUN
        );

        const needsTopUp = serviceBalanceSun < GASSTATION_LOW_BALANCE_SUN;
        const canTopUp =
          operatorBalanceSun >= OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN &&
          availableForTopUpSun >= GASSTATION_LOW_BALANCE_SUN;

        const recommendedTopUpSun = canTopUp ? availableForTopUpSun : 0;

        sendJson(req, res, env, 200, {
          ok: true,
          result: {
            gasStation: {
              balanceSun: serviceBalanceSun,
              balanceTrx: sunToTrxString(serviceBalanceSun),
              depositAddress,
              lowBalanceThresholdSun: GASSTATION_LOW_BALANCE_SUN,
              lowBalanceThresholdTrx: sunToTrxString(GASSTATION_LOW_BALANCE_SUN),
              needsTopUp
            },
            operator: {
              address: operatorAddress,
              balanceSun: operatorBalanceSun,
              balanceTrx: sunToTrxString(operatorBalanceSun),
              minBalanceForTopUpSun: OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN,
              minBalanceForTopUpTrx: sunToTrxString(OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN),
              reserveAfterTopUpSun: OPERATOR_REMAINING_RESERVE_SUN,
              reserveAfterTopUpTrx: sunToTrxString(OPERATOR_REMAINING_RESERVE_SUN),
              availableForTopUpSun,
              availableForTopUpTrx: sunToTrxString(availableForTopUpSun),
              canTopUp,
              recommendedTopUpSun,
              recommendedTopUpTrx: sunToTrxString(recommendedTopUpSun)
            }
          }
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

      if (method === "GET" && pathname === "/ambassador/by-wallet") {
        const wallet = normalizeIncomingWallet(requestUrl.searchParams.get("wallet"));
        const record = await getAmbassadorRegistryRecordByWallet(wallet);

        if (!record) {
          sendJson(req, res, env, 404, {
            ok: true,
            registered: false,
            result: null
          });
          return;
        }

        sendJson(req, res, env, 200, {
          ok: true,
          registered: true,
          result: {
            slug: record.publicProfile.slug,
            slugHash: record.publicProfile.slugHash,
            status: record.publicProfile.status,
            wallet: record.privateIdentity.wallet,
            referralLink: buildReferralLink(record.publicProfile.slug)
          }
        });
        return;
      }

      if (method === "POST" && pathname === "/ambassador/register-complete") {
        const body = await readJsonBody(req);

        const slug = normalizeIncomingSlug(body.slug);
        const slugHash = normalizeSlugHash(body.slugHash);
        const wallet = normalizeIncomingWallet(body.wallet);

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

      if (method === "GET" && pathname === "/cabinet/profile") {
        const walletParam = normalizeOptionalString(requestUrl.searchParams.get("wallet"));
        const slugParam = normalizeOptionalString(requestUrl.searchParams.get("slug"));

        if (walletParam) {
          const wallet = normalizeIncomingWallet(walletParam);
          const profile = await cabinetService.getProfileByWallet(wallet);

          sendJson(req, res, env, 200, {
            ok: true,
            registered: profile.registered,
            result: profile.registered ? profile : null,
            wallet: profile.wallet
          });
          return;
        }

        if (slugParam) {
          const slug = normalizeIncomingSlug(slugParam);
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

        sendJson(req, res, env, 400, {
          ok: false,
          error: "wallet or slug is required"
        });
        return;
      }

      if (method === "POST" && pathname === "/cabinet/replay-pending") {
        const body = await readJsonBody(req);
        const wallet = normalizeIncomingWallet(body.wallet);

        const result = await cabinetService.replayPendingByWallet(wallet, Date.now());

        sendJson(req, res, env, 200, {
          ok: true,
          result
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
        const allocationMode =
          body.allocationMode === "claim" || body.allocationMode === "eager"
            ? body.allocationMode
            : undefined;

        const feeLimitSun =
          body.feeLimitSun !== undefined
            ? parsePositiveInteger(String(body.feeLimitSun), 1, "feeLimitSun")
            : undefined;

        const result = await worker.processor.processFrontendAttribution({
          txHash,
          buyerWallet,
          slug,
          now: Date.now(),
          allocationMode,
          feeLimitSun
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
            ? parsePositiveInteger(String(body.feeLimitSun), 1, "feeLimitSun")
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

      sendJson(req, res, env, 404, {
        ok: false,
        error: "Not found"
      });
    } catch (error) {
      sendJson(req, res, env, 500, {
        ok: false,
        error: toErrorMessage(error)
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
        controllerContractAddress: resolvedControllerContractAddress,
        gasStation: {
          enabled: env.gasStationEnabled,
          apiBaseUrl: env.gasStationApiBaseUrl || null,
          minBandwidth: env.gasStationMinBandwidth,
          minEnergy: env.gasStationMinEnergy,
          serviceChargeType: env.gasStationServiceChargeType
        },
        allocationThresholds: {
          minBandwidth: env.allocationMinBandwidth,
          minEnergy: env.allocationMinEnergy
        }
      })
    );
  });
}

void bootstrap();
