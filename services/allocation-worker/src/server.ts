import http from "node:http";
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

interface EnvConfig {
  port: number;
  tronFullHost: string;
  tronPrivateKey: string;
  controllerContractAddress?: string;
  tokenContractAddress?: string;
  scanPageSize: number;
}

type TronWebConstructor = new (config: {
  fullHost: string;
  privateKey: string;
}) => any;

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

function loadEnv(): EnvConfig {
  const config: EnvConfig = {
    port: parsePositiveInteger(process.env.PORT, 3000, "PORT"),
    tronFullHost: assertNonEmpty(process.env.TRON_FULL_HOST, "TRON_FULL_HOST"),
    tronPrivateKey: assertNonEmpty(process.env.TRON_PRIVATE_KEY, "TRON_PRIVATE_KEY"),
    scanPageSize: parsePositiveInteger(process.env.SCAN_PAGE_SIZE, 50, "SCAN_PAGE_SIZE")
  };

  const controllerContractAddress = process.env.FOURTEEN_CONTROLLER_CONTRACT?.trim();
  const tokenContractAddress = process.env.FOURTEEN_TOKEN_CONTRACT?.trim();

  if (controllerContractAddress) {
    config.controllerContractAddress = controllerContractAddress;
  }

  if (tokenContractAddress) {
    config.tokenContractAddress = tokenContractAddress;
  }

  return config;
}

function sendJson(
  res: http.ServerResponse,
  statusCode: number,
  payload: unknown
): void {
  const body = JSON.stringify(payload, null, 2);

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

async function bootstrap() {
  const env = loadEnv();
  const TronWeb = getTronWebConstructor();

  await initAmbassadorRegistryTables();

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

      if (method === "GET" && pathname === "/health") {
        sendJson(res, 200, {
          ok: true,
          service: "allocation-worker",
          timestamp: Date.now()
        });
        return;
      }

      if (method === "GET" && pathname === "/slug/check") {
        const slug = normalizeIncomingSlug(requestUrl.searchParams.get("slug"));
        const taken = await isSlugTaken(slug);

        sendJson(res, 200, {
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

        sendJson(res, 200, {
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
          sendJson(res, 404, {
            ok: false,
            error: "Ambassador profile not found"
          });
          return;
        }

        sendJson(res, 200, {
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

        sendJson(res, 200, {
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

        sendJson(res, 200, {
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

        sendJson(res, 200, {
          ok: true,
          result
        });
        return;
      }

      if (method === "GET" && pathname === "/failures") {
        const failures = await worker.store.listReplayableFailures();

        sendJson(res, 200, {
          ok: true,
          count: failures.length,
          failures
        });
        return;
      }

      sendJson(res, 404, {
        ok: false,
        error: "Not found"
      });
    } catch (error) {
      sendJson(res, 500, {
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
        port: env.port
      })
    );
  });
}

void bootstrap();
