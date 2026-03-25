import { FOURTEEN_TOKEN_CONTRACT } from "../../../shared/config/contracts";
import { AttributionProcessor } from "./app/processAttribution";
import { PurchaseStore } from "./db/purchases";

export interface RunScanConfig {
  tronWeb: any;
  processor: AttributionProcessor;
  store: PurchaseStore;
  tokenContractAddress?: string;
  eventName?: string;
  pageSize?: number;
}

export interface ScanCursor {
  fingerprint?: string | null;
}

export interface BuyTokensEvent {
  txHash: string;
  buyerWallet: string;
  purchaseAmountSun: string;
  amountTokens: string;
  ownerShareSun: string;
  blockNumber: number | null;
  blockTimestamp: number | null;
  fingerprint: string | null;
  raw: unknown;
}

export type ScanProcessStatus =
  | "allocated"
  | "skipped-no-local-attribution"
  | "skipped-missing-slug"
  | "skipped-already-final"
  | "verification-blocked"
  | "allocation-failed";

export interface ScanProcessResult {
  status: ScanProcessStatus;
  event: BuyTokensEvent;
  purchaseId: string | null;
  reason: string | null;
  rawResult?: unknown;
}

export interface RunScanResult {
  events: BuyTokensEvent[];
  processed: ScanProcessResult[];
  nextCursor: ScanCursor;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  const resolved = value ?? fallback;

  if (!Number.isInteger(resolved) || resolved <= 0) {
    throw new Error("pageSize must be a positive integer");
  }

  return resolved;
}

function normalizeSunAmount(value: unknown, fieldName: string): string {
  const normalized = String(value ?? "").trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${fieldName} must be a non-negative integer string`);
  }

  return normalized;
}

function computeOwnerShareSun(purchaseAmountSun: string): string {
  return String((BigInt(purchaseAmountSun) * 7n) / 100n);
}

function pickObjectValue(source: any, keys: string[]): unknown {
  if (!source || typeof source !== "object") {
    return undefined;
  }

  for (const key of keys) {
    if (key in source) {
      return source[key];
    }
  }

  return undefined;
}

function isHexAddressLike(value: string): boolean {
  const normalized = value.trim().toLowerCase();
  return /^41[0-9a-f]{40}$/.test(normalized) || /^0x41[0-9a-f]{40}$/.test(normalized);
}

function normalizeHexAddress(value: string): string {
  const normalized = value.trim();
  return normalized.startsWith("0x") ? normalized.slice(2) : normalized;
}

function toBase58IfNeeded(tronWeb: any, value: unknown): string {
  const raw = assertNonEmpty(String(value ?? ""), "address");

  if (!isHexAddressLike(raw)) {
    return raw;
  }

  try {
    const hex = normalizeHexAddress(raw);
    if (tronWeb?.address?.fromHex) {
      return tronWeb.address.fromHex(hex);
    }
  } catch (_) {}

  return raw;
}

function normalizeTxHashFromEvent(event: any): string {
  const value =
    pickObjectValue(event, ["transaction_id", "transactionId", "txHash", "txid"]) ??
    "";

  return assertNonEmpty(String(value), "event.txHash");
}

function normalizeFingerprintFromEvent(event: any): string | null {
  const value = pickObjectValue(event, ["fingerprint"]);

  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized || null;
}

function normalizeBuyerWalletFromEvent(event: any, tronWeb: any): string {
  const result = pickObjectValue(event, ["result"]);
  const buyer =
    pickObjectValue(result, ["buyer", "_buyer", "user"]) ??
    pickObjectValue(event, ["buyer", "_buyer", "user"]);

  return toBase58IfNeeded(tronWeb, buyer);
}

function normalizePurchaseAmountSunFromEvent(event: any): string {
  const result = pickObjectValue(event, ["result"]);
  const amountTRX =
    pickObjectValue(result, ["amountTRX", "amountTrx", "trxAmount", "_amountTRX"]) ??
    pickObjectValue(event, ["amountTRX", "amountTrx", "trxAmount", "_amountTRX"]);

  return normalizeSunAmount(amountTRX, "event.result.amountTRX");
}

function normalizeAmountTokensFromEvent(event: any): string {
  const result = pickObjectValue(event, ["result"]);
  const amountTokens =
    pickObjectValue(result, ["amountTokens", "_amountTokens", "tokenAmount"]) ??
    pickObjectValue(event, ["amountTokens", "_amountTokens", "tokenAmount"]) ??
    "0";

  return normalizeSunAmount(amountTokens, "event.result.amountTokens");
}

function normalizeBlockNumberFromEvent(event: any): number | null {
  const value = pickObjectValue(event, ["block_number", "blockNumber"]);

  if (value == null || value === "") {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeBlockTimestampFromEvent(event: any): number | null {
  const value = pickObjectValue(event, ["block_timestamp", "blockTimestamp"]);

  if (value == null || value === "") {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function mapBuyTokensEvent(event: any, tronWeb: any): BuyTokensEvent {
  const txHash = normalizeTxHashFromEvent(event);
  const buyerWallet = normalizeBuyerWalletFromEvent(event, tronWeb);
  const purchaseAmountSun = normalizePurchaseAmountSunFromEvent(event);
  const amountTokens = normalizeAmountTokensFromEvent(event);

  return {
    txHash,
    buyerWallet,
    purchaseAmountSun,
    amountTokens,
    ownerShareSun: computeOwnerShareSun(purchaseAmountSun),
    blockNumber: normalizeBlockNumberFromEvent(event),
    blockTimestamp: normalizeBlockTimestampFromEvent(event),
    fingerprint: normalizeFingerprintFromEvent(event),
    raw: event
  };
}

function getTronGridBaseUrl(tronWeb: any): string {
  const candidates = [
    tronWeb?.fullNode?.host,
    tronWeb?.eventServer?.host,
    tronWeb?.solidityNode?.host
  ].filter(Boolean);

  for (const value of candidates) {
    const host = String(value).trim().replace(/\/+$/, "");
    if (!host) continue;

    if (host.includes("trongrid")) {
      return host;
    }
  }

  return "https://api.trongrid.io";
}

function buildHttpEventsUrl(
  baseUrl: string,
  contractAddress: string,
  eventName: string,
  pageSize: number,
  fingerprint?: string | null
): string {
  const url = new URL(`${baseUrl.replace(/\/+$/, "")}/v1/contracts/${contractAddress}/events`);
  url.searchParams.set("event_name", eventName);
  url.searchParams.set("only_confirmed", "true");
  url.searchParams.set("order_by", "block_timestamp,desc");
  url.searchParams.set("limit", String(pageSize));

  if (fingerprint) {
    url.searchParams.set("fingerprint", fingerprint);
  }

  return url.toString();
}

export class BuyTokensScanner {
  private readonly tronWeb: any;
  private readonly processor: AttributionProcessor;
  private readonly store: PurchaseStore;
  private readonly tokenContractAddress: string;
  private readonly eventName: string;
  private readonly pageSize: number;
  private readonly tronGridBaseUrl: string;

  constructor(config: RunScanConfig) {
    if (!config?.tronWeb) {
      throw new Error("tronWeb is required");
    }

    if (!config?.processor) {
      throw new Error("processor is required");
    }

    if (!config?.store) {
      throw new Error("store is required");
    }

    this.tronWeb = config.tronWeb;
    this.processor = config.processor;
    this.store = config.store;
    this.tokenContractAddress = assertNonEmpty(
      config.tokenContractAddress ?? FOURTEEN_TOKEN_CONTRACT,
      "tokenContractAddress"
    );
    this.eventName = assertNonEmpty(config.eventName ?? "BuyTokens", "eventName");
    this.pageSize = normalizePositiveInteger(config.pageSize, 50);
    this.tronGridBaseUrl = getTronGridBaseUrl(config.tronWeb);
  }

  private async fetchEventsViaTronWeb(cursor: ScanCursor): Promise<any[]> {
    if (typeof this.tronWeb.getEventResult !== "function") {
      return [];
    }

    try {
      const rawEvents = await this.tronWeb.getEventResult(this.tokenContractAddress, {
        eventName: this.eventName,
        limit: this.pageSize,
        onlyConfirmed: true,
        fingerprint: cursor.fingerprint ?? undefined
      });

      return Array.isArray(rawEvents) ? rawEvents : [];
    } catch (error) {
      console.error("[scan] tronWeb.getEventResult failed:", error);
      return [];
    }
  }

  private async fetchEventsViaHttp(cursor: ScanCursor): Promise<any[]> {
  const url = buildHttpEventsUrl(
    this.tronGridBaseUrl,
    this.tokenContractAddress,
    this.eventName,
    this.pageSize,
    cursor.fingerprint
  );

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json"
      }
    });

    if (!response.ok) {
      throw new Error(`HTTP events request failed with status ${response.status}`);
    }

    const payload: unknown = await response.json();

    if (
      payload &&
      typeof payload === "object" &&
      "data" in payload &&
      Array.isArray((payload as { data: unknown }).data)
    ) {
      return (payload as { data: any[] }).data;
    }

    return [];
  } catch (error) {
    console.error("[scan] HTTP fallback failed:", error);
    return [];
  }
}

  async fetchEvents(cursor: ScanCursor = {}): Promise<RunScanResult> {
    let rawEvents = await this.fetchEventsViaTronWeb(cursor);

    if (!rawEvents.length) {
      rawEvents = await this.fetchEventsViaHttp(cursor);
    }

    const events = Array.isArray(rawEvents)
      ? rawEvents.map((event) => mapBuyTokensEvent(event, this.tronWeb))
      : [];

    const nextFingerprint =
      events.length > 0 ? events[events.length - 1]?.fingerprint ?? null : null;

    const processed: ScanProcessResult[] = [];

    for (const event of events) {
      processed.push(await this.processEvent(event));
    }

    return {
      events,
      processed,
      nextCursor: {
        fingerprint: nextFingerprint
      }
    };
  }

  private async processEvent(event: BuyTokensEvent): Promise<ScanProcessResult> {
    const localPurchase = await this.store.getByTxHash(event.txHash);

    if (!localPurchase) {
      return {
        status: "skipped-no-local-attribution",
        event,
        purchaseId: null,
        reason: "No local attribution record found for txHash"
      };
    }

    if (!localPurchase.ambassadorSlug) {
      return {
        status: "skipped-missing-slug",
        event,
        purchaseId: localPurchase.purchaseId,
        reason: "Local purchase record has no ambassador slug"
      };
    }

    if (localPurchase.status === "allocated" || localPurchase.status === "ignored") {
      return {
        status: "skipped-already-final",
        event,
        purchaseId: localPurchase.purchaseId,
        reason: `Purchase already finalized with status: ${localPurchase.status}`
      };
    }

    const result = await this.processor.processVerifiedPurchaseAndAllocate({
      txHash: event.txHash,
      buyerWallet: event.buyerWallet,
      slug: localPurchase.ambassadorSlug,
      purchaseAmountSun: event.purchaseAmountSun,
      ownerShareSun: event.ownerShareSun,
      now: event.blockTimestamp ?? Date.now()
    });

    if (!result.verification.canAllocate) {
      return {
        status: "verification-blocked",
        event,
        purchaseId: result.purchaseId,
        reason: result.verification.reason,
        rawResult: result
      };
    }

    if (!result.allocation || result.allocation.status !== "allocated") {
      return {
        status: "allocation-failed",
        event,
        purchaseId: result.purchaseId,
        reason: result.allocation?.reason ?? "Allocation did not complete",
        rawResult: result
      };
    }

    return {
      status: "allocated",
      event,
      purchaseId: result.purchaseId,
      reason: null,
      rawResult: result
    };
  }
}
