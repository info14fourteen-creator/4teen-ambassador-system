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
  | "allocation-failed"
  | "event-parse-failed"
  | "event-processing-failed";

export interface ScanProcessResult {
  status: ScanProcessStatus;
  event: BuyTokensEvent | null;
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

function normalizeTxHashFromEvent(event: any): string {
  const value =
    pickObjectValue(event, ["transaction_id", "transactionId", "txHash", "txid"]) ?? "";

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

function toTronBase58Address(rawAddress: string, tronWeb: any): string {
  if (!tronWeb?.address?.fromHex) {
    throw new Error("tronWeb.address.fromHex is required to normalize buyer wallet");
  }

  const raw = assertNonEmpty(rawAddress, "buyerWallet").trim();

  if (/^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(raw)) {
    return raw;
  }

  if (/^41[0-9a-fA-F]{40}$/.test(raw)) {
    return tronWeb.address.fromHex(raw);
  }

  if (/^0x[0-9a-fA-F]{40}$/.test(raw)) {
    const hexBody = raw.slice(2);
    return tronWeb.address.fromHex(`41${hexBody}`);
  }

  if (/^[0-9a-fA-F]{40}$/.test(raw)) {
    return tronWeb.address.fromHex(`41${raw}`);
  }

  return raw;
}

function normalizeBuyerWalletFromEvent(event: any, tronWeb: any): string {
  const result = pickObjectValue(event, ["result"]);
  const buyer = pickObjectValue(result, ["buyer"]) ?? pickObjectValue(event, ["buyer"]);

  const rawBuyer = assertNonEmpty(String(buyer), "event.result.buyer");
  return toTronBase58Address(rawBuyer, tronWeb);
}

function normalizePurchaseAmountSunFromEvent(event: any): string {
  const result = pickObjectValue(event, ["result"]);
  const amountTRX =
    pickObjectValue(result, ["amountTRX"]) ?? pickObjectValue(event, ["amountTRX"]);

  return normalizeSunAmount(amountTRX, "event.result.amountTRX");
}

function normalizeAmountTokensFromEvent(event: any): string {
  const result = pickObjectValue(event, ["result"]);
  const amountTokens =
    pickObjectValue(result, ["amountTokens"]) ??
    pickObjectValue(event, ["amountTokens"]) ??
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

function parseBuyTokensEvent(event: any, tronWeb: any): BuyTokensEvent {
  const txHash = normalizeTxHashFromEvent(event);
  const buyerWallet = normalizeBuyerWalletFromEvent(event, tronWeb);
  const purchaseAmountSun = normalizePurchaseAmountSunFromEvent(event);
  const amountTokens = normalizeAmountTokensFromEvent(event);
  const ownerShareSun = computeOwnerShareSun(purchaseAmountSun);
  const blockNumber = normalizeBlockNumberFromEvent(event);
  const blockTimestamp = normalizeBlockTimestampFromEvent(event);
  const fingerprint = normalizeFingerprintFromEvent(event);

  return {
    txHash,
    buyerWallet,
    purchaseAmountSun,
    amountTokens,
    ownerShareSun,
    blockNumber,
    blockTimestamp,
    fingerprint,
    raw: event
  };
}

function extractEventArray(payload: any): any[] {
  if (Array.isArray(payload)) {
    return payload;
  }

  if (payload && typeof payload === "object" && Array.isArray(payload.data)) {
    return payload.data;
  }

  return [];
}

function extractNextFingerprint(payload: any): string | null {
  const metaFingerprint = pickObjectValue(payload, ["fingerprint"]);
  if (typeof metaFingerprint === "string" && metaFingerprint.trim()) {
    return metaFingerprint.trim();
  }

  const meta = pickObjectValue(payload, ["meta"]);
  const links = meta && typeof meta === "object" ? pickObjectValue(meta, ["links"]) : null;
  const nextLink = links && typeof links === "object" ? pickObjectValue(links, ["next"]) : null;

  if (typeof nextLink === "string" && nextLink.includes("fingerprint=")) {
    const match = nextLink.match(/[?&]fingerprint=([^&]+)/);
    if (match?.[1]) {
      return decodeURIComponent(match[1]);
    }
  }

  return null;
}

function getRetryAfterTimestamp(failureReason: string | null): number | null {
  const raw = String(failureReason || "").trim();
  if (!raw) {
    return null;
  }

  const match = raw.match(/\[RETRY_AFTER=(\d+)\]/);
  if (!match?.[1]) {
    return null;
  }

  const parsed = Number(match[1]);
  return Number.isFinite(parsed) ? parsed : null;
}

export class BuyTokensScanner {
  private readonly tronWeb: any;
  private readonly processor: AttributionProcessor;
  private readonly store: PurchaseStore;
  private readonly tokenContractAddress: string;
  private readonly eventName: string;
  private readonly pageSize: number;

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
    this.pageSize = normalizePositiveInteger(config.pageSize, 20);
  }

  async fetchEvents(cursor: ScanCursor = {}): Promise<RunScanResult> {
    const tokenContract = await this.tronWeb.contract().at(this.tokenContractAddress);

    const rawEvents = await tokenContract.getEventResult(this.eventName, {
      size: this.pageSize,
      fingerprint: cursor.fingerprint ?? undefined
    });

    console.log(
      JSON.stringify({
        stage: "scan:getEventResult",
        tokenContractAddress: this.tokenContractAddress,
        eventName: this.eventName,
        pageSize: this.pageSize,
        fingerprint: cursor.fingerprint ?? null,
        rawEventsType: Array.isArray(rawEvents) ? "array" : typeof rawEvents,
        rawEventsLength: Array.isArray(rawEvents)
          ? rawEvents.length
          : Array.isArray(rawEvents?.data)
            ? rawEvents.data.length
            : null,
        rawEventsPreview: rawEvents
      })
    );

    const rawEventList = extractEventArray(rawEvents);
    const parsedEvents: BuyTokensEvent[] = [];
    const processed: ScanProcessResult[] = [];

    for (const rawEvent of rawEventList) {
      try {
        const event = parseBuyTokensEvent(rawEvent, this.tronWeb);
        parsedEvents.push(event);

        try {
          const result = await this.processEvent(event);
          processed.push(result);
        } catch (error) {
          const message =
            error && typeof error === "object" && "message" in error
              ? String((error as { message?: unknown }).message || "").trim()
              : "";

          processed.push({
            status: "event-processing-failed",
            event,
            purchaseId: null,
            reason: message || "Failed to process parsed event",
            rawResult: error
          });
        }
      } catch (error) {
        const message =
          error && typeof error === "object" && "message" in error
            ? String((error as { message?: unknown }).message || "").trim()
            : "";

        processed.push({
          status: "event-parse-failed",
          event: null,
          purchaseId: null,
          reason: message || "Failed to parse BuyTokens event",
          rawResult: rawEvent
        });
      }
    }

    const nextFingerprint = extractNextFingerprint(rawEvents);

    return {
      events: parsedEvents,
      processed,
      nextCursor: {
        fingerprint: nextFingerprint
      }
    };
  }

  async processEvent(event: BuyTokensEvent): Promise<ScanProcessResult> {
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

    if (localPurchase.status === "failed") {
      const retryAfter = getRetryAfterTimestamp(localPurchase.failureReason);
      const now = event.blockTimestamp ?? Date.now();

      if (retryAfter && now < retryAfter) {
        return {
          status: "skipped-already-final",
          event,
          purchaseId: localPurchase.purchaseId,
          reason: `Retry deferred until ${new Date(retryAfter).toISOString()}`
        };
      }
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
