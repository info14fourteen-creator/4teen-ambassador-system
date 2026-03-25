# REPOSITORY: 4teen-ambassador-system
# SECTION: REMAINING FILES
# GENERATED_AT: 2026-03-25T17:12:48.372Z

## INCLUDED FILES

- ai/MASTER_PLAN.md
- services/allocation-worker/Procfile
- services/allocation-worker/src/app/processAttribution.ts
- services/allocation-worker/src/run-scan.ts
- services/allocation-worker/src/services/gasStation.ts

## REPOSITORY LINK BASE

- https://raw.githubusercontent.com/info14fourteen-creator/4teen-ambassador-system/main/ai/latest/4teen-ambassador-system

---

## FILE: ai/MASTER_PLAN.md

```md
# 4TEEN AMBASSADOR SYSTEM — MASTER PLAN
# VERSION: CLAIM-FIRST LEDGER STRATEGY
# STATUS: ACTIVE
# OWNER: STAN
# MODE: STRICT PHASE EXECUTION

---

## CORE IDEA

System must support TWO valid allocation modes:

1. **EAGER MODE**
   - if worker has enough Energy and Bandwidth now,
   - verified purchase may be written on-chain immediately.

2. **CLAIM-FIRST MODE (PRIMARY SAFETY MODE)**
   - if resources are insufficient,
   - purchase must remain safely recorded off-chain,
   - and be written on-chain later,
   - mainly when ambassador requests withdrawal.

This means:

- purchase attribution is accepted immediately
- verification is accepted immediately
- accounting is preserved immediately
- on-chain reward finalization is NOT mandatory immediately
- on-chain write may be postponed safely

---

## SYSTEM PRINCIPLE

Blockchain is the final reward settlement layer.

Off-chain database is the operational queue and accounting layer.

So:

- frontend captures attribution
- worker verifies purchase
- DB stores verified purchase safely
- contract write happens:
  - immediately if resources are available
  - or later during withdrawal preparation
  - or later during maintenance replay

---

## NON-NEGOTIABLE RULES

1. One purchase = one unique purchaseId
2. One purchase must NEVER be allocated twice
3. Off-chain verified purchase must NEVER be lost
4. Failed on-chain allocation must NEVER erase the verified record
5. Withdrawal preparation must process only ambassador-owned pending purchases
6. Owner balance must NEVER mix with reserved ambassador rewards
7. No silent state transitions
8. Any deferred purchase must remain replayable
9. Contract write must always be idempotent from system perspective
10. Claim flow must tolerate partial progress and resume safely

---

## TARGET BUSINESS FLOW

### FLOW A — PURCHASE

1. user opens site with `?r=slug`
2. referral is captured by first-touch logic
3. user buys token
4. frontend sends attribution:
   - txHash
   - buyerWallet
   - slug
5. worker verifies:
   - tx exists
   - event is BuyTokens
   - values match
6. worker stores purchase in DB as verified
7. system decides:
   - if enough Energy + Bandwidth -> try immediate on-chain allocation
   - else keep purchase deferred

### FLOW B — WITHDRAW REQUEST

1. ambassador opens cabinet
2. ambassador clicks withdraw request
3. backend loads ambassador pending verified purchases
4. backend builds queue ordered by creation time
5. backend attempts on-chain allocation one by one:
   - oldest first
   - only for this ambassador
6. for every successful contract write:
   - purchase becomes allocated
7. if resources end:
   - stop safely
   - keep remaining purchases pending
8. after queue pass:
   - read real contract reward state
   - if withdrawable amount > 0 -> allow reward withdrawal
9. execute reward withdrawal
10. persist result

### FLOW C — DAILY / MANUAL MAINTENANCE

1. check current Energy and Bandwidth
2. optionally rent/buy resources
3. replay deferred / failed purchases
4. stop safely when resource budget is exhausted
5. preserve full audit log

---

## ARCHITECTURE DECISION

### SOURCE OF TRUTH BY LAYER

**Referral capture**
- frontend storage + backend attribution record

**Verified purchase existence**
- off-chain DB

**Allocated reward / withdrawable reward**
- blockchain contract

**Operational queue**
- off-chain DB

This split is intentional.

---

## PURCHASE LIFECYCLE

Each purchase must move through explicit statuses.

### REQUIRED STATUSES

- `received`
- `verified`
- `deferred`
- `allocation_in_progress`
- `allocated`
- `allocation_failed_retryable`
- `allocation_failed_final`
- `withdraw_included`
- `withdraw_completed`

### STATUS RULES

#### `received`
Attribution was submitted but not yet fully verified.

#### `verified`
Purchase is valid and safe to allocate.
No on-chain write guaranteed yet.

#### `deferred`
Purchase is verified but intentionally postponed due to:
- insufficient Energy
- insufficient Bandwidth
- policy decision
- claim-first batching logic

#### `allocation_in_progress`
Temporary operational lock during active contract write attempt.

#### `allocated`
`recordVerifiedPurchase(...)` already succeeded on-chain.

#### `allocation_failed_retryable`
Write attempt failed, but may be retried later.

#### `allocation_failed_final`
Used only for truly unrecoverable cases:
- invalid purchase data
- mismatched buyer
- invalid ambassador mapping
- contract-level permanent reject

#### `withdraw_included`
Purchase has been included in a specific ambassador withdrawal preparation run.

#### `withdraw_completed`
Final withdrawal flow related to prepared rewards completed.

---

## MAIN STRATEGY CHANGE

### OLD MODEL
- worker tries to allocate every verified purchase as soon as possible

### NEW MODEL
- worker MAY still allocate immediately
- but system is optimized for:
  - safe deferred queue
  - claim-time on-chain finalization
  - resource-aware replay

### WHY
Because on-chain write cost is per purchase.
`recordVerifiedPurchase(...)` is not batch settlement.
Therefore blind eager allocation is not optimal when resources are constrained.

---

## CLAIM-FIRST WITHDRAWAL STRATEGY

When ambassador requests withdrawal:

1. lock ambassador withdrawal session
2. select all purchases where:
   - ambassadorWallet = current ambassador
   - status in (`verified`, `deferred`, `allocation_failed_retryable`)
3. sort by oldest first
4. for each purchase:
   - check resources
   - if enough -> call `recordVerifiedPurchase(...)`
   - if success -> mark `allocated`
   - if insufficient resources -> stop queue safely
   - if retryable error -> mark retryable and stop or continue based on policy
5. read contract reward state
6. if reward available:
   - proceed to withdraw
7. if no reward available:
   - return clear status to UI

### IMPORTANT
Withdrawal request does NOT mean:
- "write everything no matter what"

It means:
- "process as much verified queue as safely possible for this ambassador"

---

## RESOURCE POLICY

System must check BOTH:

- Energy
- Bandwidth

before each on-chain write.

### PRE-CHECK RULE

Do not send allocation transaction if:
- available Energy < configured minimum
- available Bandwidth < configured minimum

### POSTPONE RULE

If resources are insufficient:
- do not burn blindly
- do not send transaction
- mark purchase as deferred
- keep purchase replayable

### OPTIONAL RENT MODE

System may support a separate resource provider module:
- buy/rent Energy
- optionally top up before maintenance
- optionally top up before withdrawal preparation

But business logic must NOT depend on provider availability.

Resource provider is helper infrastructure, not core truth.

---

## PHASES

---

# PHASE 1 — REFERRAL CORE

## FILES
- apps/site-integration/src/referral/capture.ts
- apps/site-integration/src/referral/firstTouch.ts
- apps/site-integration/src/referral/storage.ts
- apps/site-integration/src/purchase/afterBuy.ts
- apps/site-integration/src/purchase/submitAttribution.ts
- shared/utils/slug.ts

## IMPLEMENTATION
- parse `?r=slug`
- validate slug
- store first-touch referral
- preserve TTL logic
- send attribution after buy

## RESULT
- stable referral capture
- no overwrite abuse
- attribution survives reload/session flow

---

# PHASE 2 — PURCHASE LEDGER / DATA MODEL

## FILES
- services/allocation-worker/src/db/purchases.ts
- services/allocation-worker/src/db/ambassadors.ts
- services/allocation-worker/src/types.ts

## IMPLEMENTATION
- extend purchase schema for new lifecycle statuses
- add fields:
  - allocationMode
  - allocationAttempts
  - lastAllocationAttemptAt
  - lastAllocationErrorCode
  - lastAllocationErrorMessage
  - deferredReason
  - withdrawSessionId
- add indexed queries for:
  - pending by ambassador
  - replayable failures
  - verified/deferred queue
- ensure idempotent updates

## RESULT
- DB becomes real operational ledger
- pending queue is safe and queryable

---

# PHASE 3 — RESOURCE GATE

## FILES
- services/allocation-worker/src/tron/resources.ts
- services/allocation-worker/src/config.ts
- services/allocation-worker/src/index.ts

## IMPLEMENTATION
- unified resource read:
  - Energy
  - Bandwidth
- configurable thresholds
- single decision helper:
  - `canSendAllocationNow()`
- classify failures:
  - insufficient_energy
  - insufficient_bandwidth
  - retryable_network
  - permanent_validation

## RESULT
- no blind allocation attempts
- deterministic defer decision

---

# PHASE 4 — ALLOCATION ENGINE REWRITE

## FILES
- services/allocation-worker/src/index.ts
- services/allocation-worker/src/run-scan.ts
- services/allocation-worker/src/tron/controller.ts
- services/allocation-worker/src/server.ts

## IMPLEMENTATION
- rewrite allocation flow around explicit modes:
  - eager
  - deferred
  - claim-first
- immediate scan path:
  - verify purchase
  - if resources enough -> allocate
  - else -> defer
- replay path:
  - only retry replayable purchases
- make `recordVerifiedPurchase(...)` strictly single-purchase unit
- add per-purchase lock/idempotency

## RESULT
- worker no longer assumes immediate allocation is mandatory
- worker becomes queue-aware

---

# PHASE 5 — WITHDRAWAL PREPARATION ENGINE

## FILES
- services/allocation-worker/src/jobs/prepareAmbassadorWithdrawal.ts
- services/allocation-worker/src/jobs/processAmbassadorPendingQueue.ts
- services/allocation-worker/src/server.ts
- services/allocation-worker/src/db/purchases.ts
- services/allocation-worker/src/tron/controller.ts

## IMPLEMENTATION
- new withdraw-preparation job
- load ambassador queue
- process purchases one by one
- stop on resource exhaustion
- create withdraw session log
- expose API endpoint for cabinet:
  - prepare withdrawal
  - get session status
- after preparation:
  - read on-chain reward
  - decide whether withdraw can continue

## RESULT
- ambassador withdrawal becomes the main settlement trigger
- queue is monetized only when needed

---

# PHASE 6 — CABINET WITHDRAW UX

## FILES
- apps/cabinet/src/app/ambassador/page.tsx
- apps/cabinet/src/hooks/useAmbassadorDashboard.ts
- apps/cabinet/src/lib/api/withdraw.ts

## IMPLEMENTATION
- withdraw button becomes two-step operationally:
  1. prepare pending purchases
  2. withdraw rewards
- show statuses:
  - pending purchases count
  - prepared count
  - deferred count
  - on-chain ready amount
- show clear messages:
  - "processing pending purchases"
  - "resource shortage, try again later"
  - "rewards ready for withdrawal"

## RESULT
- ambassador understands why reward may not be instantly withdrawable
- UX matches real accounting model

---

# PHASE 7 — DAILY MAINTENANCE

## FILES
- services/allocation-worker/src/jobs/dailyMaintenance.ts
- services/allocation-worker/src/jobs/replayDeferredPurchases.ts
- .github/workflows/allocation-worker-daily.yml

## IMPLEMENTATION
- daily maintenance sequence:
  1. resource check
  2. optional rent call
  3. replay deferred queue
  4. scan new events
- stop when resource budget is exhausted
- never spam chain blindly
- maintain logs and counters

## RESULT
- backlog shrinks automatically
- system still works even before ambassador presses withdraw

---

# PHASE 8 — OPTIONAL RESOURCE RENT MODULE

## FILES
- services/allocation-worker/src/providers/resources/gasstation.ts
- services/allocation-worker/src/jobs/rentResources.ts
- services/allocation-worker/src/config.ts

## IMPLEMENTATION
- isolated provider client
- no direct coupling with core accounting
- support:
  - check balance/reserve
  - request resource top-up
- provider failure must never break core queue logic

## RESULT
- resource rental becomes optional optimization layer

---

# PHASE 9 — TELEGRAM / NOTIFICATIONS

## FILES
- services/telegram-bot/src/server.ts
- services/telegram-bot/src/commands/start.ts
- services/telegram-bot/src/services/auth.ts
- services/telegram-bot/src/services/notifications.ts

## IMPLEMENTATION
- notify ambassador:
  - purchase verified
  - purchase deferred
  - purchase allocated
  - withdrawal ready
- never promise on-chain allocation before it really happened

## RESULT
- real-time feedback without accounting lies

---

# PHASE 10 — AUDIT / OBSERVABILITY

## FILES
- services/allocation-worker/src/logging/
- services/allocation-worker/src/server.ts
- services/allocation-worker/src/db/purchases.ts

## IMPLEMENTATION
- structured logs for every purchase state transition
- withdraw session logs
- counters:
  - verified
  - deferred
  - allocated
  - retryable_failed
  - final_failed
- admin endpoints for diagnostics

## RESULT
- we can always explain where any purchase currently is

---

## REQUIRED ENDPOINTS

### EXISTING / KEEP
- `GET /health`
- `POST /scan`
- `GET /failures`
- `POST /replay-failed`

### NEW / REQUIRED
- `POST /withdraw/prepare`
- `GET /withdraw/session`
- `GET /purchases/pending-by-ambassador`
- `POST /maintenance/replay-deferred`
- `GET /resources`
- `POST /resources/rent` (optional provider-backed)

---

## FAILURE POLICY

### RETRYABLE
- insufficient Energy
- insufficient Bandwidth
- temporary RPC/provider issue
- timeout before confirmed permanent reject

### FINAL
- invalid txHash
- purchase does not match BuyTokens event
- buyer mismatch
- ambassador mapping invalid
- purchase already permanently invalidated

Retryable failures must stay in replay queue.

---

## ORDERING POLICY

For ambassador withdrawal preparation:
- oldest purchase first

Reason:
- deterministic accounting
- simpler debugging
- fair queue progression

---

## SECURITY POLICY

1. only cabinet-authenticated ambassador may request own withdrawal preparation
2. ambassador may never trigger processing for another ambassador queue
3. purchase must always remain bound to original ambassador once verified
4. replay must remain idempotent
5. server must reject duplicate active withdraw sessions for same ambassador

---

## FINAL ACCEPTANCE CRITERIA

System is complete when:

- referral capture is stable
- verified purchases are always persisted
- no verified purchase is lost due to resource shortage
- immediate allocation works when resources exist
- deferred allocation works when resources are missing
- ambassador withdrawal preparation processes pending purchases one by one
- reward withdrawal reflects real on-chain state
- replay queue works
- no double allocation is possible
- logs explain every purchase state

---

## EXECUTION RULE

Work strictly phase-by-phase.

DO NOT:
- mix unrelated layers
- do partial hidden rewrites
- change contract assumptions silently
- remove safety statuses
- build UI before backend state machine is stable

Each phase must be:
- implemented
- tested
- validated
- frozen

before moving to the next one.

---

# END
```

---

## FILE: services/allocation-worker/Procfile

```text
web: npm run start
```

---

## FILE: services/allocation-worker/src/app/processAttribution.ts

```ts
import {
  AttributionDecision,
  AttributionService,
  FrontendAttributionInput,
  PrepareVerifiedPurchaseResult,
  VerifiedPurchaseInput
} from "../domain/attribution";
import {
  AllocationDecision,
  AllocationService
} from "../domain/allocation";

export interface ProcessAttributionConfig {
  attributionService: AttributionService;
  allocationService: AllocationService;
}

export interface ProcessFrontendAttributionResult {
  stage: "frontend-attribution";
  attribution: AttributionDecision;
}

export interface ProcessVerifiedPurchaseAndAllocateInput {
  txHash: string;
  buyerWallet: string;
  slug: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  feeLimitSun?: number;
  now?: number;
}

export interface ProcessVerifiedPurchaseAndAllocateResult {
  stage: "verified-purchase";
  purchaseId: string;
  attribution: AttributionDecision | null;
  verification: PrepareVerifiedPurchaseResult;
  allocation: AllocationDecision | null;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

export class AttributionProcessor {
  private readonly attributionService: AttributionService;
  private readonly allocationService: AllocationService;

  constructor(config: ProcessAttributionConfig) {
    if (!config?.attributionService) {
      throw new Error("attributionService is required");
    }

    if (!config?.allocationService) {
      throw new Error("allocationService is required");
    }

    this.attributionService = config.attributionService;
    this.allocationService = config.allocationService;
  }

  async processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<ProcessFrontendAttributionResult> {
    const attribution = await this.attributionService.captureFrontendAttribution(input);

    return {
      stage: "frontend-attribution",
      attribution
    };
  }

  async processVerifiedPurchaseAndAllocate(
    input: ProcessVerifiedPurchaseAndAllocateInput
  ): Promise<ProcessVerifiedPurchaseAndAllocateResult> {
    const txHash = assertNonEmpty(input.txHash, "txHash");
    const buyerWallet = assertNonEmpty(input.buyerWallet, "buyerWallet");
    const slug = assertNonEmpty(input.slug, "slug");
    const purchaseAmountSun = assertNonEmpty(input.purchaseAmountSun, "purchaseAmountSun");
    const ownerShareSun = assertNonEmpty(input.ownerShareSun, "ownerShareSun");
    const now = input.now ?? Date.now();

    let attribution: AttributionDecision | null = null;

    try {
      attribution = await this.attributionService.captureFrontendAttribution({
        txHash,
        buyerWallet,
        slug,
        now
      });
    } catch (error) {
      const message =
        error && typeof error === "object" && "message" in error
          ? String((error as { message?: unknown }).message || "").trim()
          : "";

      throw new Error(message || "Failed to capture frontend attribution");
    }

    const purchaseId = attribution.purchase.purchaseId;

    const verification = await this.attributionService.prepareVerifiedPurchase({
      purchaseId,
      txHash,
      buyerWallet,
      slug,
      purchaseAmountSun,
      ownerShareSun,
      now
    });

    if (!verification.canAllocate) {
      return {
        stage: "verified-purchase",
        purchaseId,
        attribution,
        verification,
        allocation: null
      };
    }

    const allocation = await this.allocationService.executeAllocation({
      purchaseId,
      feeLimitSun: input.feeLimitSun,
      now
    });

    return {
      stage: "verified-purchase",
      purchaseId,
      attribution,
      verification,
      allocation
    };
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<AllocationDecision> {
    return this.allocationService.replayFailedAllocation(
      assertNonEmpty(purchaseId, "purchaseId"),
      feeLimitSun,
      now
    );
  }
}
```

---

## FILE: services/allocation-worker/src/run-scan.ts

```ts
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
    const rawEvents = await this.tronWeb.getEventResult(this.tokenContractAddress, {
      eventName: this.eventName,
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
```

---

## FILE: services/allocation-worker/src/services/gasStation.ts

```ts
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

function normalizeBaseUrl(value?: string): string {
  return String(value || "https://openapi.gasstation.ai").replace(/\/+$/, "");
}

function pkcs7Pad(buffer: Buffer): Buffer {
  const blockSize = 16;
  const padLength = blockSize - (buffer.length % blockSize || blockSize);
  const padding = Buffer.alloc(padLength, padLength);
  return Buffer.concat([buffer, padding]);
}

function toBase64UrlSafe(buffer: Buffer): string {
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function fromBase64UrlSafe(value: string): Buffer {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(normalized, "base64");
}

function encryptAesEcbPkcs7Base64UrlSafe(plainText: string, secretKey: string): string {
  const key = Buffer.from(assertNonEmpty(secretKey, "secretKey"), "utf8");
  const plainBuffer = Buffer.from(plainText, "utf8");
  const padded = pkcs7Pad(plainBuffer);

  const cipher = crypto.createCipheriv(`aes-${key.length * 8}-ecb`, key, null);
  cipher.setAutoPadding(false);

  const encrypted = Buffer.concat([cipher.update(padded), cipher.final()]);
  return toBase64UrlSafe(encrypted);
}

async function requestJson<T>(
  url: string,
  init?: RequestInit
): Promise<T> {
  const response = await fetch(url, init);

  const text = await response.text();
  let parsed: any = null;

  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    throw new Error(`GasStation returned non-JSON response: ${text || "empty response"}`);
  }

  if (!response.ok) {
    throw new Error(
      parsed?.msg
        ? `GasStation HTTP ${response.status}: ${parsed.msg}`
        : `GasStation HTTP ${response.status}`
    );
  }

  if (!parsed || typeof parsed !== "object") {
    throw new Error("GasStation returned invalid response");
  }

  if (parsed.code !== 0) {
    throw new Error(
      parsed.msg
        ? `GasStation error ${parsed.code}: ${parsed.msg}`
        : `GasStation error ${parsed.code}`
    );
  }

  return parsed.data as T;
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
    payload: Record<string, unknown>
  ): string {
    const plainText = JSON.stringify(payload);
    const encrypted = encryptAesEcbPkcs7Base64UrlSafe(plainText, this.secretKey);

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
    return requestJson<GasStationBalanceResult>(url, {
      method: "GET"
    });
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
    return requestJson<GasStationEstimateResult>(url, {
      method: "GET"
    });
  }

  async createEnergyOrder(input: {
    requestId: string;
    receiveAddress: string;
    energyNum: number;
    serviceChargeType?: string;
  }): Promise<GasStationCreateOrderResult> {
    const energyNum = Number(input.energyNum);

    if (!Number.isFinite(energyNum) || energyNum < 64400) {
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
      energy_num: Math.ceil(energyNum)
    };

    const url = this.buildEncryptedUrl("/api/tron/gas/create_order", payload);
    return requestJson<GasStationCreateOrderResult>(url, {
      method: "POST"
    });
  }
}

export function createGasStationClientFromEnv(): GasStationClient {
  return new GasStationClient({
    appId: assertNonEmpty(process.env.GASSTATION_APP_ID, "GASSTATION_APP_ID"),
    secretKey: assertNonEmpty(process.env.GASSTATION_SECRET_KEY, "GASSTATION_SECRET_KEY"),
    baseUrl: process.env.GASSTATION_BASE_URL
  });
}
```
