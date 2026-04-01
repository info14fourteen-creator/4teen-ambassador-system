# 4teen-ambassador-system — ALLOCATION WORKER

Generated: 2026-04-01T07:19:10.339Z
Repository: info14fourteen-creator/4teen-ambassador-system
Branch: main

## Included files

- 4teen-ambassador-system :: services/allocation-worker/package.json
- 4teen-ambassador-system :: services/allocation-worker/src/app/processAttribution.ts
- 4teen-ambassador-system :: services/allocation-worker/src/db/ambassadors.ts
- 4teen-ambassador-system :: services/allocation-worker/src/db/postgres.ts
- 4teen-ambassador-system :: services/allocation-worker/src/db/purchases.ts
- 4teen-ambassador-system :: services/allocation-worker/src/domain/allocation.ts
- 4teen-ambassador-system :: services/allocation-worker/src/domain/attribution.ts
- 4teen-ambassador-system :: services/allocation-worker/src/index.ts
- 4teen-ambassador-system :: services/allocation-worker/src/jobs/allocatePurchase.ts
- 4teen-ambassador-system :: services/allocation-worker/src/jobs/dailyMaintenance.ts
- 4teen-ambassador-system :: services/allocation-worker/src/jobs/finalizeAmbassadorWithdrawal.ts
- 4teen-ambassador-system :: services/allocation-worker/src/jobs/prepareAmbassadorWithdrawal.ts
- 4teen-ambassador-system :: services/allocation-worker/src/jobs/processAmbassadorPendingQueue.ts
- 4teen-ambassador-system :: services/allocation-worker/src/jobs/rentEnergy.ts
- 4teen-ambassador-system :: services/allocation-worker/src/jobs/replayDeferredPurchases.ts
- 4teen-ambassador-system :: services/allocation-worker/src/run-scan.ts
- 4teen-ambassador-system :: services/allocation-worker/src/server.ts
- 4teen-ambassador-system :: services/allocation-worker/src/services/cabinet.ts
- 4teen-ambassador-system :: services/allocation-worker/src/services/gasStation.ts
- 4teen-ambassador-system :: services/allocation-worker/src/tron/controller.ts
- 4teen-ambassador-system :: services/allocation-worker/src/tron/hashing.ts
- 4teen-ambassador-system :: services/allocation-worker/src/tron/resources.ts
- 4teen-ambassador-system :: services/allocation-worker/tsconfig.json

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/package.json

```json
{
  "name": "allocation-worker",
  "version": "1.0.0",
  "private": true,
  "main": "dist/services/allocation-worker/src/server.js",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "start": "node dist/services/allocation-worker/src/server.js",
    "dev": "tsx src/server.ts",
    "check": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "@noble/hashes": "^1.8.0",
    "pg": "^8.13.1",
    "tronweb": "^6.0.4",
    "undici": "^7.13.0"
  },
  "devDependencies": {
    "@types/node": "^24.3.0",
    "@types/pg": "^8.11.10",
    "tsx": "^4.20.5",
    "typescript": "^5.9.2"
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/app/processAttribution.ts

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
    const replayResult = await this.allocationService.replayFailedAllocation(
      assertNonEmpty(purchaseId, "purchaseId"),
      feeLimitSun,
      now
    );

    return {
      status:
        replayResult.status === "allocated"
          ? "allocated"
          : replayResult.status === "skipped"
            ? "skipped-already-final"
            : "retryable-failed",
      purchase: replayResult.purchase,
      txid: replayResult.txid,
      reason: replayResult.reason,
      errorCode: replayResult.errorCode,
      errorMessage: replayResult.errorMessage
    };
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/db/ambassadors.ts

```ts
import { getClient, query } from "./postgres";

export type AmbassadorRegistryStatus =
  | "pending"
  | "active"
  | "disabled";

export interface AmbassadorPublicProfile {
  id: string;
  slug: string;
  slugHash: string;
  status: AmbassadorRegistryStatus;
  createdAt: number;
  updatedAt: number;
}

export interface AmbassadorPrivateIdentity {
  ambassadorId: string;
  wallet: string;
  createdAt: number;
  updatedAt: number;
}

export interface AmbassadorRegistryRecord {
  publicProfile: AmbassadorPublicProfile;
  privateIdentity: AmbassadorPrivateIdentity;
}

export interface CreateAmbassadorRegistryRecordInput {
  slug: string;
  slugHash: string;
  wallet: string;
  status?: AmbassadorRegistryStatus;
  now?: number;
}

export interface CompleteAmbassadorRegistrationInput {
  slug: string;
  slugHash: string;
  wallet: string;
  now?: number;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeSlug(value: string): string {
  return assertNonEmpty(value, "slug").toLowerCase();
}

function normalizeSlugHash(value: string): string {
  const normalized = assertNonEmpty(value, "slugHash").toLowerCase();

  if (!/^0x[0-9a-f]{64}$/.test(normalized)) {
    throw new Error("slugHash must be a bytes32 hex string");
  }

  return normalized;
}

function normalizeWallet(value: string): string {
  return assertNonEmpty(value, "wallet");
}

function normalizeStatus(value?: AmbassadorRegistryStatus): AmbassadorRegistryStatus {
  return value ?? "pending";
}

function rowToPublicProfile(row: any): AmbassadorPublicProfile {
  return {
    id: String(row.id),
    slug: String(row.slug),
    slugHash: String(row.slug_hash),
    status: String(row.status) as AmbassadorRegistryStatus,
    createdAt: Number(row.created_at_ms),
    updatedAt: Number(row.updated_at_ms)
  };
}

function rowToPrivateIdentity(row: any): AmbassadorPrivateIdentity {
  return {
    ambassadorId: String(row.ambassador_id),
    wallet: String(row.wallet),
    createdAt: Number(row.created_at_ms),
    updatedAt: Number(row.updated_at_ms)
  };
}

function isUniqueViolation(error: unknown): boolean {
  return (
    !!error &&
    typeof error === "object" &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505"
  );
}

function getConstraintName(error: unknown): string {
  if (
    error &&
    typeof error === "object" &&
    "constraint" in error &&
    typeof (error as { constraint?: unknown }).constraint === "string"
  ) {
    return String((error as { constraint: string }).constraint);
  }

  return "";
}

function mapRegistryWriteError(error: unknown): Error {
  if (!isUniqueViolation(error)) {
    return error instanceof Error ? error : new Error("Registry write failed");
  }

  const constraint = getConstraintName(error);

  if (constraint.includes("slug")) {
    return new Error("Slug is already taken");
  }

  if (constraint.includes("wallet")) {
    return new Error("Wallet is already registered");
  }

  return new Error("Ambassador registration conflict");
}

export async function initAmbassadorRegistryTables(): Promise<void> {
  await query(`
    CREATE TABLE IF NOT EXISTS ambassador_public_profiles (
      id BIGSERIAL PRIMARY KEY,
      slug TEXT NOT NULL UNIQUE,
      slug_hash TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS ambassador_private_identities (
      ambassador_id BIGINT NOT NULL UNIQUE REFERENCES ambassador_public_profiles(id) ON DELETE CASCADE,
      wallet TEXT NOT NULL UNIQUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_ambassador_public_profiles_slug
    ON ambassador_public_profiles(slug)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_ambassador_public_profiles_slug_hash
    ON ambassador_public_profiles(slug_hash)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_ambassador_private_identities_wallet
    ON ambassador_private_identities(wallet)
  `);
}

export async function isSlugTaken(slug: string): Promise<boolean> {
  const normalizedSlug = normalizeSlug(slug);

  const result = await query<{ exists: boolean }>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM ambassador_public_profiles
        WHERE slug = $1
      ) AS exists
    `,
    [normalizedSlug]
  );

  return Boolean(result.rows[0]?.exists);
}

export async function getAmbassadorPublicProfileBySlug(
  slug: string
): Promise<AmbassadorPublicProfile | null> {
  const normalizedSlug = normalizeSlug(slug);

  const result = await query(
    `
      SELECT
        id,
        slug,
        slug_hash,
        status,
        FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
        FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms
      FROM ambassador_public_profiles
      WHERE slug = $1
      LIMIT 1
    `,
    [normalizedSlug]
  );

  const row = result.rows[0];
  return row ? rowToPublicProfile(row) : null;
}

export async function getAmbassadorPublicProfileBySlugHash(
  slugHash: string
): Promise<AmbassadorPublicProfile | null> {
  const normalizedSlugHash = normalizeSlugHash(slugHash);

  const result = await query(
    `
      SELECT
        id,
        slug,
        slug_hash,
        status,
        FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
        FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms
      FROM ambassador_public_profiles
      WHERE slug_hash = $1
      LIMIT 1
    `,
    [normalizedSlugHash]
  );

  const row = result.rows[0];
  return row ? rowToPublicProfile(row) : null;
}

export async function getAmbassadorRegistryRecordByWallet(
  wallet: string
): Promise<AmbassadorRegistryRecord | null> {
  const normalizedWallet = normalizeWallet(wallet);

  const result = await query(
    `
      SELECT
        p.id,
        p.slug,
        p.slug_hash,
        p.status,
        FLOOR(EXTRACT(EPOCH FROM p.created_at) * 1000) AS public_created_at_ms,
        FLOOR(EXTRACT(EPOCH FROM p.updated_at) * 1000) AS public_updated_at_ms,
        i.ambassador_id,
        i.wallet,
        FLOOR(EXTRACT(EPOCH FROM i.created_at) * 1000) AS private_created_at_ms,
        FLOOR(EXTRACT(EPOCH FROM i.updated_at) * 1000) AS private_updated_at_ms
      FROM ambassador_public_profiles p
      INNER JOIN ambassador_private_identities i
        ON i.ambassador_id = p.id
      WHERE i.wallet = $1
      LIMIT 1
    `,
    [normalizedWallet]
  );

  const row = result.rows[0];
  if (!row) {
    return null;
  }

  return {
    publicProfile: {
      id: String(row.id),
      slug: String(row.slug),
      slugHash: String(row.slug_hash),
      status: String(row.status) as AmbassadorRegistryStatus,
      createdAt: Number(row.public_created_at_ms),
      updatedAt: Number(row.public_updated_at_ms)
    },
    privateIdentity: {
      ambassadorId: String(row.ambassador_id),
      wallet: String(row.wallet),
      createdAt: Number(row.private_created_at_ms),
      updatedAt: Number(row.private_updated_at_ms)
    }
  };
}

export async function createAmbassadorRegistryRecord(
  input: CreateAmbassadorRegistryRecordInput
): Promise<AmbassadorRegistryRecord> {
  const normalizedSlug = normalizeSlug(input.slug);
  const normalizedSlugHash = normalizeSlugHash(input.slugHash);
  const normalizedWallet = normalizeWallet(input.wallet);
  const normalizedStatus = normalizeStatus(input.status);

  const client = await getClient();

  try {
    await client.query("BEGIN");

    const publicInsert = await client.query(
      `
        INSERT INTO ambassador_public_profiles (
          slug,
          slug_hash,
          status
        )
        VALUES ($1, $2, $3)
        RETURNING
          id,
          slug,
          slug_hash,
          status,
          FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
          FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms
      `,
      [normalizedSlug, normalizedSlugHash, normalizedStatus]
    );

    const publicRow = publicInsert.rows[0];

    const privateInsert = await client.query(
      `
        INSERT INTO ambassador_private_identities (
          ambassador_id,
          wallet
        )
        VALUES ($1, $2)
        RETURNING
          ambassador_id,
          wallet,
          FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
          FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms
      `,
      [publicRow.id, normalizedWallet]
    );

    const privateRow = privateInsert.rows[0];

    await client.query("COMMIT");

    return {
      publicProfile: rowToPublicProfile(publicRow),
      privateIdentity: rowToPrivateIdentity(privateRow)
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw mapRegistryWriteError(error);
  } finally {
    client.release();
  }
}

export async function completeAmbassadorRegistration(
  input: CompleteAmbassadorRegistrationInput
): Promise<AmbassadorRegistryRecord> {
  return createAmbassadorRegistryRecord({
    slug: input.slug,
    slugHash: input.slugHash,
    wallet: input.wallet,
    status: "active",
    now: input.now
  });
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/db/postgres.ts

```ts
import { Pool, PoolClient, QueryResult, QueryResultRow } from "pg";

let pool: Pool | null = null;

function assertNonEmpty(value: string | undefined, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

export function getDatabaseUrl(): string {
  return assertNonEmpty(process.env.DATABASE_URL, "DATABASE_URL");
}

export function getPool(): Pool {
  if (pool) {
    return pool;
  }

  pool = new Pool({
    connectionString: getDatabaseUrl(),
    ssl: {
      rejectUnauthorized: false
    }
  });

  return pool;
}

export async function query<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params: unknown[] = []
): Promise<QueryResult<T>> {
  return getPool().query<T>(text, params);
}

export async function getClient(): Promise<PoolClient> {
  return getPool().connect();
}

export async function closePool(): Promise<void> {
  if (!pool) {
    return;
  }

  await pool.end();
  pool = null;
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/db/purchases.ts

```ts
import { query } from "./postgres";

export type PurchaseProcessingStatus =
  | "received"
  | "verified"
  | "deferred"
  | "allocation_in_progress"
  | "allocated"
  | "allocation_failed_retryable"
  | "allocation_failed_final"
  | "withdraw_included"
  | "withdraw_completed"
  | "ignored";

export type PurchaseSource =
  | "frontend-attribution"
  | "event-scan"
  | "manual-replay"
  | "withdraw-prepare";

export type AllocationMode =
  | "eager"
  | "deferred"
  | "claim-first"
  | "maintenance-replay"
  | "manual-replay"
  | null;

export interface PurchaseRecord {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  purchaseAmountSun: string;
  ownerShareSun: string;
  status: PurchaseProcessingStatus;
  failureReason: string | null;
  source: PurchaseSource;
  allocationMode: AllocationMode;
  allocationAttempts: number;
  lastAllocationAttemptAt: number | null;
  lastAllocationErrorCode: string | null;
  lastAllocationErrorMessage: string | null;
  deferredReason: string | null;
  withdrawSessionId: string | null;
  createdAt: number;
  updatedAt: number;
  allocatedAt: number | null;
}

export interface AmbassadorStoreRecord {
  slug: string;
  slugHash: string;
  status: "pending" | "active" | "disabled";
  wallet: string | null;
  ambassadorId: string | null;
}

export interface CabinetStatsRecord {
  totalBuyers: number;
  trackedVolumeSun: string;

  /**
   * These fields are intentionally always zero in DB-derived stats.
   * Real withdrawable/claimable rewards must come from blockchain reads,
   * not from purchase-table aggregation.
   */
  claimableRewardsSun: string;
  availableOnChainSun: string;
  availableOnChainCount: number;

  /**
   * Backend operational accounting only.
   * These values are safe to show as non-blockchain backend queue/accounting.
   */
  allocatedInDbSun: string;
  allocatedInDbCount: number;
  pendingBackendSyncSun: string;
  pendingBackendSyncCount: number;
  requestedForProcessingSun: string;
  requestedForProcessingCount: number;

  /**
   * Historical accounting only.
   * This is DB-derived lifecycle accounting, not contract truth.
   */
  lifetimeRewardsSun: string;
  withdrawnRewardsSun: string;

  hasProcessingWithdrawal: boolean;
}

export interface CreateOrGetReceivedPurchaseInput {
  txHash: string;
  buyerWallet: string;
  ambassadorSlug?: string | null;
  now?: number;
}

export interface CreateOrGetReceivedPurchaseResult {
  created: boolean;
  purchase: PurchaseRecord;
}

export interface AttachAmbassadorToPurchaseInput {
  purchaseId: string;
  ambassadorSlug: string;
  ambassadorWallet: string;
  purchaseAmountSun?: string;
  ownerShareSun?: string;
  now?: number;
}

export interface MarkVerifiedPurchaseInput {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  now?: number;
}

export interface CreatePurchaseRecordInput {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug?: string | null;
  ambassadorWallet?: string | null;
  purchaseAmountSun?: string;
  ownerShareSun?: string;
  source?: PurchaseSource;
  status?: PurchaseProcessingStatus;
  failureReason?: string | null;
  allocationMode?: AllocationMode;
  allocationAttempts?: number;
  lastAllocationAttemptAt?: number | null;
  lastAllocationErrorCode?: string | null;
  lastAllocationErrorMessage?: string | null;
  deferredReason?: string | null;
  withdrawSessionId?: string | null;
  allocatedAt?: number | null;
  now?: number;
}

export interface UpdatePurchaseRecordInput {
  purchaseAmountSun?: string;
  ownerShareSun?: string;
  ambassadorSlug?: string | null;
  ambassadorWallet?: string | null;
  status?: PurchaseProcessingStatus;
  failureReason?: string | null;
  allocationMode?: AllocationMode;
  allocationAttempts?: number;
  incrementAllocationAttempts?: boolean;
  lastAllocationAttemptAt?: number | null;
  lastAllocationErrorCode?: string | null;
  lastAllocationErrorMessage?: string | null;
  deferredReason?: string | null;
  withdrawSessionId?: string | null;
  allocatedAt?: number | null;
  now?: number;
}

export interface PendingPurchaseQuery {
  ambassadorWallet: string;
  statuses?: PurchaseProcessingStatus[];
  limit?: number;
}

export interface PurchaseStore {
  getByPurchaseId(purchaseId: string): Promise<PurchaseRecord | null>;
  getByTxHash(txHash: string): Promise<PurchaseRecord | null>;
  create(input: CreatePurchaseRecordInput): Promise<PurchaseRecord>;
  update(purchaseId: string, input: UpdatePurchaseRecordInput): Promise<PurchaseRecord>;

  getAmbassadorBySlug(slug: string): Promise<AmbassadorStoreRecord | null>;

  createOrGetReceivedPurchase(
    input: CreateOrGetReceivedPurchaseInput
  ): Promise<CreateOrGetReceivedPurchaseResult>;

  attachAmbassadorToPurchase(
    input: AttachAmbassadorToPurchaseInput
  ): Promise<PurchaseRecord>;

  markVerifiedPurchase(
    input: MarkVerifiedPurchaseInput
  ): Promise<PurchaseRecord>;

  markVerified(
    purchaseId: string,
    input: {
      purchaseAmountSun: string;
      ownerShareSun: string;
      ambassadorSlug?: string | null;
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markDeferred(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markAllocationInProgress(
    purchaseId: string,
    input?: {
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markAllocationRetryableFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markAllocationFinalFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markWithdrawIncluded(
    purchaseId: string,
    input: {
      withdrawSessionId: string;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markWithdrawCompleted(
    purchaseId: string,
    input?: {
      withdrawSessionId?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord>;

  markFailed(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord>;

  markIgnored(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord>;

  assignWithdrawSession(
    purchaseId: string,
    withdrawSessionId: string,
    now?: number
  ): Promise<PurchaseRecord>;

  clearWithdrawSession(
    purchaseId: string,
    now?: number
  ): Promise<PurchaseRecord>;

  listReplayableFailures(limit?: number): Promise<PurchaseRecord[]>;

  listPendingByAmbassador(
    input: PendingPurchaseQuery
  ): Promise<PurchaseRecord[]>;

  getCabinetStatsByAmbassadorWallet(
    ambassadorWallet: string
  ): Promise<CabinetStatsRecord>;

  hasProcessedPurchase(purchaseId: string): Promise<boolean>;
}

const DEFAULT_PENDING_STATUSES: PurchaseProcessingStatus[] = [
  "verified",
  "deferred",
  "allocation_failed_retryable"
];

const RATE_LIMIT_ERROR_CODES = new Set([
  "429",
  "ERR_BAD_REQUEST",
  "TRON_RATE_LIMIT",
  "GASSTATION_RATE_LIMIT"
]);

const RATE_LIMIT_MESSAGE_PARTS = [
  "status code 429",
  "http 429",
  "too many requests",
  "rate limit",
  "rate limited"
];

const RESOURCE_ERROR_CODES = new Set([
  "ACCOUNT_RESOURCE_INSUFFICIENT",
  "ACCOUNT_RESOURCE_INSUFFICIENT_AFTER_RENTAL",
  "ACCOUNT_RESOURCE_CONSUMED_BEFORE_SEND",
  "ACCOUNT_RESOURCE_INSUFFICIENT_DURING_SEND",
  "GASSTATION_OPERATOR_BALANCE_LOW",
  "GASSTATION_TOPUP_NOT_SETTLED",
  "GASSTATION_TOPUP_TRANSFER_FAILED",
  "GASSTATION_TOPUP_FAILED",
  "GASSTATION_SERVICE_BALANCE_LOW_AFTER_TOPUP",
  "GASSTATION_ORDER_FAILED"
]);

const RESOURCE_MESSAGE_PARTS = [
  "out of energy",
  "resource insufficient",
  "bandwidth",
  "energy",
  "gasstation",
  "top-up",
  "top up"
];

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeOptionalString(value: string | null | undefined): string | null {
  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized || null;
}

function normalizeWallet(value: string | null | undefined): string | null {
  return normalizeOptionalString(value);
}

function normalizeSunAmount(value: string | number | bigint | undefined): string {
  if (value == null) {
    return "0";
  }

  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error("SUN amount must be a non-negative integer string");
  }

  return normalized;
}

function normalizeStatus(value?: PurchaseProcessingStatus): PurchaseProcessingStatus {
  return value ?? "received";
}

function normalizeSource(value?: PurchaseSource): PurchaseSource {
  return value ?? "frontend-attribution";
}

function normalizeAllocationMode(value?: AllocationMode | string | null): AllocationMode {
  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();

  if (!normalized) {
    return null;
  }

  if (
    normalized === "eager" ||
    normalized === "deferred" ||
    normalized === "claim-first" ||
    normalized === "maintenance-replay" ||
    normalized === "manual-replay"
  ) {
    return normalized;
  }

  return null;
}

function normalizeCount(value: number | undefined, fieldName: string): number {
  if (value == null) {
    return 0;
  }

  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${fieldName} must be a non-negative integer`);
  }

  return value;
}

function normalizeTimestamp(value: number | null | undefined, fieldName: string): number | null {
  if (value == null) {
    return null;
  }

  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${fieldName} must be a non-negative timestamp`);
  }

  return Math.floor(value);
}

function normalizePendingStatuses(
  statuses?: PurchaseProcessingStatus[]
): PurchaseProcessingStatus[] {
  const value = statuses?.length ? statuses : DEFAULT_PENDING_STATUSES;
  return [...new Set(value)];
}

function normalizeTxHash(value: string): string {
  return assertNonEmpty(value, "txHash").toLowerCase();
}

function buildPurchaseIdFromTxHash(txHash: string): string {
  const normalized = normalizeTxHash(txHash);

  if (/^0x[0-9a-f]{64}$/.test(normalized)) {
    return normalized;
  }

  if (/^[0-9a-f]{64}$/.test(normalized)) {
    return `0x${normalized}`;
  }

  return normalized;
}

function sumSunStrings(left: string, right: string): string {
  return (BigInt(left || "0") + BigInt(right || "0")).toString();
}

function emptyCabinetStatsRecord(): CabinetStatsRecord {
  return {
    totalBuyers: 0,
    trackedVolumeSun: "0",
    claimableRewardsSun: "0",
    lifetimeRewardsSun: "0",
    withdrawnRewardsSun: "0",
    availableOnChainSun: "0",
    availableOnChainCount: 0,
    allocatedInDbSun: "0",
    allocatedInDbCount: 0,
    pendingBackendSyncSun: "0",
    requestedForProcessingSun: "0",
    pendingBackendSyncCount: 0,
    requestedForProcessingCount: 0,
    hasProcessingWithdrawal: false
  };
}

export function isRateLimitedAllocationFailure(record: PurchaseRecord): boolean {
  const code = String(record.lastAllocationErrorCode || "").trim().toUpperCase();
  const message = String(
    record.lastAllocationErrorMessage || record.failureReason || ""
  ).toLowerCase();

  if (RATE_LIMIT_ERROR_CODES.has(code)) {
    return (
      RATE_LIMIT_MESSAGE_PARTS.some((part) => message.includes(part)) ||
      code !== "ERR_BAD_REQUEST"
    );
  }

  return RATE_LIMIT_MESSAGE_PARTS.some((part) => message.includes(part));
}

export function isResourceLimitedAllocationFailure(record: PurchaseRecord): boolean {
  const code = String(record.lastAllocationErrorCode || "").trim().toUpperCase();
  const message = String(
    record.lastAllocationErrorMessage ||
      record.deferredReason ||
      record.failureReason ||
      ""
  ).toLowerCase();

  if (RESOURCE_ERROR_CODES.has(code)) {
    return true;
  }

  return RESOURCE_MESSAGE_PARTS.some((part) => message.includes(part));
}

export function computeAllocationRetryDelayMs(record: PurchaseRecord): number {
  if (
    record.status !== "allocation_failed_retryable" &&
    record.status !== "deferred"
  ) {
    return 0;
  }

  const attempts = Math.max(1, Number(record.allocationAttempts || 0));

  if (isRateLimitedAllocationFailure(record)) {
    if (attempts <= 1) return 30_000;
    if (attempts === 2) return 60_000;
    if (attempts === 3) return 180_000;
    if (attempts === 4) return 600_000;
    return 1_800_000;
  }

  if (isResourceLimitedAllocationFailure(record)) {
    if (attempts <= 1) return 60_000;
    if (attempts === 2) return 180_000;
    if (attempts === 3) return 300_000;
    if (attempts === 4) return 600_000;
    return 1_800_000;
  }

  if (record.status === "deferred") {
    if (attempts <= 1) return 15_000;
    if (attempts === 2) return 30_000;
    if (attempts === 3) return 60_000;
    if (attempts === 4) return 180_000;
    return 300_000;
  }

  if (attempts <= 1) return 10_000;
  if (attempts === 2) return 30_000;
  if (attempts === 3) return 60_000;
  if (attempts === 4) return 180_000;
  return 600_000;
}

export function getAllocationRetryReadyAt(record: PurchaseRecord): number {
  const base =
    Number(record.lastAllocationAttemptAt || 0) ||
    Number(record.updatedAt || 0) ||
    Number(record.createdAt || 0);

  return base + computeAllocationRetryDelayMs(record);
}

export function isPurchaseReadyForAllocationRetry(
  record: PurchaseRecord,
  now: number = Date.now()
): boolean {
  if (record.status === "verified") {
    return true;
  }

  if (
    record.status !== "deferred" &&
    record.status !== "allocation_failed_retryable"
  ) {
    return false;
  }

  return now >= getAllocationRetryReadyAt(record);
}

function createRecord(input: CreatePurchaseRecordInput): PurchaseRecord {
  const now = input.now ?? Date.now();
  const status = normalizeStatus(input.status);
  const allocatedAt =
    input.allocatedAt !== undefined
      ? normalizeTimestamp(input.allocatedAt, "allocatedAt")
      : status === "allocated" ||
          status === "withdraw_included" ||
          status === "withdraw_completed"
        ? now
        : null;

  return {
    purchaseId: assertNonEmpty(input.purchaseId, "purchaseId"),
    txHash: normalizeTxHash(input.txHash),
    buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
    ambassadorSlug: normalizeOptionalString(input.ambassadorSlug),
    ambassadorWallet: normalizeWallet(input.ambassadorWallet),
    purchaseAmountSun: normalizeSunAmount(input.purchaseAmountSun),
    ownerShareSun: normalizeSunAmount(input.ownerShareSun),
    status,
    failureReason: normalizeOptionalString(input.failureReason),
    source: normalizeSource(input.source),
    allocationMode: normalizeAllocationMode(input.allocationMode),
    allocationAttempts: normalizeCount(input.allocationAttempts, "allocationAttempts"),
    lastAllocationAttemptAt: normalizeTimestamp(
      input.lastAllocationAttemptAt,
      "lastAllocationAttemptAt"
    ),
    lastAllocationErrorCode: normalizeOptionalString(input.lastAllocationErrorCode),
    lastAllocationErrorMessage: normalizeOptionalString(input.lastAllocationErrorMessage),
    deferredReason: normalizeOptionalString(input.deferredReason),
    withdrawSessionId: normalizeOptionalString(input.withdrawSessionId),
    createdAt: now,
    updatedAt: now,
    allocatedAt
  };
}

function mergeRecord(
  current: PurchaseRecord,
  input: UpdatePurchaseRecordInput
): PurchaseRecord {
  const now = input.now ?? Date.now();
  const nextStatus = input.status ?? current.status;

  const nextAllocationAttempts =
    input.allocationAttempts !== undefined
      ? normalizeCount(input.allocationAttempts, "allocationAttempts")
      : input.incrementAllocationAttempts
        ? current.allocationAttempts + 1
        : current.allocationAttempts;

  const nextAllocatedAt =
    input.allocatedAt !== undefined
      ? normalizeTimestamp(input.allocatedAt, "allocatedAt")
      : nextStatus === "allocated" ||
          nextStatus === "withdraw_included" ||
          nextStatus === "withdraw_completed"
        ? current.allocatedAt ?? now
        : current.allocatedAt;

  return {
    ...current,
    purchaseAmountSun:
      input.purchaseAmountSun !== undefined
        ? normalizeSunAmount(input.purchaseAmountSun)
        : current.purchaseAmountSun,
    ownerShareSun:
      input.ownerShareSun !== undefined
        ? normalizeSunAmount(input.ownerShareSun)
        : current.ownerShareSun,
    ambassadorSlug:
      input.ambassadorSlug !== undefined
        ? normalizeOptionalString(input.ambassadorSlug)
        : current.ambassadorSlug,
    ambassadorWallet:
      input.ambassadorWallet !== undefined
        ? normalizeWallet(input.ambassadorWallet)
        : current.ambassadorWallet,
    status: nextStatus,
    failureReason:
      input.failureReason !== undefined
        ? normalizeOptionalString(input.failureReason)
        : current.failureReason,
    allocationMode:
      input.allocationMode !== undefined
        ? normalizeAllocationMode(input.allocationMode)
        : current.allocationMode,
    allocationAttempts: nextAllocationAttempts,
    lastAllocationAttemptAt:
      input.lastAllocationAttemptAt !== undefined
        ? normalizeTimestamp(input.lastAllocationAttemptAt, "lastAllocationAttemptAt")
        : current.lastAllocationAttemptAt,
    lastAllocationErrorCode:
      input.lastAllocationErrorCode !== undefined
        ? normalizeOptionalString(input.lastAllocationErrorCode)
        : current.lastAllocationErrorCode,
    lastAllocationErrorMessage:
      input.lastAllocationErrorMessage !== undefined
        ? normalizeOptionalString(input.lastAllocationErrorMessage)
        : current.lastAllocationErrorMessage,
    deferredReason:
      input.deferredReason !== undefined
        ? normalizeOptionalString(input.deferredReason)
        : current.deferredReason,
    withdrawSessionId:
      input.withdrawSessionId !== undefined
        ? normalizeOptionalString(input.withdrawSessionId)
        : current.withdrawSessionId,
    allocatedAt: nextAllocatedAt,
    updatedAt: now
  };
}

function rowToPurchaseRecord(row: any): PurchaseRecord {
  return {
    purchaseId: String(row.purchase_id),
    txHash: String(row.tx_hash),
    buyerWallet: String(row.buyer_wallet),
    ambassadorSlug: normalizeOptionalString(row.ambassador_slug),
    ambassadorWallet: normalizeWallet(row.ambassador_wallet),
    purchaseAmountSun: String(row.purchase_amount_sun),
    ownerShareSun: String(row.owner_share_sun),
    status: String(row.status) as PurchaseProcessingStatus,
    failureReason: normalizeOptionalString(row.failure_reason),
    source: String(row.source) as PurchaseSource,
    allocationMode: normalizeAllocationMode(row.allocation_mode),
    allocationAttempts: Number(row.allocation_attempts || 0),
    lastAllocationAttemptAt:
      row.last_allocation_attempt_at_ms == null
        ? null
        : Number(row.last_allocation_attempt_at_ms),
    lastAllocationErrorCode: normalizeOptionalString(row.last_allocation_error_code),
    lastAllocationErrorMessage: normalizeOptionalString(
      row.last_allocation_error_message
    ),
    deferredReason: normalizeOptionalString(row.deferred_reason),
    withdrawSessionId: normalizeOptionalString(row.withdraw_session_id),
    createdAt: Number(row.created_at_ms),
    updatedAt: Number(row.updated_at_ms),
    allocatedAt: row.allocated_at_ms == null ? null : Number(row.allocated_at_ms)
  };
}

function rowToCabinetStatsRecord(row: any): CabinetStatsRecord {
  const requestedCount = Number(row.requested_for_processing_count || 0);

  return {
    totalBuyers: Number(row.total_buyers || 0),
    trackedVolumeSun: String(row.tracked_volume_sun || "0"),

    /**
     * These remain zero by design.
     * Contract-derived cabinet service must populate real on-chain fields later.
     */
    claimableRewardsSun: "0",
    availableOnChainSun: "0",
    availableOnChainCount: 0,

    allocatedInDbSun: String(row.allocated_in_db_sun || "0"),
    allocatedInDbCount: Number(row.allocated_in_db_count || 0),

    lifetimeRewardsSun: String(row.lifetime_rewards_sun || "0"),
    withdrawnRewardsSun: String(row.withdrawn_rewards_sun || "0"),

    pendingBackendSyncSun: String(row.pending_backend_sync_sun || "0"),
    requestedForProcessingSun: String(row.requested_for_processing_sun || "0"),
    pendingBackendSyncCount: Number(row.pending_backend_sync_count || 0),
    requestedForProcessingCount: requestedCount,

    hasProcessingWithdrawal: requestedCount > 0
  };
}

function mapPgConflict(error: unknown): Error {
  const isPgUniqueViolation =
    !!error &&
    typeof error === "object" &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505";

  if (isPgUniqueViolation) {
    const constraint =
      "constraint" in (error as Record<string, unknown>) &&
      typeof (error as Record<string, unknown>).constraint === "string"
        ? String((error as Record<string, unknown>).constraint)
        : "";

    if (constraint.includes("purchase_id")) {
      return new Error("Purchase already exists for purchaseId");
    }

    if (constraint.includes("tx_hash")) {
      return new Error("Purchase already exists for txHash");
    }

    return new Error("Purchase already exists");
  }

  return error instanceof Error ? error : new Error("Purchase store error");
}

function buildSelectSql(): string {
  return `
    SELECT
      purchase_id,
      tx_hash,
      buyer_wallet,
      ambassador_slug,
      ambassador_wallet,
      purchase_amount_sun,
      owner_share_sun,
      status,
      failure_reason,
      source,
      allocation_mode,
      allocation_attempts,
      CASE
        WHEN last_allocation_attempt_at IS NULL THEN NULL
        ELSE FLOOR(EXTRACT(EPOCH FROM last_allocation_attempt_at) * 1000)
      END AS last_allocation_attempt_at_ms,
      last_allocation_error_code,
      last_allocation_error_message,
      deferred_reason,
      withdraw_session_id,
      FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
      FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms,
      CASE
        WHEN allocated_at IS NULL THEN NULL
        ELSE FLOOR(EXTRACT(EPOCH FROM allocated_at) * 1000)
      END AS allocated_at_ms
    FROM purchases
  `;
}

export async function initPurchaseTables(): Promise<void> {
  await query(`
    CREATE TABLE IF NOT EXISTS purchases (
      purchase_id TEXT PRIMARY KEY,
      tx_hash TEXT NOT NULL UNIQUE,
      buyer_wallet TEXT NOT NULL,
      ambassador_slug TEXT NULL,
      ambassador_wallet TEXT NULL,
      purchase_amount_sun TEXT NOT NULL DEFAULT '0',
      owner_share_sun TEXT NOT NULL DEFAULT '0',
      status TEXT NOT NULL,
      failure_reason TEXT NULL,
      source TEXT NOT NULL,
      allocation_mode TEXT NULL,
      allocation_attempts INTEGER NOT NULL DEFAULT 0,
      last_allocation_attempt_at TIMESTAMPTZ NULL,
      last_allocation_error_code TEXT NULL,
      last_allocation_error_message TEXT NULL,
      deferred_reason TEXT NULL,
      withdraw_session_id TEXT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      allocated_at TIMESTAMPTZ NULL
    )
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS allocation_mode TEXT NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS allocation_attempts INTEGER NOT NULL DEFAULT 0
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS last_allocation_attempt_at TIMESTAMPTZ NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS last_allocation_error_code TEXT NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS last_allocation_error_message TEXT NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS deferred_reason TEXT NULL
  `);

  await query(`
    ALTER TABLE purchases
    ADD COLUMN IF NOT EXISTS withdraw_session_id TEXT NULL
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_tx_hash
    ON purchases(tx_hash)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_status
    ON purchases(status)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_buyer_wallet
    ON purchases(buyer_wallet)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_ambassador_slug
    ON purchases(ambassador_slug)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_ambassador_wallet
    ON purchases(ambassador_wallet)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_status_ambassador_wallet_created_at
    ON purchases(status, ambassador_wallet, created_at)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_withdraw_session_id
    ON purchases(withdraw_session_id)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_purchases_retry_queue
    ON purchases(status, updated_at)
  `);
}

async function getAmbassadorBySlugFromDb(slug: string): Promise<AmbassadorStoreRecord | null> {
  const normalizedSlug = assertNonEmpty(slug, "slug").toLowerCase();

  const result = await query(
    `
      SELECT
        p.id,
        p.slug,
        p.slug_hash,
        p.status,
        i.wallet
      FROM ambassador_public_profiles p
      LEFT JOIN ambassador_private_identities i
        ON i.ambassador_id = p.id
      WHERE p.slug = $1
      LIMIT 1
    `,
    [normalizedSlug]
  );

  const row = result.rows[0];

  if (!row) {
    return null;
  }

  return {
    slug: String(row.slug),
    slugHash: String(row.slug_hash),
    status: String(row.status) as AmbassadorStoreRecord["status"],
    wallet: normalizeWallet(row.wallet),
    ambassadorId: row.id == null ? null : String(row.id)
  };
}

export class PostgresPurchaseStore implements PurchaseStore {
  async getByPurchaseId(purchaseId: string): Promise<PurchaseRecord | null> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");

    const result = await query(
      `
        ${buildSelectSql()}
        WHERE purchase_id = $1
        LIMIT 1
      `,
      [normalizedPurchaseId]
    );

    const row = result.rows[0];
    return row ? rowToPurchaseRecord(row) : null;
  }

  async getByTxHash(txHash: string): Promise<PurchaseRecord | null> {
    const normalizedTxHash = normalizeTxHash(txHash);

    const result = await query(
      `
        ${buildSelectSql()}
        WHERE tx_hash = $1
        LIMIT 1
      `,
      [normalizedTxHash]
    );

    const row = result.rows[0];
    return row ? rowToPurchaseRecord(row) : null;
  }

  async getAmbassadorBySlug(slug: string): Promise<AmbassadorStoreRecord | null> {
    return getAmbassadorBySlugFromDb(slug);
  }

  async createOrGetReceivedPurchase(
    input: CreateOrGetReceivedPurchaseInput
  ): Promise<CreateOrGetReceivedPurchaseResult> {
    const txHash = normalizeTxHash(input.txHash);
    const existing = await this.getByTxHash(txHash);

    if (existing) {
      return {
        created: false,
        purchase: existing
      };
    }

    try {
      const created = await this.create({
        purchaseId: buildPurchaseIdFromTxHash(txHash),
        txHash,
        buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
        ambassadorSlug: normalizeOptionalString(input.ambassadorSlug),
        purchaseAmountSun: "0",
        ownerShareSun: "0",
        source: "frontend-attribution",
        status: "received",
        now: input.now
      });

      return {
        created: true,
        purchase: created
      };
    } catch (error) {
      const fallback = await this.getByTxHash(txHash);

      if (fallback) {
        return {
          created: false,
          purchase: fallback
        };
      }

      throw error;
    }
  }

  async attachAmbassadorToPurchase(
    input: AttachAmbassadorToPurchaseInput
  ): Promise<PurchaseRecord> {
    return this.update(input.purchaseId, {
      ambassadorSlug: input.ambassadorSlug,
      ambassadorWallet: input.ambassadorWallet,
      purchaseAmountSun: input.purchaseAmountSun ?? "0",
      ownerShareSun: input.ownerShareSun ?? "0",
      now: input.now
    });
  }

  async markVerifiedPurchase(
    input: MarkVerifiedPurchaseInput
  ): Promise<PurchaseRecord> {
    const current = await this.getByPurchaseId(input.purchaseId);

    if (!current) {
      throw new Error(`Purchase not found: ${input.purchaseId}`);
    }

    return this.markVerified(input.purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorSlug: current.ambassadorSlug,
      ambassadorWallet: current.ambassadorWallet,
      allocationMode: current.allocationMode,
      now: input.now
    });
  }

  async create(input: CreatePurchaseRecordInput): Promise<PurchaseRecord> {
    const record = createRecord(input);

    try {
      const result = await query(
        `
          INSERT INTO purchases (
            purchase_id,
            tx_hash,
            buyer_wallet,
            ambassador_slug,
            ambassador_wallet,
            purchase_amount_sun,
            owner_share_sun,
            status,
            failure_reason,
            source,
            allocation_mode,
            allocation_attempts,
            last_allocation_attempt_at,
            last_allocation_error_code,
            last_allocation_error_message,
            deferred_reason,
            withdraw_session_id,
            created_at,
            updated_at,
            allocated_at
          )
          VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12,
            CASE WHEN $13::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($13 / 1000.0) END,
            $14, $15, $16, $17,
            TO_TIMESTAMP($18 / 1000.0),
            TO_TIMESTAMP($19 / 1000.0),
            CASE WHEN $20::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($20 / 1000.0) END
          )
          RETURNING
            purchase_id,
            tx_hash,
            buyer_wallet,
            ambassador_slug,
            ambassador_wallet,
            purchase_amount_sun,
            owner_share_sun,
            status,
            failure_reason,
            source,
            allocation_mode,
            allocation_attempts,
            CASE
              WHEN last_allocation_attempt_at IS NULL THEN NULL
              ELSE FLOOR(EXTRACT(EPOCH FROM last_allocation_attempt_at) * 1000)
            END AS last_allocation_attempt_at_ms,
            last_allocation_error_code,
            last_allocation_error_message,
            deferred_reason,
            withdraw_session_id,
            FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
            FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms,
            CASE
              WHEN allocated_at IS NULL THEN NULL
              ELSE FLOOR(EXTRACT(EPOCH FROM allocated_at) * 1000)
            END AS allocated_at_ms
        `,
        [
          record.purchaseId,
          record.txHash,
          record.buyerWallet,
          record.ambassadorSlug,
          record.ambassadorWallet,
          record.purchaseAmountSun,
          record.ownerShareSun,
          record.status,
          record.failureReason,
          record.source,
          record.allocationMode,
          record.allocationAttempts,
          record.lastAllocationAttemptAt,
          record.lastAllocationErrorCode,
          record.lastAllocationErrorMessage,
          record.deferredReason,
          record.withdrawSessionId,
          record.createdAt,
          record.updatedAt,
          record.allocatedAt
        ]
      );

      return rowToPurchaseRecord(result.rows[0]);
    } catch (error) {
      throw mapPgConflict(error);
    }
  }

  async update(
    purchaseId: string,
    input: UpdatePurchaseRecordInput
  ): Promise<PurchaseRecord> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");
    const current = await this.getByPurchaseId(normalizedPurchaseId);

    if (!current) {
      throw new Error(`Purchase not found: ${normalizedPurchaseId}`);
    }

    const updated = mergeRecord(current, input);

    const result = await query(
      `
        UPDATE purchases
        SET
          purchase_amount_sun = $2,
          owner_share_sun = $3,
          ambassador_slug = $4,
          ambassador_wallet = $5,
          status = $6,
          failure_reason = $7,
          allocation_mode = $8,
          allocation_attempts = $9,
          last_allocation_attempt_at = CASE WHEN $10::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($10 / 1000.0) END,
          last_allocation_error_code = $11,
          last_allocation_error_message = $12,
          deferred_reason = $13,
          withdraw_session_id = $14,
          updated_at = TO_TIMESTAMP($15 / 1000.0),
          allocated_at = CASE WHEN $16::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($16 / 1000.0) END
        WHERE purchase_id = $1
        RETURNING
          purchase_id,
          tx_hash,
          buyer_wallet,
          ambassador_slug,
          ambassador_wallet,
          purchase_amount_sun,
          owner_share_sun,
          status,
          failure_reason,
          source,
          allocation_mode,
          allocation_attempts,
          CASE
            WHEN last_allocation_attempt_at IS NULL THEN NULL
            ELSE FLOOR(EXTRACT(EPOCH FROM last_allocation_attempt_at) * 1000)
          END AS last_allocation_attempt_at_ms,
          last_allocation_error_code,
          last_allocation_error_message,
          deferred_reason,
          withdraw_session_id,
          FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
          FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms,
          CASE
            WHEN allocated_at IS NULL THEN NULL
            ELSE FLOOR(EXTRACT(EPOCH FROM allocated_at) * 1000)
          END AS allocated_at_ms
      `,
      [
        normalizedPurchaseId,
        updated.purchaseAmountSun,
        updated.ownerShareSun,
        updated.ambassadorSlug,
        updated.ambassadorWallet,
        updated.status,
        updated.failureReason,
        updated.allocationMode,
        updated.allocationAttempts,
        updated.lastAllocationAttemptAt,
        updated.lastAllocationErrorCode,
        updated.lastAllocationErrorMessage,
        updated.deferredReason,
        updated.withdrawSessionId,
        updated.updatedAt,
        updated.allocatedAt
      ]
    );

    return rowToPurchaseRecord(result.rows[0]);
  }

  async markVerified(
    purchaseId: string,
    input: {
      purchaseAmountSun: string;
      ownerShareSun: string;
      ambassadorSlug?: string | null;
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorSlug: input.ambassadorSlug,
      ambassadorWallet: input.ambassadorWallet,
      status: "verified",
      failureReason: null,
      allocationMode: input.allocationMode,
      deferredReason: null,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      withdrawSessionId: null,
      now: input.now
    });
  }

  async markDeferred(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "deferred",
      failureReason: null,
      deferredReason: assertNonEmpty(input.reason, "reason"),
      allocationMode: input.allocationMode ?? "deferred",
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ??
        assertNonEmpty(input.reason, "reason"),
      now
    });
  }

  async markAllocationInProgress(
    purchaseId: string,
    input?: {
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input?.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_in_progress",
      failureReason: null,
      allocationMode: input?.allocationMode,
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      deferredReason: null,
      now
    });
  }

  async markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input?.now ?? Date.now();

    return this.update(purchaseId, {
      ambassadorWallet: input?.ambassadorWallet,
      status: "allocated",
      failureReason: null,
      allocationMode: input?.allocationMode,
      deferredReason: null,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      allocatedAt: now,
      now
    });
  }

  async markAllocationRetryableFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const reason = assertNonEmpty(input.reason, "reason");
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_failed_retryable",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markAllocationFinalFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const reason = assertNonEmpty(input.reason, "reason");
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_failed_final",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markWithdrawIncluded(
    purchaseId: string,
    input: {
      withdrawSessionId: string;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "withdraw_included",
      withdrawSessionId: assertNonEmpty(input.withdrawSessionId, "withdrawSessionId"),
      now
    });
  }

  async markWithdrawCompleted(
    purchaseId: string,
    input?: {
      withdrawSessionId?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input?.now ?? Date.now();

    return this.update(purchaseId, {
      status: "withdraw_completed",
      withdrawSessionId:
        input?.withdrawSessionId !== undefined
          ? normalizeOptionalString(input.withdrawSessionId)
          : null,
      now
    });
  }

  async markFailed(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.markAllocationRetryableFailed(purchaseId, {
      reason,
      allocationMode: "manual-replay",
      now
    });
  }

  async markIgnored(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "ignored",
      failureReason: assertNonEmpty(reason, "reason"),
      now
    });
  }

  async assignWithdrawSession(
    purchaseId: string,
    withdrawSessionId: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      withdrawSessionId: assertNonEmpty(withdrawSessionId, "withdrawSessionId"),
      now
    });
  }

  async clearWithdrawSession(
    purchaseId: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      withdrawSessionId: null,
      now
    });
  }

  async listReplayableFailures(limit?: number): Promise<PurchaseRecord[]> {
    const params: unknown[] = [];
    let sql = `
      ${buildSelectSql()}
      WHERE status IN ('deferred', 'allocation_failed_retryable')
      ORDER BY created_at ASC
    `;

    if (limit && limit > 0) {
      params.push(Math.floor(limit));
      sql += ` LIMIT $1`;
    }

    const result = await query(sql, params);
    return result.rows.map(rowToPurchaseRecord);
  }

  async listPendingByAmbassador(
    input: PendingPurchaseQuery
  ): Promise<PurchaseRecord[]> {
    const ambassadorWallet = assertNonEmpty(
      input.ambassadorWallet,
      "ambassadorWallet"
    );
    const statuses = normalizePendingStatuses(input.statuses);
    const limit = input.limit && input.limit > 0 ? Math.floor(input.limit) : null;

    const params: unknown[] = [ambassadorWallet, statuses];
    let sql = `
      ${buildSelectSql()}
      WHERE ambassador_wallet = $1
        AND status = ANY($2::text[])
      ORDER BY created_at ASC
    `;

    if (limit != null) {
      params.push(limit);
      sql += ` LIMIT $3`;
    }

    const result = await query(sql, params);
    return result.rows.map(rowToPurchaseRecord);
  }

  async getCabinetStatsByAmbassadorWallet(
    ambassadorWallet: string
  ): Promise<CabinetStatsRecord> {
    const normalizedAmbassadorWallet = assertNonEmpty(
      ambassadorWallet,
      "ambassadorWallet"
    );

    const result = await query(
      `
        SELECT
          COUNT(DISTINCT CASE
            WHEN status <> 'received' AND buyer_wallet <> '' THEN buyer_wallet
            ELSE NULL
          END) AS total_buyers,

          COALESCE(SUM(CASE
            WHEN status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocated',
              'allocation_failed_retryable',
              'allocation_failed_final',
              'withdraw_included',
              'withdraw_completed'
            ) THEN purchase_amount_sun::numeric
            ELSE 0
          END)::text, '0') AS tracked_volume_sun,

          COALESCE(SUM(CASE
            WHEN status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocated',
              'allocation_failed_retryable',
              'allocation_failed_final',
              'withdraw_included',
              'withdraw_completed'
            ) THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS lifetime_rewards_sun,

          COALESCE(SUM(CASE
            WHEN status = 'withdraw_completed' THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS withdrawn_rewards_sun,

          COALESCE(SUM(CASE
            WHEN status = 'allocated'
              AND withdraw_session_id IS NULL
              AND owner_share_sun::numeric > 0
            THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS allocated_in_db_sun,

          COUNT(*) FILTER (
            WHERE status = 'allocated'
              AND withdraw_session_id IS NULL
              AND owner_share_sun::numeric > 0
          ) AS allocated_in_db_count,

          COALESCE(SUM(CASE
            WHEN status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocation_failed_retryable'
            )
              AND withdraw_session_id IS NULL
              AND owner_share_sun::numeric > 0
            THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS pending_backend_sync_sun,

          COUNT(*) FILTER (
            WHERE status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocation_failed_retryable'
            )
              AND withdraw_session_id IS NULL
              AND owner_share_sun::numeric > 0
          ) AS pending_backend_sync_count,

          COALESCE(SUM(CASE
            WHEN status IN ('withdraw_included')
              AND withdraw_session_id IS NOT NULL
              AND owner_share_sun::numeric > 0
            THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS requested_for_processing_sun,

          COUNT(*) FILTER (
            WHERE status IN ('withdraw_included')
              AND withdraw_session_id IS NOT NULL
              AND owner_share_sun::numeric > 0
          ) AS requested_for_processing_count
        FROM purchases
        WHERE ambassador_wallet = $1
      `,
      [normalizedAmbassadorWallet]
    );

    const row = result.rows[0];
    return row ? rowToCabinetStatsRecord(row) : emptyCabinetStatsRecord();
  }

  async hasProcessedPurchase(purchaseId: string): Promise<boolean> {
    const record = await this.getByPurchaseId(purchaseId);

    if (!record) {
      return false;
    }

    return (
      record.status === "allocated" ||
      record.status === "withdraw_completed" ||
      record.status === "ignored" ||
      record.status === "allocation_failed_final"
    );
  }
}

export class InMemoryPurchaseStore implements PurchaseStore {
  private readonly byPurchaseId = new Map<string, PurchaseRecord>();
  private readonly purchaseIdByTxHash = new Map<string, string>();
  private readonly ambassadorsBySlug = new Map<string, AmbassadorStoreRecord>();

  async getByPurchaseId(purchaseId: string): Promise<PurchaseRecord | null> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");
    return this.byPurchaseId.get(normalizedPurchaseId) ?? null;
  }

  async getByTxHash(txHash: string): Promise<PurchaseRecord | null> {
    const normalizedTxHash = normalizeTxHash(txHash);
    const purchaseId = this.purchaseIdByTxHash.get(normalizedTxHash);

    if (!purchaseId) {
      return null;
    }

    return this.byPurchaseId.get(purchaseId) ?? null;
  }

  async getAmbassadorBySlug(slug: string): Promise<AmbassadorStoreRecord | null> {
    const normalizedSlug = assertNonEmpty(slug, "slug").toLowerCase();
    return this.ambassadorsBySlug.get(normalizedSlug) ?? null;
  }

  async createOrGetReceivedPurchase(
    input: CreateOrGetReceivedPurchaseInput
  ): Promise<CreateOrGetReceivedPurchaseResult> {
    const txHash = normalizeTxHash(input.txHash);
    const existing = await this.getByTxHash(txHash);

    if (existing) {
      return {
        created: false,
        purchase: existing
      };
    }

    const created = await this.create({
      purchaseId: buildPurchaseIdFromTxHash(txHash),
      txHash,
      buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
      ambassadorSlug: normalizeOptionalString(input.ambassadorSlug),
      purchaseAmountSun: "0",
      ownerShareSun: "0",
      source: "frontend-attribution",
      status: "received",
      now: input.now
    });

    return {
      created: true,
      purchase: created
    };
  }

  async attachAmbassadorToPurchase(
    input: AttachAmbassadorToPurchaseInput
  ): Promise<PurchaseRecord> {
    return this.update(input.purchaseId, {
      ambassadorSlug: input.ambassadorSlug,
      ambassadorWallet: input.ambassadorWallet,
      purchaseAmountSun: input.purchaseAmountSun ?? "0",
      ownerShareSun: input.ownerShareSun ?? "0",
      now: input.now
    });
  }

  async markVerifiedPurchase(
    input: MarkVerifiedPurchaseInput
  ): Promise<PurchaseRecord> {
    const current = await this.getByPurchaseId(input.purchaseId);

    if (!current) {
      throw new Error(`Purchase not found: ${input.purchaseId}`);
    }

    return this.markVerified(input.purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorSlug: current.ambassadorSlug,
      ambassadorWallet: current.ambassadorWallet,
      allocationMode: current.allocationMode,
      now: input.now
    });
  }

  async create(input: CreatePurchaseRecordInput): Promise<PurchaseRecord> {
    const record = createRecord(input);

    if (this.byPurchaseId.has(record.purchaseId)) {
      throw new Error(`Purchase already exists for purchaseId: ${record.purchaseId}`);
    }

    if (this.purchaseIdByTxHash.has(record.txHash)) {
      throw new Error(`Purchase already exists for txHash: ${record.txHash}`);
    }

    this.byPurchaseId.set(record.purchaseId, record);
    this.purchaseIdByTxHash.set(record.txHash, record.purchaseId);

    return record;
  }

  async update(
    purchaseId: string,
    input: UpdatePurchaseRecordInput
  ): Promise<PurchaseRecord> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");
    const current = this.byPurchaseId.get(normalizedPurchaseId);

    if (!current) {
      throw new Error(`Purchase not found: ${normalizedPurchaseId}`);
    }

    const updated = mergeRecord(current, input);
    this.byPurchaseId.set(normalizedPurchaseId, updated);

    return updated;
  }

  async markVerified(
    purchaseId: string,
    input: {
      purchaseAmountSun: string;
      ownerShareSun: string;
      ambassadorSlug?: string | null;
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      ambassadorSlug: input.ambassadorSlug,
      ambassadorWallet: input.ambassadorWallet,
      status: "verified",
      failureReason: null,
      allocationMode: input.allocationMode,
      deferredReason: null,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      withdrawSessionId: null,
      now: input.now
    });
  }

  async markDeferred(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "deferred",
      failureReason: null,
      deferredReason: assertNonEmpty(input.reason, "reason"),
      allocationMode: input.allocationMode ?? "deferred",
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ??
        assertNonEmpty(input.reason, "reason"),
      now
    });
  }

  async markAllocationInProgress(
    purchaseId: string,
    input?: {
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input?.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_in_progress",
      failureReason: null,
      allocationMode: input?.allocationMode,
      incrementAllocationAttempts: true,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      deferredReason: null,
      now
    });
  }

  async markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
      allocationMode?: AllocationMode;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const now = input?.now ?? Date.now();

    return this.update(purchaseId, {
      ambassadorWallet: input?.ambassadorWallet,
      status: "allocated",
      failureReason: null,
      allocationMode: input?.allocationMode,
      deferredReason: null,
      lastAllocationErrorCode: null,
      lastAllocationErrorMessage: null,
      allocatedAt: now,
      now
    });
  }

  async markAllocationRetryableFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const reason = assertNonEmpty(input.reason, "reason");
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_failed_retryable",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markAllocationFinalFailed(
    purchaseId: string,
    input: {
      reason: string;
      allocationMode?: AllocationMode;
      errorCode?: string | null;
      errorMessage?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const reason = assertNonEmpty(input.reason, "reason");
    const now = input.now ?? Date.now();

    return this.update(purchaseId, {
      status: "allocation_failed_final",
      failureReason: reason,
      allocationMode: input.allocationMode,
      lastAllocationAttemptAt: now,
      lastAllocationErrorCode: normalizeOptionalString(input.errorCode),
      lastAllocationErrorMessage:
        normalizeOptionalString(input.errorMessage) ?? reason,
      now
    });
  }

  async markWithdrawIncluded(
    purchaseId: string,
    input: {
      withdrawSessionId: string;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "withdraw_included",
      withdrawSessionId: assertNonEmpty(input.withdrawSessionId, "withdrawSessionId"),
      now: input.now
    });
  }

  async markWithdrawCompleted(
    purchaseId: string,
    input?: {
      withdrawSessionId?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "withdraw_completed",
      withdrawSessionId:
        input?.withdrawSessionId !== undefined
          ? normalizeOptionalString(input.withdrawSessionId)
          : null,
      now: input?.now
    });
  }

  async markFailed(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.markAllocationRetryableFailed(purchaseId, {
      reason,
      allocationMode: "manual-replay",
      now
    });
  }

  async markIgnored(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      status: "ignored",
      failureReason: assertNonEmpty(reason, "reason"),
      now
    });
  }

  async assignWithdrawSession(
    purchaseId: string,
    withdrawSessionId: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      withdrawSessionId: assertNonEmpty(withdrawSessionId, "withdrawSessionId"),
      now
    });
  }

  async clearWithdrawSession(
    purchaseId: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.update(purchaseId, {
      withdrawSessionId: null,
      now
    });
  }

  async listReplayableFailures(limit?: number): Promise<PurchaseRecord[]> {
    let rows = Array.from(this.byPurchaseId.values())
      .filter(
        (record) =>
          record.status === "deferred" ||
          record.status === "allocation_failed_retryable"
      )
      .sort((left, right) => left.createdAt - right.createdAt);

    if (limit && limit > 0) {
      rows = rows.slice(0, Math.floor(limit));
    }

    return rows;
  }

  async listPendingByAmbassador(
    input: PendingPurchaseQuery
  ): Promise<PurchaseRecord[]> {
    const ambassadorWallet = assertNonEmpty(
      input.ambassadorWallet,
      "ambassadorWallet"
    );
    const statuses = normalizePendingStatuses(input.statuses);
    const allowed = new Set(statuses);

    let rows = Array.from(this.byPurchaseId.values())
      .filter(
        (record) =>
          record.ambassadorWallet === ambassadorWallet &&
          allowed.has(record.status)
      )
      .sort((left, right) => left.createdAt - right.createdAt);

    if (input.limit && input.limit > 0) {
      rows = rows.slice(0, Math.floor(input.limit));
    }

    return rows;
  }

  async getCabinetStatsByAmbassadorWallet(
    ambassadorWallet: string
  ): Promise<CabinetStatsRecord> {
    const normalizedAmbassadorWallet = assertNonEmpty(
      ambassadorWallet,
      "ambassadorWallet"
    );

    const rows = Array.from(this.byPurchaseId.values()).filter(
      (record) => record.ambassadorWallet === normalizedAmbassadorWallet
    );

    const buyers = new Set<string>();

    let trackedVolumeSun = "0";
    let lifetimeRewardsSun = "0";
    let withdrawnRewardsSun = "0";
    let allocatedInDbSun = "0";
    let pendingBackendSyncSun = "0";
    let requestedForProcessingSun = "0";

    let allocatedInDbCount = 0;
    let pendingBackendSyncCount = 0;
    let requestedForProcessingCount = 0;

    for (const row of rows) {
      if (row.status !== "received" && row.buyerWallet) {
        buyers.add(row.buyerWallet);
      }

      const rewardAmount = BigInt(row.ownerShareSun || "0");

      const contributesVolume =
        row.status === "verified" ||
        row.status === "deferred" ||
        row.status === "allocation_in_progress" ||
        row.status === "allocated" ||
        row.status === "allocation_failed_retryable" ||
        row.status === "allocation_failed_final" ||
        row.status === "withdraw_included" ||
        row.status === "withdraw_completed";

      if (contributesVolume) {
        trackedVolumeSun = sumSunStrings(trackedVolumeSun, row.purchaseAmountSun);
        lifetimeRewardsSun = sumSunStrings(lifetimeRewardsSun, row.ownerShareSun);
      }

      if (row.status === "withdraw_completed" && rewardAmount > 0n) {
        withdrawnRewardsSun = sumSunStrings(withdrawnRewardsSun, row.ownerShareSun);
      }

      const isAllocatedInDb =
        row.status === "allocated" &&
        !row.withdrawSessionId &&
        rewardAmount > 0n;

      if (isAllocatedInDb) {
        allocatedInDbSun = sumSunStrings(allocatedInDbSun, row.ownerShareSun);
        allocatedInDbCount += 1;
      }

      const isPendingBackendSync =
        (row.status === "verified" ||
          row.status === "deferred" ||
          row.status === "allocation_in_progress" ||
          row.status === "allocation_failed_retryable") &&
        !row.withdrawSessionId &&
        rewardAmount > 0n;

      if (isPendingBackendSync) {
        pendingBackendSyncSun = sumSunStrings(pendingBackendSyncSun, row.ownerShareSun);
        pendingBackendSyncCount += 1;
      }

      const isRequestedForProcessing =
        row.status === "withdraw_included" &&
        !!row.withdrawSessionId &&
        rewardAmount > 0n;

      if (isRequestedForProcessing) {
        requestedForProcessingSun = sumSunStrings(
          requestedForProcessingSun,
          row.ownerShareSun
        );
        requestedForProcessingCount += 1;
      }
    }

    return {
      totalBuyers: buyers.size,
      trackedVolumeSun,
      claimableRewardsSun: "0",
      availableOnChainSun: "0",
      availableOnChainCount: 0,
      allocatedInDbSun,
      allocatedInDbCount,
      pendingBackendSyncSun,
      requestedForProcessingSun,
      pendingBackendSyncCount,
      requestedForProcessingCount,
      lifetimeRewardsSun,
      withdrawnRewardsSun,
      hasProcessingWithdrawal: requestedForProcessingCount > 0
    };
  }

  async hasProcessedPurchase(purchaseId: string): Promise<boolean> {
    const record = await this.getByPurchaseId(purchaseId);

    if (!record) {
      return false;
    }

    return (
      record.status === "allocated" ||
      record.status === "withdraw_completed" ||
      record.status === "ignored" ||
      record.status === "allocation_failed_final"
    );
  }
}

export function createPurchaseStore(): PurchaseStore {
  return new PostgresPurchaseStore();
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/domain/allocation.ts

```ts
import type {
  AllocationMode,
  PurchaseRecord,
  PurchaseStore
} from "../db/purchases";

export interface AllocationDependencies {
  store: PurchaseStore;
  executor: AllocationExecutor;
  now?: () => number;
  logger?: AllocationLogger;
}

export interface AllocationLogger {
  info?(payload: Record<string, unknown>): void;
  warn?(payload: Record<string, unknown>): void;
  error?(payload: Record<string, unknown>): void;
}

export interface AllocationExecutorInput {
  purchase: PurchaseRecord;
  feeLimitSun?: number;
}

export interface AllocationExecutorResult {
  txid: string;
}

export interface AllocationExecutor {
  allocate(input: AllocationExecutorInput): Promise<AllocationExecutorResult>;
}

export type AllocationAttemptStatus =
  | "allocated"
  | "deferred"
  | "retryable-failed"
  | "final-failed"
  | "skipped-already-final"
  | "skipped-no-ambassador-wallet"
  | "stopped-on-resource-shortage";

export interface AllocationAttemptResult {
  status: AllocationAttemptStatus;
  purchase: PurchaseRecord;
  txid: string | null;
  reason: string | null;
  errorCode: string | null;
  errorMessage: string | null;
}

export type AllocationDecision = AllocationAttemptResult;

export interface ReplayFailedAllocationResult {
  status: "allocated" | "failed" | "skipped";
  purchase: PurchaseRecord;
  ambassadorWallet: string | null;
  txid: string | null;
  reason: string | null;
  errorCode: string | null;
  errorMessage: string | null;
}

export interface PrepareWithdrawBatchInput {
  ambassadorWallet: string;
  limit?: number;
}

export interface PrepareWithdrawBatchResult {
  ambassadorWallet: string;
  purchases: PurchaseRecord[];
}

export interface AllocatePendingBatchInput {
  ambassadorWallet: string;
  feeLimitSun?: number;
  limit?: number;
  allocationMode?: AllocationMode;
  stopOnFirstDeferred?: boolean;
}

export interface AllocatePendingBatchResult {
  ambassadorWallet: string;
  processed: AllocationAttemptResult[];
  stoppedEarly: boolean;
  stopReason: string | null;
}

type AllocationErrorKind = "resource" | "retryable" | "final" | "unknown";

interface ClassifiedAllocationError {
  kind: AllocationErrorKind;
  code: string | null;
  reason: string;
  message: string;
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

  try {
    return JSON.stringify(error);
  } catch {
    return "Unknown allocation error";
  }
}

function toLowerSafe(value: unknown): string {
  return typeof value === "string" ? value.toLowerCase() : "";
}

function extractErrorCode(error: unknown): string | null {
  if (!error || typeof error !== "object") {
    return null;
  }

  const candidates: unknown[] = [
    (error as any).code,
    (error as any).errorCode,
    (error as any).error_code,
    (error as any).name,
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

function isResourceInsufficientMessage(message: string): boolean {
  const value = message.toLowerCase();

  return (
    value.includes("account resource insufficient") ||
    value.includes("resource insufficient") ||
    value.includes("out of energy") ||
    value.includes("energy limit") ||
    value.includes("insufficient energy") ||
    value.includes("insufficient bandwidth") ||
    value.includes("bandwidth limit") ||
    value.includes("account resources are insufficient") ||
    value.includes("not enough energy") ||
    value.includes("not enough bandwidth") ||
    value.includes("gasstation service balance") ||
    value.includes("gasstation balance") ||
    value.includes("gasstation operator balance") ||
    value.includes("top-up") ||
    value.includes("top up")
  );
}

function isRateLimitedMessage(message: string): boolean {
  const value = message.toLowerCase();

  return (
    value.includes("status code 429") ||
    value.includes("http 429") ||
    value.includes("429") ||
    value.includes("too many requests") ||
    value.includes("rate limit") ||
    value.includes("rate limited")
  );
}

function isRetryableTransportMessage(message: string): boolean {
  const value = message.toLowerCase();

  return (
    value.includes("timeout") ||
    value.includes("timed out") ||
    value.includes("network") ||
    value.includes("socket hang up") ||
    value.includes("econnreset") ||
    value.includes("503") ||
    value.includes("502") ||
    value.includes("gateway") ||
    value.includes("temporar") ||
    value.includes("service unavailable")
  );
}

function isFinalMessage(message: string): boolean {
  const value = message.toLowerCase();

  return (
    value.includes("contract validate error") ||
    value.includes("revert") ||
    value.includes("invalid address") ||
    value.includes("owner address is not set") ||
    value.includes("purchase not eligible") ||
    value.includes("already allocated") ||
    value.includes("already processed") ||
    value.includes("permission denied")
  );
}

function isKnownRetryableCode(lowerCode: string): boolean {
  return (
    lowerCode === "429" ||
    lowerCode.includes("rate_limit") ||
    lowerCode.includes("rate limited") ||
    lowerCode.includes("too_many_requests") ||
    lowerCode.includes("timeout") ||
    lowerCode.includes("network") ||
    lowerCode.includes("econnreset") ||
    lowerCode.includes("temporar") ||
    lowerCode.includes("gasstation_rate_limit") ||
    lowerCode.includes("tron_rate_limit") ||
    lowerCode.includes("gasstation_order_failed") ||
    lowerCode.includes("gasstation_topup_transfer_failed") ||
    lowerCode.includes("gasstation_topup_not_settled") ||
    lowerCode.includes("gasstation_topup_failed") ||
    lowerCode.includes("gasstation_fetch_failed") ||
    lowerCode.includes("gasstation_invalid_response") ||
    lowerCode.includes("gasstation_timeout") ||
    lowerCode.includes("gasstation_http_5") ||
    lowerCode.includes("gasstation_error_100003")
  );
}

function isKnownResourceCode(lowerCode: string): boolean {
  return (
    lowerCode.includes("out_of_energy") ||
    lowerCode.includes("insufficient") ||
    lowerCode.includes("bandwidth") ||
    lowerCode.includes("energy") ||
    lowerCode.includes("account_resource") ||
    lowerCode.includes("gasstation_operator_balance_low") ||
    lowerCode.includes("gasstation_service_balance_low") ||
    lowerCode.includes("gasstation_topup_not_settled") ||
    lowerCode.includes("gasstation_topup_transfer_failed") ||
    lowerCode.includes("gasstation_topup_failed") ||
    lowerCode.includes("account_resource_consumed_before_send") ||
    lowerCode.includes("account_resource_insufficient_during_send")
  );
}

function isKnownFinalCode(lowerCode: string): boolean {
  return (
    lowerCode.includes("invalid_status") ||
    lowerCode.includes("no_ambassador_wallet")
  );
}

function classifyAllocationError(error: unknown): ClassifiedAllocationError {
  const message = toErrorMessage(error);
  const code = extractErrorCode(error);
  const lowerCode = toLowerSafe(code);

  if (isKnownResourceCode(lowerCode) || isResourceInsufficientMessage(message)) {
    return {
      kind: "resource",
      code,
      reason: message,
      message
    };
  }

  if (
    isRateLimitedMessage(message) ||
    lowerCode === "429" ||
    lowerCode.includes("rate_limit") ||
    lowerCode.includes("rate limited") ||
    lowerCode.includes("too_many_requests") ||
    lowerCode.includes("gasstation_rate_limit") ||
    lowerCode.includes("tron_rate_limit")
  ) {
    return {
      kind: "retryable",
      code,
      reason: "Temporary allocation rate limit error.",
      message
    };
  }

  if (
    isRetryableTransportMessage(message) ||
    lowerCode.includes("timeout") ||
    lowerCode.includes("network") ||
    lowerCode.includes("econnreset") ||
    lowerCode.includes("temporar") ||
    lowerCode.includes("gasstation_fetch_failed") ||
    lowerCode.includes("gasstation_invalid_response") ||
    lowerCode.includes("gasstation_timeout")
  ) {
    return {
      kind: "retryable",
      code,
      reason: "Temporary allocation transport error.",
      message
    };
  }

  if (isKnownFinalCode(lowerCode) || isFinalMessage(message)) {
    return {
      kind: "final",
      code,
      reason: message,
      message
    };
  }

  if (lowerCode === "err_bad_request" && !isRateLimitedMessage(message)) {
    return {
      kind: "final",
      code,
      reason: message,
      message
    };
  }

  if (isKnownRetryableCode(lowerCode)) {
    return {
      kind: "retryable",
      code,
      reason: message,
      message
    };
  }

  return {
    kind: "unknown",
    code,
    reason: message,
    message
  };
}

function isFinalPurchaseStatus(status: PurchaseRecord["status"]): boolean {
  return (
    status === "allocated" ||
    status === "withdraw_included" ||
    status === "withdraw_completed" ||
    status === "ignored" ||
    status === "allocation_failed_final"
  );
}

function isClaimQueueEligible(status: PurchaseRecord["status"]): boolean {
  return (
    status === "verified" ||
    status === "deferred" ||
    status === "allocation_failed_retryable"
  );
}

export class AllocationService {
  private readonly store: PurchaseStore;
  private readonly executor: AllocationExecutor;
  private readonly now: () => number;
  private readonly logger?: AllocationLogger;

  constructor(deps: AllocationDependencies) {
    this.store = deps.store;
    this.executor = deps.executor;
    this.now = deps.now ?? (() => Date.now());
    this.logger = deps.logger;
  }

  async executeAllocation(input: {
    purchaseId: string;
    feeLimitSun?: number;
    allocationMode?: AllocationMode;
    now?: number;
  }): Promise<AllocationDecision> {
    const purchase = await this.store.getByPurchaseId(input.purchaseId);

    if (!purchase) {
      throw new Error(`Purchase not found: ${input.purchaseId}`);
    }

    return this.tryAllocatePurchaseRecord(purchase, {
      feeLimitSun: input.feeLimitSun,
      allocationMode: input.allocationMode,
      nowOverride: input.now
    });
  }

  async tryAllocateVerifiedPurchase(
    purchaseId: string,
    options?: {
      feeLimitSun?: number;
      allocationMode?: AllocationMode;
    }
  ): Promise<AllocationAttemptResult> {
    const purchase = await this.store.getByPurchaseId(purchaseId);

    if (!purchase) {
      throw new Error(`Purchase not found: ${purchaseId}`);
    }

    return this.tryAllocatePurchaseRecord(purchase, {
      feeLimitSun: options?.feeLimitSun,
      allocationMode: options?.allocationMode ?? "eager"
    });
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<ReplayFailedAllocationResult> {
    const purchase = await this.store.getByPurchaseId(purchaseId);

    if (!purchase) {
      throw new Error(`Purchase not found: ${purchaseId}`);
    }

    if (isFinalPurchaseStatus(purchase.status) && purchase.status !== "allocated") {
      return {
        status: "skipped",
        purchase,
        ambassadorWallet: purchase.ambassadorWallet,
        txid: null,
        reason: `Purchase already finalized with status: ${purchase.status}`,
        errorCode: null,
        errorMessage: null
      };
    }

    const result = await this.tryAllocatePurchaseRecord(purchase, {
      feeLimitSun,
      allocationMode: "manual-replay",
      nowOverride: now
    });

    return {
      status:
        result.status === "allocated"
          ? "allocated"
          : result.status === "skipped-already-final" ||
              result.status === "skipped-no-ambassador-wallet"
            ? "skipped"
            : "failed",
      purchase: result.purchase,
      ambassadorWallet: result.purchase.ambassadorWallet,
      txid: result.txid,
      reason: result.reason,
      errorCode: result.errorCode,
      errorMessage: result.errorMessage
    };
  }

  async prepareWithdrawBatch(
    input: PrepareWithdrawBatchInput
  ): Promise<PrepareWithdrawBatchResult> {
    const ambassadorWallet = String(input.ambassadorWallet || "").trim();

    if (!ambassadorWallet) {
      throw new Error("ambassadorWallet is required");
    }

    const purchases = await this.store.listPendingByAmbassador({
      ambassadorWallet,
      limit: input.limit
    });

    return {
      ambassadorWallet,
      purchases
    };
  }

  async allocatePendingBatch(
    input: AllocatePendingBatchInput
  ): Promise<AllocatePendingBatchResult> {
    const ambassadorWallet = String(input.ambassadorWallet || "").trim();

    if (!ambassadorWallet) {
      throw new Error("ambassadorWallet is required");
    }

    const pending = await this.store.listPendingByAmbassador({
      ambassadorWallet,
      limit: input.limit
    });

    const processed: AllocationAttemptResult[] = [];
    let stoppedEarly = false;
    let stopReason: string | null = null;
    const stopOnFirstDeferred = input.stopOnFirstDeferred !== false;

    for (const purchase of pending) {
      const result = await this.tryAllocatePurchaseRecord(purchase, {
        feeLimitSun: input.feeLimitSun,
        allocationMode: input.allocationMode ?? "claim-first"
      });

      processed.push(result);

      if (
        stopOnFirstDeferred &&
        (result.status === "deferred" || result.status === "stopped-on-resource-shortage")
      ) {
        stoppedEarly = true;
        stopReason =
          result.reason || "Allocation stopped because resources were not sufficient.";
        break;
      }
    }

    return {
      ambassadorWallet,
      processed,
      stoppedEarly,
      stopReason
    };
  }

  private async tryAllocatePurchaseRecord(
    purchase: PurchaseRecord,
    options: {
      feeLimitSun?: number;
      allocationMode?: AllocationMode;
      nowOverride?: number;
    }
  ): Promise<AllocationAttemptResult> {
    const now = options.nowOverride ?? this.now();
    const allocationMode = options.allocationMode ?? "eager";

    if (isFinalPurchaseStatus(purchase.status)) {
      return {
        status: "skipped-already-final",
        purchase,
        txid: null,
        reason: `Purchase already finalized with status: ${purchase.status}`,
        errorCode: null,
        errorMessage: null
      };
    }

    if (!isClaimQueueEligible(purchase.status) && purchase.status !== "allocation_in_progress") {
      const refreshed = await this.store.markAllocationRetryableFailed(purchase.purchaseId, {
        reason: `Purchase is not eligible for allocation from status: ${purchase.status}`,
        allocationMode,
        errorCode: "INVALID_STATUS",
        errorMessage: `Purchase is not eligible for allocation from status: ${purchase.status}`,
        now
      });

      return {
        status: "retryable-failed",
        purchase: refreshed,
        txid: null,
        reason: refreshed.failureReason,
        errorCode: "INVALID_STATUS",
        errorMessage: `Purchase is not eligible for allocation from status: ${purchase.status}`
      };
    }

    if (!purchase.ambassadorWallet) {
      const deferred = await this.store.markDeferred(purchase.purchaseId, {
        reason: "Ambassador wallet is missing for this purchase.",
        allocationMode,
        errorCode: "NO_AMBASSADOR_WALLET",
        errorMessage: "Ambassador wallet is missing for this purchase.",
        now
      });

      return {
        status: "skipped-no-ambassador-wallet",
        purchase: deferred,
        txid: null,
        reason: "Ambassador wallet is missing for this purchase.",
        errorCode: "NO_AMBASSADOR_WALLET",
        errorMessage: "Ambassador wallet is missing for this purchase."
      };
    }

    const inProgress = await this.store.markAllocationInProgress(purchase.purchaseId, {
      allocationMode,
      now
    });

    try {
      this.logger?.info?.({
        scope: "allocation",
        stage: "execute-start",
        purchaseId: inProgress.purchaseId,
        txHash: inProgress.txHash,
        ambassadorWallet: inProgress.ambassadorWallet,
        allocationMode,
        feeLimitSun: options.feeLimitSun ?? null
      });

      const execution = await this.executor.allocate({
        purchase: inProgress,
        feeLimitSun: options.feeLimitSun
      });

      const allocated = await this.store.markAllocated(inProgress.purchaseId, {
        ambassadorWallet: inProgress.ambassadorWallet,
        allocationMode,
        now: this.now()
      });

      this.logger?.info?.({
        scope: "allocation",
        stage: "execute-success",
        purchaseId: allocated.purchaseId,
        txHash: allocated.txHash,
        ambassadorWallet: allocated.ambassadorWallet,
        allocationMode,
        txid: execution.txid
      });

      return {
        status: "allocated",
        purchase: allocated,
        txid: execution.txid,
        reason: null,
        errorCode: null,
        errorMessage: null
      };
    } catch (error) {
      const classified = classifyAllocationError(error);

      this.logger?.warn?.({
        scope: "allocation",
        stage: "execute-failed",
        purchaseId: inProgress.purchaseId,
        txHash: inProgress.txHash,
        ambassadorWallet: inProgress.ambassadorWallet,
        allocationMode,
        kind: classified.kind,
        code: classified.code,
        reason: classified.reason,
        message: classified.message
      });

      if (classified.kind === "resource") {
        const deferred = await this.store.markDeferred(inProgress.purchaseId, {
          reason: classified.reason,
          allocationMode,
          errorCode: classified.code,
          errorMessage: classified.message,
          now: this.now()
        });

        return {
          status: allocationMode === "claim-first" ? "stopped-on-resource-shortage" : "deferred",
          purchase: deferred,
          txid: null,
          reason: classified.reason,
          errorCode: classified.code,
          errorMessage: classified.message
        };
      }

      if (classified.kind === "retryable" || classified.kind === "unknown") {
        const failed = await this.store.markAllocationRetryableFailed(
          inProgress.purchaseId,
          {
            reason: classified.reason,
            allocationMode,
            errorCode: classified.code,
            errorMessage: classified.message,
            now: this.now()
          }
        );

        return {
          status: "retryable-failed",
          purchase: failed,
          txid: null,
          reason: classified.reason,
          errorCode: classified.code,
          errorMessage: classified.message
        };
      }

      const finalFailed = await this.store.markAllocationFinalFailed(
        inProgress.purchaseId,
        {
          reason: classified.reason,
          allocationMode,
          errorCode: classified.code,
          errorMessage: classified.message,
          now: this.now()
        }
      );

      return {
        status: "final-failed",
        purchase: finalFailed,
        txid: null,
        reason: classified.reason,
        errorCode: classified.code,
        errorMessage: classified.message
      };
    }
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/domain/attribution.ts

```ts
import { assertValidSlug, normalizeSlug } from "../../../../shared/utils/slug";
import { query } from "../db/postgres";
import { PurchaseRecord, PurchaseStore } from "../db/purchases";
import { ControllerClient } from "../tron/controller";

export interface FrontendAttributionInput {
  txHash: string;
  buyerWallet: string;
  slug: string;
  now?: number;
}

export interface VerifiedPurchaseInput {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  slug: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  now?: number;
}

export interface AttributionHashing {
  hashSlugToBytes32Hex(slug: string): string;
  derivePurchaseId(input: {
    txHash: string;
    buyerWallet: string;
  }): string;
}

export type AttributionDecisionStatus =
  | "ready-for-allocation"
  | "already-processed-on-chain"
  | "duplicate-local-record"
  | "ambassador-not-found"
  | "binding-not-allowed"
  | "ignored";

export interface AttributionDecision {
  status: AttributionDecisionStatus;
  purchase: PurchaseRecord;
  slug: string;
  slugHash: string;
  ambassadorWallet: string | null;
  reason: string | null;
}

export interface PrepareVerifiedPurchaseResult extends AttributionDecision {
  canAllocate: boolean;
}

export interface AttributionServiceConfig {
  store: PurchaseStore;
  controllerClient: ControllerClient;
  hashing: AttributionHashing;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeWallet(value: string, fieldName: string): string {
  return assertNonEmpty(value, fieldName);
}

function normalizeTxHash(value: string): string {
  return assertNonEmpty(value, "txHash").toLowerCase();
}

function normalizeSunAmount(value: string | number | bigint, fieldName: string): string {
  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${fieldName} must be a non-negative integer string`);
  }

  return normalized;
}

function normalizeIncomingSlug(value: string): string {
  return assertValidSlug(normalizeSlug(value));
}

async function getLocalAmbassadorWalletBySlug(slug: string): Promise<string | null> {
  const normalizedSlug = normalizeIncomingSlug(slug);

  const result = await query<{ wallet: string }>(
    `
      SELECT i.wallet
      FROM ambassador_public_profiles p
      INNER JOIN ambassador_private_identities i
        ON i.ambassador_id = p.id
      WHERE p.slug = $1
      LIMIT 1
    `,
    [normalizedSlug]
  );

  const wallet = String(result.rows[0]?.wallet || "").trim();
  return wallet || null;
}

export class AttributionService {
  private readonly store: PurchaseStore;
  private readonly controllerClient: ControllerClient;
  private readonly hashing: AttributionHashing;

  constructor(config: AttributionServiceConfig) {
    if (!config?.store) {
      throw new Error("store is required");
    }

    if (!config?.controllerClient) {
      throw new Error("controllerClient is required");
    }

    if (!config?.hashing) {
      throw new Error("hashing is required");
    }

    this.store = config.store;
    this.controllerClient = config.controllerClient;
    this.hashing = config.hashing;
  }

  async captureFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<AttributionDecision> {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const slug = normalizeIncomingSlug(input.slug);
    const now = input.now ?? Date.now();

    const purchaseId = this.hashing.derivePurchaseId({
      txHash,
      buyerWallet
    });

    const slugHash = this.hashing.hashSlugToBytes32Hex(slug);

    const existingByPurchaseId = await this.store.getByPurchaseId(purchaseId);
    if (existingByPurchaseId) {
      return {
        status: "duplicate-local-record",
        purchase: existingByPurchaseId,
        slug,
        slugHash,
        ambassadorWallet: existingByPurchaseId.ambassadorWallet,
        reason: "Purchase already exists in local store"
      };
    }

    const existingByTxHash = await this.store.getByTxHash(txHash);
    if (existingByTxHash) {
      return {
        status: "duplicate-local-record",
        purchase: existingByTxHash,
        slug,
        slugHash,
        ambassadorWallet: existingByTxHash.ambassadorWallet,
        reason: "Transaction already exists in local store"
      };
    }

    const ambassadorWallet = await getLocalAmbassadorWalletBySlug(slug);

    if (!ambassadorWallet) {
      const ignoredPurchase = await this.store.create({
        purchaseId,
        txHash,
        buyerWallet,
        ambassadorSlug: slug,
        ambassadorWallet: null,
        purchaseAmountSun: "0",
        ownerShareSun: "0",
        source: "frontend-attribution",
        status: "ignored",
        failureReason: "Ambassador wallet not found in local registry",
        now
      });

      return {
        status: "ambassador-not-found",
        purchase: ignoredPurchase,
        slug,
        slugHash,
        ambassadorWallet: null,
        reason: "Ambassador wallet not found in local registry"
      };
    }

    const purchase = await this.store.create({
      purchaseId,
      txHash,
      buyerWallet,
      ambassadorSlug: slug,
      ambassadorWallet,
      purchaseAmountSun: "0",
      ownerShareSun: "0",
      source: "frontend-attribution",
      status: "received",
      failureReason: null,
      now
    });

    return {
      status: "ready-for-allocation",
      purchase,
      slug,
      slugHash,
      ambassadorWallet,
      reason: null
    };
  }

  async prepareVerifiedPurchase(
    input: VerifiedPurchaseInput
  ): Promise<PrepareVerifiedPurchaseResult> {
    const purchaseId = assertNonEmpty(input.purchaseId, "purchaseId");
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const slug = normalizeIncomingSlug(input.slug);
    const purchaseAmountSun = normalizeSunAmount(input.purchaseAmountSun, "purchaseAmountSun");
    const ownerShareSun = normalizeSunAmount(input.ownerShareSun, "ownerShareSun");
    const now = input.now ?? Date.now();

    const slugHash = this.hashing.hashSlugToBytes32Hex(slug);

    const existing = await this.store.getByPurchaseId(purchaseId);
    if (!existing) {
      throw new Error(`Purchase not found: ${purchaseId}`);
    }

    if (existing.txHash.toLowerCase() !== txHash) {
      throw new Error("Purchase txHash does not match existing record");
    }

    if (existing.buyerWallet !== buyerWallet) {
      throw new Error("Purchase buyerWallet does not match existing record");
    }

    const ambassadorWallet =
      existing.ambassadorWallet || (await getLocalAmbassadorWalletBySlug(slug));

    if (!ambassadorWallet) {
      const purchase = await this.store.markFailed(
        purchaseId,
        "Ambassador wallet not found in local registry",
        now
      );

      return {
        status: "ambassador-not-found",
        purchase,
        slug,
        slugHash,
        ambassadorWallet: null,
        reason: "Ambassador wallet not found in local registry",
        canAllocate: false
      };
    }

    const purchase = await this.store.markVerified(purchaseId, {
      purchaseAmountSun,
      ownerShareSun,
      ambassadorSlug: slug,
      ambassadorWallet,
      now
    });

    return {
      status: "ready-for-allocation",
      purchase,
      slug,
      slugHash,
      ambassadorWallet,
      reason: null,
      canAllocate: true
    };
  }

  async markAllocationSuccess(
    purchaseId: string,
    ambassadorWallet?: string | null,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.store.markAllocated(purchaseId, {
      ambassadorWallet: ambassadorWallet ?? undefined,
      now
    });
  }

  async markAllocationFailure(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    return this.store.markFailed(
      assertNonEmpty(purchaseId, "purchaseId"),
      assertNonEmpty(reason, "reason"),
      now
    );
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/index.ts

```ts
import { AllocationService, type AllocationDecision } from "./domain/allocation";
import {
  createPurchaseStore,
  type AllocationMode,
  type PurchaseRecord,
  type PurchaseStore
} from "./db/purchases";
import {
  TronControllerAllocationExecutor,
  type TronControllerAllocationExecutorConfig
} from "./tron/controller";
import {
  createGasStationClientFromEnv,
  type GasStationClient
} from "./services/gasStation";

export interface CreateAllocationWorkerOptions {
  tronWeb: any;
  controllerContractAddress?: string;
  logger?: WorkerLogger;
}

export interface WorkerLogger {
  info?(payload: Record<string, unknown>): void;
  warn?(payload: Record<string, unknown>): void;
  error?(payload: Record<string, unknown>): void;
}

export interface FrontendAttributionInput {
  txHash: string;
  buyerWallet: string;
  slug: string;
  now: number;
  allocationMode?: AllocationMode;
  feeLimitSun?: number;
}

export interface FrontendAttributionResult {
  stage: "received-purchase" | "verified-purchase";
  purchaseId: string;
  attribution: {
    status:
      | "created"
      | "duplicate-local-record"
      | "slug-not-found"
      | "slug-inactive"
      | "wallet-missing";
    purchase: PurchaseRecord | null;
    slug: string;
    slugHash: string | null;
    ambassadorWallet: string | null;
    reason: string | null;
  };
  verification: {
    status:
      | "waiting-scan"
      | "ready-for-allocation"
      | "already-finalized"
      | "ignored";
    purchase: PurchaseRecord;
    slug: string;
    slugHash: string | null;
    ambassadorWallet: string | null;
    reason: string | null;
    canAllocate: boolean;
  };
  allocation?: {
    status: "allocated" | "deferred" | "failed" | "skipped";
    purchase: PurchaseRecord;
    ambassadorWallet: string | null;
    txid: string | null;
    reason: string | null;
  };
}

export interface ReplayFailedAllocationApiResult {
  status: "allocated" | "failed" | "skipped";
  purchase: PurchaseRecord;
  ambassadorWallet: string | null;
  txid: string | null;
  reason: string | null;
}

export interface ProcessChainEventInput {
  txHash: string;
  buyerWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  blockTimestamp: number;
  allocationMode?: AllocationMode;
  feeLimitSun?: number;
}

export interface ProcessChainEventResult {
  stage: "verified-purchase";
  purchaseId: string | null;
  attribution: {
    status:
      | "matched-local-record"
      | "duplicate-local-record"
      | "no-local-record"
      | "wallet-mismatch";
    purchase: PurchaseRecord | null;
    slug: string | null;
    slugHash: string | null;
    ambassadorWallet: string | null;
    reason: string | null;
  };
  verification: {
    status:
      | "ready-for-allocation"
      | "already-finalized"
      | "ignored"
      | "no-attribution";
    purchase: PurchaseRecord | null;
    slug: string | null;
    slugHash: string | null;
    ambassadorWallet: string | null;
    reason: string | null;
    canAllocate: boolean;
  };
  allocation?: {
    status: "allocated" | "deferred" | "failed" | "skipped";
    purchase: PurchaseRecord;
    ambassadorWallet: string | null;
    txid: string | null;
    reason: string | null;
  };
}

export interface ProcessVerifiedPurchaseAndAllocateInput {
  txHash: string;
  buyerWallet: string;
  slug?: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  feeLimitSun?: number;
  now?: number;
  allocationMode?: AllocationMode;
}

export interface ProcessVerifiedPurchaseAndAllocateResult {
  stage: "verified-purchase";
  purchaseId: string | null;
  attribution: ProcessChainEventResult["attribution"] | null;
  verification: ProcessChainEventResult["verification"];
  allocation: AllocationDecision | null;
}

export interface AllocationWorkerProcessor {
  attributionService: any;
  allocationService: AllocationService;

  processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<FrontendAttributionResult>;

  processVerifiedChainEvent(
    input: ProcessChainEventInput
  ): Promise<ProcessChainEventResult>;

  processVerifiedPurchaseAndAllocate(
    input: ProcessVerifiedPurchaseAndAllocateInput
  ): Promise<ProcessVerifiedPurchaseAndAllocateResult>;

  replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<ReplayFailedAllocationApiResult>;

  prepareWithdrawBatch(
    input: {
      ambassadorWallet: string;
      limit?: number;
    }
  ): Promise<{
    ambassadorWallet: string;
    purchases: PurchaseRecord[];
  }>;

  allocatePendingBatch(
    input: {
      ambassadorWallet: string;
      feeLimitSun?: number;
      limit?: number;
      allocationMode?: AllocationMode;
      stopOnFirstDeferred?: boolean;
    }
  ): Promise<{
    ambassadorWallet: string;
    processed: Awaited<ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>>[];
    stoppedEarly: boolean;
    stopReason: string | null;
  }>;
}

export interface AllocationWorker {
  store: PurchaseStore;
  allocation: AllocationService;
  processor: AllocationWorkerProcessor;
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
}

function assertNonEmpty(value: unknown, fieldName: string): string {
  const normalized = normalizeOptionalString(value);

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeTxHash(value: unknown): string {
  return assertNonEmpty(value, "txHash").toLowerCase();
}

function normalizeWallet(value: unknown, fieldName: string): string {
  return assertNonEmpty(value, fieldName);
}

function parseAmountAsString(value: unknown, fieldName: string): string {
  const raw = assertNonEmpty(value, fieldName);

  if (!/^\d+$/.test(raw)) {
    throw new Error(`${fieldName} must be a numeric string`);
  }

  return raw;
}

function isFinalPurchaseStatus(status: PurchaseRecord["status"]): boolean {
  return (
    status === "allocated" ||
    status === "withdraw_included" ||
    status === "withdraw_completed" ||
    status === "ignored" ||
    status === "allocation_failed_final"
  );
}

function parseBooleanEnv(value: string | undefined, fallback = false): boolean {
  if (value == null || String(value).trim() === "") {
    return fallback;
  }

  const normalized = String(value).trim().toLowerCase();

  return (
    normalized === "1" ||
    normalized === "true" ||
    normalized === "yes" ||
    normalized === "on"
  );
}

function parseNonNegativeIntegerEnv(
  value: string | undefined,
  fallback: number
): number {
  if (value == null || String(value).trim() === "") {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`Invalid non-negative integer env value: ${value}`);
  }

  return Math.floor(parsed);
}

function parseStringEnv(value: string | undefined, fallback: string): string {
  const normalized = String(value || "").trim();
  return normalized || fallback;
}

function mapAllocationAttemptToApiResult(
  result: Awaited<ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>>
): {
  status: "allocated" | "deferred" | "failed" | "skipped";
  purchase: PurchaseRecord;
  ambassadorWallet: string | null;
  txid: string | null;
  reason: string | null;
} {
  if (result.status === "allocated") {
    return {
      status: "allocated",
      purchase: result.purchase,
      ambassadorWallet: result.purchase.ambassadorWallet,
      txid: result.txid,
      reason: null
    };
  }

  if (
    result.status === "deferred" ||
    result.status === "stopped-on-resource-shortage"
  ) {
    return {
      status: "deferred",
      purchase: result.purchase,
      ambassadorWallet: result.purchase.ambassadorWallet,
      txid: null,
      reason: result.reason
    };
  }

  if (
    result.status === "skipped-already-final" ||
    result.status === "skipped-no-ambassador-wallet"
  ) {
    return {
      status: "skipped",
      purchase: result.purchase,
      ambassadorWallet: result.purchase.ambassadorWallet,
      txid: null,
      reason: result.reason
    };
  }

  return {
    status: "failed",
    purchase: result.purchase,
    ambassadorWallet: result.purchase.ambassadorWallet,
    txid: null,
    reason: result.reason
  };
}

function mapApiAllocationToDecision(
  purchase: PurchaseRecord,
  allocation:
    | {
        status: "allocated" | "deferred" | "failed" | "skipped";
        purchase: PurchaseRecord;
        ambassadorWallet: string | null;
        txid: string | null;
        reason: string | null;
      }
    | undefined
): AllocationDecision | null {
  if (!allocation) {
    return null;
  }

  if (allocation.status === "allocated") {
    return {
      status: "allocated",
      purchase,
      txid: allocation.txid,
      reason: null,
      errorCode: null,
      errorMessage: null
    };
  }

  if (allocation.status === "deferred") {
    return {
      status: "deferred",
      purchase,
      txid: null,
      reason: allocation.reason,
      errorCode: null,
      errorMessage: allocation.reason
    };
  }

  if (allocation.status === "skipped") {
    return {
      status: "skipped-no-ambassador-wallet",
      purchase,
      txid: null,
      reason: allocation.reason,
      errorCode: null,
      errorMessage: allocation.reason
    };
  }

  return {
    status: "retryable-failed",
    purchase,
    txid: null,
    reason: allocation.reason,
    errorCode: null,
    errorMessage: allocation.reason
  };
}

class AllocationWorkerProcessorImpl implements AllocationWorkerProcessor {
  public readonly attributionService: any = null;
  public readonly allocationService: AllocationService;

  private readonly store: PurchaseStore;
  private readonly allocation: AllocationService;
  private readonly logger?: WorkerLogger;

  constructor(options: {
    store: PurchaseStore;
    allocation: AllocationService;
    logger?: WorkerLogger;
  }) {
    this.store = options.store;
    this.allocation = options.allocation;
    this.allocationService = options.allocation;
    this.logger = options.logger;
  }

  async processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<FrontendAttributionResult> {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const slug = assertNonEmpty(input.slug, "slug");
    const now = input.now;

    const ambassador = await this.store.getAmbassadorBySlug(slug);

    const receivedPurchase = await this.store.createOrGetReceivedPurchase({
      txHash,
      buyerWallet,
      ambassadorSlug: slug,
      now
    });

    let attributionStatus:
      | "created"
      | "duplicate-local-record"
      | "slug-not-found"
      | "slug-inactive"
      | "wallet-missing" = receivedPurchase.created ? "created" : "duplicate-local-record";

    let attributionReason: string | null = receivedPurchase.created
      ? null
      : "Purchase already exists in local store";

    let slugHash: string | null = null;
    let ambassadorWallet: string | null = null;

    if (!ambassador) {
      attributionStatus = "slug-not-found";
      attributionReason = "Ambassador slug not found";
    } else if (ambassador.status !== "active") {
      attributionStatus = "slug-inactive";
      attributionReason = `Ambassador status is not active: ${ambassador.status}`;
      slugHash = ambassador.slugHash;
      ambassadorWallet = ambassador.wallet ?? null;
    } else if (!ambassador.wallet) {
      attributionStatus = "wallet-missing";
      attributionReason = "Ambassador wallet is missing";
      slugHash = ambassador.slugHash;
    } else {
      slugHash = ambassador.slugHash;
      ambassadorWallet = ambassador.wallet;

      receivedPurchase.purchase = await this.store.attachAmbassadorToPurchase({
        purchaseId: receivedPurchase.purchase.purchaseId,
        ambassadorSlug: slug,
        ambassadorWallet: ambassador.wallet,
        purchaseAmountSun: "0",
        ownerShareSun: "0",
        now
      });
    }

    const currentPurchase = await this.store.getByPurchaseId(
      receivedPurchase.purchase.purchaseId
    );

    if (!currentPurchase) {
      throw new Error("Failed to reload purchase after frontend attribution");
    }

    this.logger?.info?.({
      scope: "frontend-attribution",
      stage: "received",
      txHash,
      purchaseId: currentPurchase.purchaseId,
      slug,
      ambassadorWallet,
      attributionStatus
    });

    return {
      stage: "received-purchase",
      purchaseId: currentPurchase.purchaseId,
      attribution: {
        status: attributionStatus,
        purchase: currentPurchase,
        slug,
        slugHash,
        ambassadorWallet,
        reason: attributionReason
      },
      verification: {
        status: "waiting-scan",
        purchase: currentPurchase,
        slug,
        slugHash,
        ambassadorWallet,
        reason:
          !ambassador || ambassador.status !== "active" || !ambassador.wallet
            ? attributionReason
            : null,
        canAllocate: false
      }
    };
  }

  async processVerifiedChainEvent(
    input: ProcessChainEventInput
  ): Promise<ProcessChainEventResult> {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const purchaseAmountSun = parseAmountAsString(
      input.purchaseAmountSun,
      "purchaseAmountSun"
    );
    const ownerShareSun = parseAmountAsString(input.ownerShareSun, "ownerShareSun");
    const blockTimestamp = Number(input.blockTimestamp);
    const allocationMode = input.allocationMode ?? "eager";

    if (!Number.isFinite(blockTimestamp) || blockTimestamp <= 0) {
      throw new Error("blockTimestamp must be a positive number");
    }

    const purchase = await this.store.getByTxHash(txHash);

    if (!purchase) {
      return {
        stage: "verified-purchase",
        purchaseId: null,
        attribution: {
          status: "no-local-record",
          purchase: null,
          slug: null,
          slugHash: null,
          ambassadorWallet: null,
          reason: "No local attribution record found for txHash"
        },
        verification: {
          status: "no-attribution",
          purchase: null,
          slug: null,
          slugHash: null,
          ambassadorWallet: null,
          reason: "No local attribution record found for txHash",
          canAllocate: false
        }
      };
    }

    if (
      purchase.buyerWallet &&
      purchase.buyerWallet.toLowerCase() !== buyerWallet.toLowerCase()
    ) {
      return {
        stage: "verified-purchase",
        purchaseId: purchase.purchaseId,
        attribution: {
          status: "wallet-mismatch",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Buyer wallet mismatch for txHash"
        },
        verification: {
          status: "ignored",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Buyer wallet mismatch for txHash",
          canAllocate: false
        }
      };
    }

    if (isFinalPurchaseStatus(purchase.status)) {
      const ambassador =
        purchase.ambassadorSlug
          ? await this.store.getAmbassadorBySlug(purchase.ambassadorSlug)
          : null;

      return {
        stage: "verified-purchase",
        purchaseId: purchase.purchaseId,
        attribution: {
          status: "duplicate-local-record",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: ambassador?.slugHash ?? null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: "Purchase already finalized"
        },
        verification: {
          status: "already-finalized",
          purchase,
          slug: purchase.ambassadorSlug,
          slugHash: ambassador?.slugHash ?? null,
          ambassadorWallet: purchase.ambassadorWallet,
          reason: `Purchase already finalized with status: ${purchase.status}`,
          canAllocate: false
        }
      };
    }

    const verifiedPurchase = await this.store.markVerifiedPurchase({
      purchaseId: purchase.purchaseId,
      txHash,
      buyerWallet,
      purchaseAmountSun,
      ownerShareSun,
      now: blockTimestamp
    });

    const ambassador =
      verifiedPurchase.ambassadorSlug
        ? await this.store.getAmbassadorBySlug(verifiedPurchase.ambassadorSlug)
        : null;

    const slugHash = ambassador?.slugHash ?? null;

    if (!verifiedPurchase.ambassadorWallet) {
      return {
        stage: "verified-purchase",
        purchaseId: verifiedPurchase.purchaseId,
        attribution: {
          status:
            purchase.status === "received"
              ? "matched-local-record"
              : "duplicate-local-record",
          purchase: verifiedPurchase,
          slug: verifiedPurchase.ambassadorSlug,
          slugHash,
          ambassadorWallet: verifiedPurchase.ambassadorWallet,
          reason: null
        },
        verification: {
          status: "ignored",
          purchase: verifiedPurchase,
          slug: verifiedPurchase.ambassadorSlug,
          slugHash,
          ambassadorWallet: verifiedPurchase.ambassadorWallet,
          reason: "Ambassador wallet is missing for verified purchase",
          canAllocate: false
        }
      };
    }

    const allocationResult = await this.allocation.tryAllocateVerifiedPurchase(
      verifiedPurchase.purchaseId,
      {
        feeLimitSun: input.feeLimitSun,
        allocationMode
      }
    );

    return {
      stage: "verified-purchase",
      purchaseId: verifiedPurchase.purchaseId,
      attribution: {
        status:
          purchase.status === "received"
            ? "matched-local-record"
            : "duplicate-local-record",
        purchase: allocationResult.purchase,
        slug: allocationResult.purchase.ambassadorSlug,
        slugHash,
        ambassadorWallet: allocationResult.purchase.ambassadorWallet,
        reason:
          purchase.status === "received"
            ? null
            : "Purchase already exists in local store"
      },
      verification: {
        status: "ready-for-allocation",
        purchase: allocationResult.purchase,
        slug: allocationResult.purchase.ambassadorSlug,
        slugHash,
        ambassadorWallet: allocationResult.purchase.ambassadorWallet,
        reason: null,
        canAllocate: true
      },
      allocation: mapAllocationAttemptToApiResult(allocationResult)
    };
  }

  async processVerifiedPurchaseAndAllocate(
    input: ProcessVerifiedPurchaseAndAllocateInput
  ): Promise<ProcessVerifiedPurchaseAndAllocateResult> {
    const result = await this.processVerifiedChainEvent({
      txHash: input.txHash,
      buyerWallet: input.buyerWallet,
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      blockTimestamp: input.now ?? Date.now(),
      allocationMode: input.allocationMode ?? "eager",
      feeLimitSun: input.feeLimitSun
    });

    let allocationDecision: AllocationDecision | null = null;

    if (result.purchaseId) {
      const purchase = await this.store.getByPurchaseId(result.purchaseId);

      if (purchase) {
        allocationDecision = mapApiAllocationToDecision(purchase, result.allocation);
      }
    }

    return {
      stage: "verified-purchase",
      purchaseId: result.purchaseId,
      attribution: result.attribution,
      verification: result.verification,
      allocation: allocationDecision
    };
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<ReplayFailedAllocationApiResult> {
    return this.allocation.replayFailedAllocation(purchaseId, feeLimitSun, now);
  }

  async prepareWithdrawBatch(
    input: {
      ambassadorWallet: string;
      limit?: number;
    }
  ): Promise<{
    ambassadorWallet: string;
    purchases: PurchaseRecord[];
  }> {
    return this.allocation.prepareWithdrawBatch(input);
  }

  async allocatePendingBatch(
    input: {
      ambassadorWallet: string;
      feeLimitSun?: number;
      limit?: number;
      allocationMode?: AllocationMode;
      stopOnFirstDeferred?: boolean;
    }
  ): Promise<{
    ambassadorWallet: string;
    processed: Awaited<ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>>[];
    stoppedEarly: boolean;
    stopReason: string | null;
  }> {
    const result = await this.allocation.allocatePendingBatch({
      ambassadorWallet: input.ambassadorWallet,
      feeLimitSun: input.feeLimitSun,
      limit: input.limit,
      allocationMode: input.allocationMode,
      stopOnFirstDeferred: input.stopOnFirstDeferred
    });

    return {
      ambassadorWallet: result.ambassadorWallet,
      processed: result.processed as Awaited<
        ReturnType<AllocationService["tryAllocateVerifiedPurchase"]>
      >[],
      stoppedEarly: result.stoppedEarly,
      stopReason: result.stopReason
    };
  }
}

export function createAllocationWorker(
  options: CreateAllocationWorkerOptions
): AllocationWorker {
  const store = createPurchaseStore();

  const gasStationEnabled = parseBooleanEnv(process.env.GASSTATION_ENABLED, false);
  const gasStationMinBandwidth = parseNonNegativeIntegerEnv(
    process.env.GASSTATION_MIN_BANDWIDTH,
    0
  );
  const gasStationMinEnergy = parseNonNegativeIntegerEnv(
    process.env.GASSTATION_MIN_ENERGY,
    0
  );
  const allocationMinBandwidth = parseNonNegativeIntegerEnv(
    process.env.ALLOCATION_MIN_BANDWIDTH,
    0
  );
  const allocationMinEnergy = parseNonNegativeIntegerEnv(
    process.env.ALLOCATION_MIN_ENERGY,
    0
  );
  const gasStationServiceChargeType = parseStringEnv(
    process.env.GASSTATION_SERVICE_CHARGE_TYPE,
    "10010"
  );

  let gasStationClient: GasStationClient | null = null;

  if (gasStationEnabled) {
    gasStationClient = createGasStationClientFromEnv();
  }

  options.logger?.info?.({
    scope: "gasstation",
    stage: "configured",
    enabled: gasStationEnabled,
    gasStationMinBandwidth,
    gasStationMinEnergy,
    allocationMinBandwidth,
    allocationMinEnergy,
    gasStationServiceChargeType
  });

  const executorConfig: TronControllerAllocationExecutorConfig = {
    tronWeb: options.tronWeb,
    controllerContractAddress: options.controllerContractAddress,
    gasStationClient,
    gasStationEnabled,
    gasStationMinBandwidth,
    gasStationMinEnergy,
    allocationMinBandwidth,
    allocationMinEnergy,
    gasStationServiceChargeType
  };

  const executor = new TronControllerAllocationExecutor(executorConfig);

  const allocation = new AllocationService({
    store,
    executor,
    logger: options.logger
  });

  const processor = new AllocationWorkerProcessorImpl({
    store,
    allocation,
    logger: options.logger
  });

  return {
    store,
    allocation,
    processor
  };
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/jobs/allocatePurchase.ts

```ts
import {
  AttributionProcessor,
  ProcessVerifiedPurchaseAndAllocateInput,
  ProcessVerifiedPurchaseAndAllocateResult
} from "../app/processAttribution";

export interface AllocatePurchaseJobConfig {
  processor: AttributionProcessor;
}

export interface AllocatePurchaseJobInput {
  txHash: string;
  buyerWallet: string;
  slug: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  feeLimitSun?: number;
  now?: number;
}

export interface AllocatePurchaseJobResult {
  ok: boolean;
  result: ProcessVerifiedPurchaseAndAllocateResult;
}

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

export class AllocatePurchaseJob {
  private readonly processor: AttributionProcessor;

  constructor(config: AllocatePurchaseJobConfig) {
    if (!config?.processor) {
      throw new Error("processor is required");
    }

    this.processor = config.processor;
  }

  async run(input: AllocatePurchaseJobInput): Promise<AllocatePurchaseJobResult> {
    const payload: ProcessVerifiedPurchaseAndAllocateInput = {
      txHash: assertNonEmpty(input.txHash, "txHash"),
      buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
      slug: assertNonEmpty(input.slug, "slug"),
      purchaseAmountSun: normalizeSunAmount(input.purchaseAmountSun, "purchaseAmountSun"),
      ownerShareSun: normalizeSunAmount(input.ownerShareSun, "ownerShareSun"),
      feeLimitSun: input.feeLimitSun,
      now: input.now
    };

    const result = await this.processor.processVerifiedPurchaseAndAllocate(payload);

    const ok =
      result.verification.canAllocate &&
      result.allocation !== null &&
      result.allocation.status === "allocated";

    return {
      ok,
      result
    };
  }

  async replayFailed(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ) {
    return this.processor.replayFailedAllocation(
      assertNonEmpty(purchaseId, "purchaseId"),
      feeLimitSun,
      now
    );
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/jobs/dailyMaintenance.ts

```ts
import type { AllocationWorker } from "../index";
import {
  processAmbassadorPendingQueue,
  type ProcessAmbassadorPendingQueueJobResult
} from "./processAmbassadorPendingQueue";
import {
  replayDeferredPurchases,
  type ReplayDeferredPurchasesJobResult
} from "./replayDeferredPurchases";

export interface DailyMaintenanceJobOptions {
  now?: number;
  replayLimit?: number;
  queueLimit?: number;
  feeLimitSun?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface DailyMaintenanceJobResult {
  ok: boolean;
  startedAt: number;
  finishedAt: number;
  replayDeferredPurchases: ReplayDeferredPurchasesJobResult | null;
  processAmbassadorPendingQueue: ProcessAmbassadorPendingQueueJobResult | null;
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

function toOptionalPositiveInteger(value: unknown): number | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return undefined;
  }

  return parsed;
}

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

export async function dailyMaintenance(
  worker: AllocationWorker,
  options: DailyMaintenanceJobOptions = {}
): Promise<DailyMaintenanceJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const replayLimit = toPositiveInteger(options.replayLimit, 200);
  const queueLimit = toPositiveInteger(options.queueLimit, 200);
  const feeLimitSun = toOptionalPositiveInteger(options.feeLimitSun);

  const result: DailyMaintenanceJobResult = {
    ok: true,
    startedAt,
    finishedAt: startedAt,
    replayDeferredPurchases: null,
    processAmbassadorPendingQueue: null
  };

  logger.info?.(
    JSON.stringify({
      ok: true,
      job: "dailyMaintenance",
      stage: "started",
      startedAt,
      now,
      replayLimit,
      queueLimit,
      feeLimitSun: feeLimitSun ?? null
    })
  );

  try {
    result.replayDeferredPurchases = await replayDeferredPurchases(worker, {
      now,
      limit: replayLimit,
      feeLimitSun,
      logger
    });

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "dailyMaintenance",
        stage: "replayDeferredPurchases-finished",
        replay: {
          scanned: result.replayDeferredPurchases.scanned,
          allocated: result.replayDeferredPurchases.allocated,
          deferred: result.replayDeferredPurchases.deferred,
          skipped: result.replayDeferredPurchases.skipped,
          failed: result.replayDeferredPurchases.failed
        }
      })
    );

    result.processAmbassadorPendingQueue = await processAmbassadorPendingQueue(worker, {
      now,
      limit: queueLimit,
      feeLimitSun,
      logger
    });

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "dailyMaintenance",
        stage: "processAmbassadorPendingQueue-finished",
        queue: {
          scanned: result.processAmbassadorPendingQueue.scanned,
          allocated: result.processAmbassadorPendingQueue.allocated,
          deferred: result.processAmbassadorPendingQueue.deferred,
          skipped: result.processAmbassadorPendingQueue.skipped,
          failed: result.processAmbassadorPendingQueue.failed
        }
      })
    );

    result.finishedAt = Date.now();

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "dailyMaintenance",
        stage: "finished",
        startedAt: result.startedAt,
        finishedAt: result.finishedAt,
        durationMs: result.finishedAt - result.startedAt,
        replayDeferredPurchases: result.replayDeferredPurchases
          ? {
              scanned: result.replayDeferredPurchases.scanned,
              allocated: result.replayDeferredPurchases.allocated,
              deferred: result.replayDeferredPurchases.deferred,
              skipped: result.replayDeferredPurchases.skipped,
              failed: result.replayDeferredPurchases.failed
            }
          : null,
        processAmbassadorPendingQueue: result.processAmbassadorPendingQueue
          ? {
              scanned: result.processAmbassadorPendingQueue.scanned,
              allocated: result.processAmbassadorPendingQueue.allocated,
              deferred: result.processAmbassadorPendingQueue.deferred,
              skipped: result.processAmbassadorPendingQueue.skipped,
              failed: result.processAmbassadorPendingQueue.failed
            }
          : null
      })
    );

    return result;
  } catch (error) {
    result.ok = false;
    result.finishedAt = Date.now();

    logger.error?.(
      JSON.stringify({
        ok: false,
        job: "dailyMaintenance",
        stage: "failed",
        error: toErrorMessage(error),
        startedAt: result.startedAt,
        finishedAt: result.finishedAt,
        durationMs: result.finishedAt - result.startedAt
      })
    );

    return result;
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/jobs/finalizeAmbassadorWithdrawal.ts

```ts
import type { AllocationWorker } from "../index";

export interface FinalizeAmbassadorWithdrawalJobOptions {
  ambassadorSlug?: string;
  ambassadorWallet?: string;
  withdrawSessionId?: string;
  txid?: string;
  limit?: number;
  now?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface FinalizeAmbassadorWithdrawalJobItem {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug: string;
  ambassadorWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  previousStatus: string;
  status: string;
  withdrawSessionId: string | null;
  finalized: boolean;
  reason: string | null;
}

export interface FinalizeAmbassadorWithdrawalJobResult {
  ok: boolean;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  withdrawSessionId: string | null;
  txid: string | null;
  scanned: number;
  finalized: number;
  skipped: number;
  items: FinalizeAmbassadorWithdrawalJobItem[];
  startedAt: number;
  finishedAt: number;
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
}

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
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

async function resolveAmbassadorWallet(
  worker: AllocationWorker,
  input: {
    ambassadorSlug?: string;
    ambassadorWallet?: string;
  }
): Promise<{
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
}> {
  const ambassadorWallet = normalizeOptionalString(input.ambassadorWallet) ?? null;
  const ambassadorSlug = normalizeOptionalString(input.ambassadorSlug) ?? null;

  if (ambassadorWallet) {
    return {
      ambassadorSlug,
      ambassadorWallet
    };
  }

  if (!ambassadorSlug) {
    throw new Error("ambassadorWallet or ambassadorSlug is required");
  }

  const record = await worker.store.getAmbassadorBySlug(ambassadorSlug);

  if (!record?.wallet) {
    throw new Error(`Ambassador wallet not found for slug: ${ambassadorSlug}`);
  }

  return {
    ambassadorSlug,
    ambassadorWallet: record.wallet
  };
}

async function loadWithdrawIncludedPurchases(
  worker: AllocationWorker,
  options: {
    ambassadorWallet: string;
    withdrawSessionId?: string;
    limit: number;
  }
): Promise<any[]> {
  const rows = await worker.store.listPendingByAmbassador({
    ambassadorWallet: options.ambassadorWallet,
    statuses: ["withdraw_included"],
    limit: options.limit
  });

  if (!options.withdrawSessionId) {
    return rows;
  }

  return rows.filter((purchase) => {
    return String(purchase.withdrawSessionId || "").trim() === options.withdrawSessionId;
  });
}

export async function finalizeAmbassadorWithdrawal(
  worker: AllocationWorker,
  options: FinalizeAmbassadorWithdrawalJobOptions = {}
): Promise<FinalizeAmbassadorWithdrawalJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 1000);
  const txid = normalizeOptionalString(options.txid) ?? null;
  const requestedWithdrawSessionId = normalizeOptionalString(options.withdrawSessionId) ?? null;

  const resolved = await resolveAmbassadorWallet(worker, {
    ambassadorSlug: options.ambassadorSlug,
    ambassadorWallet: options.ambassadorWallet
  });

  const ambassadorSlug = resolved.ambassadorSlug;
  const ambassadorWallet = resolved.ambassadorWallet;

  const result: FinalizeAmbassadorWithdrawalJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    withdrawSessionId: requestedWithdrawSessionId,
    txid,
    scanned: 0,
    finalized: 0,
    skipped: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    if (!ambassadorWallet) {
      throw new Error("Ambassador wallet is required");
    }

    const purchases = await loadWithdrawIncludedPurchases(worker, {
      ambassadorWallet,
      withdrawSessionId: requestedWithdrawSessionId ?? undefined,
      limit
    });

    result.scanned = purchases.length;

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "finalizeAmbassadorWithdrawal",
        message: "Loaded purchases for withdrawal finalization",
        ambassadorSlug,
        ambassadorWallet,
        withdrawSessionId: requestedWithdrawSessionId,
        txid,
        scanned: result.scanned,
        limit
      })
    );

    if (!purchases.length) {
      result.finishedAt = Date.now();

      logger.info?.(
        JSON.stringify({
          ok: true,
          job: "finalizeAmbassadorWithdrawal",
          message: "No withdraw_included purchases found for finalization",
          ambassadorSlug,
          ambassadorWallet,
          withdrawSessionId: requestedWithdrawSessionId,
          txid,
          scanned: 0,
          durationMs: result.finishedAt - result.startedAt
        })
      );

      return result;
    }

    for (const purchase of purchases) {
      const previousStatus = String(purchase.status || "").trim();
      const currentWithdrawSessionId = String(purchase.withdrawSessionId || "").trim();

      if (previousStatus !== "withdraw_included") {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          previousStatus,
          status: previousStatus || "unknown",
          withdrawSessionId: currentWithdrawSessionId || null,
          finalized: false,
          reason: `Unsupported status for withdrawal finalization: ${previousStatus || "unknown"}`
        });
        continue;
      }

      if (
        requestedWithdrawSessionId &&
        currentWithdrawSessionId !== requestedWithdrawSessionId
      ) {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          previousStatus,
          status: previousStatus,
          withdrawSessionId: currentWithdrawSessionId || null,
          finalized: false,
          reason: "Withdraw session mismatch"
        });
        continue;
      }

      const updated = await worker.store.markWithdrawCompleted(String(purchase.purchaseId), {
        withdrawSessionId: currentWithdrawSessionId || null,
        now
      });

      result.finalized += 1;

      result.items.push({
        purchaseId: String(updated?.purchaseId || purchase.purchaseId || ""),
        txHash: String(updated?.txHash || purchase.txHash || ""),
        buyerWallet: String(updated?.buyerWallet || purchase.buyerWallet || ""),
        ambassadorSlug: String(updated?.ambassadorSlug || purchase.ambassadorSlug || ""),
        ambassadorWallet: String(updated?.ambassadorWallet || purchase.ambassadorWallet || ""),
        purchaseAmountSun: String(updated?.purchaseAmountSun ?? purchase.purchaseAmountSun ?? "0"),
        ownerShareSun: String(updated?.ownerShareSun ?? purchase.ownerShareSun ?? "0"),
        previousStatus,
        status: String(updated?.status || "withdraw_completed"),
        withdrawSessionId: String(updated?.withdrawSessionId || currentWithdrawSessionId || "") || null,
        finalized: true,
        reason: null
      });
    }

    result.finishedAt = Date.now();

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "finalizeAmbassadorWithdrawal",
        message: "Ambassador withdrawal finalization finished",
        ambassadorSlug,
        ambassadorWallet,
        withdrawSessionId: requestedWithdrawSessionId,
        txid,
        scanned: result.scanned,
        finalized: result.finalized,
        skipped: result.skipped,
        durationMs: result.finishedAt - result.startedAt
      })
    );

    return result;
  } catch (error) {
    result.ok = false;
    result.finishedAt = Date.now();

    logger.error?.(
      JSON.stringify({
        ok: false,
        job: "finalizeAmbassadorWithdrawal",
        ambassadorSlug,
        ambassadorWallet,
        withdrawSessionId: requestedWithdrawSessionId,
        txid,
        error: toErrorMessage(error)
      })
    );

    throw error;
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/jobs/prepareAmbassadorWithdrawal.ts

```ts
import crypto from "node:crypto";
import type { AllocationWorker } from "../index";

export interface PrepareAmbassadorWithdrawalJobOptions {
  ambassadorSlug?: string;
  ambassadorWallet?: string;
  limit?: number;
  now?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface PrepareAmbassadorWithdrawalJobItem {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug: string;
  ambassadorWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  status: string;
  queuedForWithdrawal: boolean;
  withdrawSessionId: string | null;
  reason: string | null;
}

export interface PrepareAmbassadorWithdrawalJobResult {
  ok: boolean;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  withdrawSessionId: string | null;
  scanned: number;
  prepared: number;
  skipped: number;
  items: PrepareAmbassadorWithdrawalJobItem[];
  startedAt: number;
  finishedAt: number;
}

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
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

function buildWithdrawSessionId(input: {
  ambassadorWallet: string;
  now: number;
}): string {
  const hash = crypto
    .createHash("sha256")
    .update(`withdraw:${input.ambassadorWallet}:${input.now}:${Math.random()}`)
    .digest("hex");

  return hash.slice(0, 32);
}

async function resolveAmbassadorWallet(
  worker: AllocationWorker,
  input: {
    ambassadorSlug?: string;
    ambassadorWallet?: string;
  }
): Promise<{
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
}> {
  const ambassadorWallet = normalizeOptionalString(input.ambassadorWallet) ?? null;
  const ambassadorSlug = normalizeOptionalString(input.ambassadorSlug) ?? null;

  if (ambassadorWallet) {
    return {
      ambassadorSlug,
      ambassadorWallet
    };
  }

  if (!ambassadorSlug) {
    throw new Error("ambassadorWallet or ambassadorSlug is required");
  }

  const record = await worker.store.getAmbassadorBySlug(ambassadorSlug);

  if (!record?.wallet) {
    throw new Error(`Ambassador wallet not found for slug: ${ambassadorSlug}`);
  }

  return {
    ambassadorSlug,
    ambassadorWallet: record.wallet
  };
}

async function loadCandidatePurchases(
  worker: AllocationWorker,
  options: {
    ambassadorWallet: string;
    limit: number;
  }
): Promise<any[]> {
  const pending = await worker.store.listPendingByAmbassador({
    ambassadorWallet: options.ambassadorWallet,
    statuses: ["allocated"],
    limit: options.limit
  });

  return pending.filter((purchase) => {
    const reward = BigInt(String(purchase.ownerShareSun || "0"));

    return (
      purchase.status === "allocated" &&
      !purchase.withdrawSessionId &&
      reward > 0n
    );
  });
}

async function markPreparedForWithdrawal(
  worker: AllocationWorker,
  purchaseId: string,
  withdrawSessionId: string,
  now: number
): Promise<any> {
  if (typeof (worker.store as any).markWithdrawIncluded === "function") {
    return (worker.store as any).markWithdrawIncluded(purchaseId, {
      withdrawSessionId,
      now
    });
  }

  await worker.store.assignWithdrawSession(purchaseId, withdrawSessionId, now);
  return worker.store.getByPurchaseId(purchaseId);
}

export async function prepareAmbassadorWithdrawal(
  worker: AllocationWorker,
  options: PrepareAmbassadorWithdrawalJobOptions = {}
): Promise<PrepareAmbassadorWithdrawalJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 500);

  const resolved = await resolveAmbassadorWallet(worker, {
    ambassadorSlug: options.ambassadorSlug,
    ambassadorWallet: options.ambassadorWallet
  });

  const ambassadorSlug = resolved.ambassadorSlug;
  const ambassadorWallet = resolved.ambassadorWallet;

  const result: PrepareAmbassadorWithdrawalJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    withdrawSessionId: null,
    scanned: 0,
    prepared: 0,
    skipped: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    if (!ambassadorWallet) {
      throw new Error("Ambassador wallet is required");
    }

    const purchases = await loadCandidatePurchases(worker, {
      ambassadorWallet,
      limit
    });

    result.scanned = purchases.length;

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "prepareAmbassadorWithdrawal",
        message: "Loaded purchases for ambassador withdrawal preparation",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        limit
      })
    );

    if (!purchases.length) {
      result.finishedAt = Date.now();

      logger.info?.(
        JSON.stringify({
          ok: true,
          job: "prepareAmbassadorWithdrawal",
          message: "No allocated purchases found for withdrawal preparation",
          ambassadorSlug,
          ambassadorWallet,
          scanned: 0,
          durationMs: result.finishedAt - result.startedAt
        })
      );

      return result;
    }

    const withdrawSessionId = buildWithdrawSessionId({
      ambassadorWallet,
      now
    });

    result.withdrawSessionId = withdrawSessionId;

    for (const purchase of purchases) {
      const currentStatus = String(purchase.status || "").trim();
      const rewardAmount = BigInt(String(purchase.ownerShareSun ?? "0"));

      if (currentStatus !== "allocated") {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus || "unknown",
          queuedForWithdrawal: false,
          withdrawSessionId: null,
          reason: `Unsupported status for withdrawal preparation: ${currentStatus || "unknown"}`
        });
        continue;
      }

      if (purchase.withdrawSessionId) {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus,
          queuedForWithdrawal: true,
          withdrawSessionId: String(purchase.withdrawSessionId),
          reason: "Already included in withdrawal preparation"
        });
        continue;
      }

      if (rewardAmount <= 0n) {
        result.skipped += 1;
        result.items.push({
          purchaseId: String(purchase.purchaseId || ""),
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus,
          queuedForWithdrawal: false,
          withdrawSessionId: null,
          reason: "Reward amount is zero"
        });
        continue;
      }

      const updated = await markPreparedForWithdrawal(
        worker,
        String(purchase.purchaseId),
        withdrawSessionId,
        now
      );

      result.prepared += 1;
      result.items.push({
        purchaseId: String(updated?.purchaseId || purchase.purchaseId || ""),
        txHash: String(updated?.txHash || purchase.txHash || ""),
        buyerWallet: String(updated?.buyerWallet || purchase.buyerWallet || ""),
        ambassadorSlug: String(updated?.ambassadorSlug || purchase.ambassadorSlug || ""),
        ambassadorWallet: String(updated?.ambassadorWallet || purchase.ambassadorWallet || ""),
        purchaseAmountSun: String(updated?.purchaseAmountSun ?? purchase.purchaseAmountSun ?? "0"),
        ownerShareSun: String(updated?.ownerShareSun ?? purchase.ownerShareSun ?? "0"),
        status: String(updated?.status || "withdraw_included"),
        queuedForWithdrawal: true,
        withdrawSessionId,
        reason: null
      });
    }

    result.finishedAt = Date.now();

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "prepareAmbassadorWithdrawal",
        message: "Ambassador withdrawal preparation finished",
        ambassadorSlug,
        ambassadorWallet,
        withdrawSessionId,
        scanned: result.scanned,
        prepared: result.prepared,
        skipped: result.skipped,
        durationMs: result.finishedAt - result.startedAt
      })
    );

    return result;
  } catch (error) {
    result.ok = false;
    result.finishedAt = Date.now();

    logger.error?.(
      JSON.stringify({
        ok: false,
        job: "prepareAmbassadorWithdrawal",
        ambassadorSlug,
        ambassadorWallet,
        error: toErrorMessage(error)
      })
    );

    throw error;
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/jobs/processAmbassadorPendingQueue.ts

```ts
import type { AllocationWorker } from "../index";

export interface ProcessAmbassadorPendingQueueJobOptions {
  ambassadorSlug?: string;
  ambassadorWallet?: string;
  limit?: number;
  now?: number;
  feeLimitSun?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface ProcessAmbassadorPendingQueueJobItem {
  purchaseId: string;
  txHash: string;
  buyerWallet: string;
  ambassadorSlug: string;
  ambassadorWallet: string;
  purchaseAmountSun: string;
  ownerShareSun: string;
  status: string;
  action: "allocated" | "deferred" | "skipped" | "failed" | "stopped";
  reason: string | null;
  txid: string | null;
}

export interface ProcessAmbassadorPendingQueueJobResult {
  ok: boolean;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
  scanned: number;
  allocated: number;
  deferred: number;
  skipped: number;
  failed: number;
  stoppedEarly: boolean;
  stopReason: string | null;
  items: ProcessAmbassadorPendingQueueJobItem[];
  startedAt: number;
  finishedAt: number;
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
}

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

function toOptionalPositiveInteger(value: unknown): number | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return undefined;
  }

  return parsed;
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

async function resolveAmbassadorWallet(
  worker: AllocationWorker,
  input: {
    ambassadorSlug?: string;
    ambassadorWallet?: string;
  }
): Promise<{
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
}> {
  const ambassadorWallet = normalizeOptionalString(input.ambassadorWallet) ?? null;
  const ambassadorSlug = normalizeOptionalString(input.ambassadorSlug) ?? null;

  if (ambassadorWallet) {
    return {
      ambassadorSlug,
      ambassadorWallet
    };
  }

  if (!ambassadorSlug) {
    throw new Error("ambassadorWallet or ambassadorSlug is required");
  }

  const record = await worker.store.getAmbassadorBySlug(ambassadorSlug);

  if (!record?.wallet) {
    throw new Error(`Ambassador wallet not found for slug: ${ambassadorSlug}`);
  }

  return {
    ambassadorSlug,
    ambassadorWallet: record.wallet
  };
}

function mapProcessedItem(attempt: any): ProcessAmbassadorPendingQueueJobItem {
  const purchase = attempt?.purchase ?? {};

  let action: "allocated" | "deferred" | "skipped" | "failed" | "stopped" = "failed";
  const status = String(attempt?.status || "").trim();

  if (status === "allocated") {
    action = "allocated";
  } else if (status === "deferred") {
    action = "deferred";
  } else if (status === "stopped-on-resource-shortage") {
    action = "stopped";
  } else if (
    status === "skipped-already-final" ||
    status === "skipped-no-ambassador-wallet"
  ) {
    action = "skipped";
  } else if (status === "retryable-failed" || status === "final-failed") {
    action = "failed";
  }

  return {
    purchaseId: String(purchase.purchaseId || "").trim(),
    txHash: String(purchase.txHash || "").trim(),
    buyerWallet: String(purchase.buyerWallet || "").trim(),
    ambassadorSlug: String(purchase.ambassadorSlug || "").trim(),
    ambassadorWallet: String(purchase.ambassadorWallet || "").trim(),
    purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
    ownerShareSun: String(purchase.ownerShareSun ?? "0"),
    status: String(purchase.status || status || "unknown"),
    action,
    reason: attempt?.reason ? String(attempt.reason) : null,
    txid: attempt?.txid ? String(attempt.txid) : null
  };
}

export async function processAmbassadorPendingQueue(
  worker: AllocationWorker,
  options: ProcessAmbassadorPendingQueueJobOptions = {}
): Promise<ProcessAmbassadorPendingQueueJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 100);
  const feeLimitSun = toOptionalPositiveInteger(options.feeLimitSun);

  const resolved = await resolveAmbassadorWallet(worker, {
    ambassadorSlug: options.ambassadorSlug,
    ambassadorWallet: options.ambassadorWallet
  });

  const ambassadorSlug = resolved.ambassadorSlug;
  const ambassadorWallet = resolved.ambassadorWallet;

  const result: ProcessAmbassadorPendingQueueJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    scanned: 0,
    allocated: 0,
    deferred: 0,
    skipped: 0,
    failed: 0,
    stoppedEarly: false,
    stopReason: null,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    const prepared = await worker.processor.prepareWithdrawBatch({
      ambassadorWallet: ambassadorWallet || "",
      limit
    });

    result.scanned = prepared.purchases.length;

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "processAmbassadorPendingQueue",
        message: "Loaded ambassador pending queue",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        limit,
        now
      })
    );

    if (!prepared.purchases.length) {
      result.finishedAt = Date.now();

      logger.info?.(
        JSON.stringify({
          ok: true,
          job: "processAmbassadorPendingQueue",
          message: "No pending purchases found",
          ambassadorSlug,
          ambassadorWallet,
          scanned: 0,
          durationMs: result.finishedAt - result.startedAt
        })
      );

      return result;
    }

    const allocationResult = await worker.processor.allocatePendingBatch({
      ambassadorWallet: ambassadorWallet || "",
      feeLimitSun,
      limit,
      allocationMode: "claim-first",
      stopOnFirstDeferred: true
    });

    result.stoppedEarly = Boolean(allocationResult.stoppedEarly);
    result.stopReason = allocationResult.stopReason ?? null;
    result.items = allocationResult.processed.map(mapProcessedItem);

    for (const item of result.items) {
      if (item.action === "allocated") {
        result.allocated += 1;
      } else if (item.action === "deferred" || item.action === "stopped") {
        result.deferred += 1;
      } else if (item.action === "skipped") {
        result.skipped += 1;
      } else {
        result.failed += 1;
      }
    }

    result.finishedAt = Date.now();

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "processAmbassadorPendingQueue",
        message: "Ambassador pending queue processing finished",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        allocated: result.allocated,
        deferred: result.deferred,
        skipped: result.skipped,
        failed: result.failed,
        stoppedEarly: result.stoppedEarly,
        stopReason: result.stopReason,
        durationMs: result.finishedAt - result.startedAt
      })
    );

    return result;
  } catch (error) {
    result.ok = false;
    result.finishedAt = Date.now();

    logger.error?.(
      JSON.stringify({
        ok: false,
        job: "processAmbassadorPendingQueue",
        ambassadorSlug,
        ambassadorWallet,
        error: toErrorMessage(error)
      })
    );

    throw error;
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/jobs/rentEnergy.ts

```ts
import { createGasStationClientFromEnv } from "../services/gasStation";

function assertNonEmpty(value: string | undefined, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function getTargetEnergy(): number {
  const raw = Number(process.env.GASSTATION_TARGET_ENERGY || "220000");
  return Number.isFinite(raw) && raw >= 64400 ? Math.ceil(raw) : 220000;
}

function getServiceChargeType(): string {
  return String(process.env.GASSTATION_SERVICE_CHARGE_TYPE || "10010").trim() || "10010";
}

export interface RentEnergyJobResult {
  ok: boolean;
  stage: "checked-balance" | "order-created" | "skipped";
  gasBalance: string | null;
  tradeNo: string | null;
  reason: string | null;
}

export async function rentDailyEnergy(): Promise<RentEnergyJobResult> {
  const client = createGasStationClientFromEnv();

  const receiveAddress = assertNonEmpty(
    process.env.TRON_RESOURCE_ADDRESS || process.env.CONTROLLER_OWNER_WALLET,
    "TRON_RESOURCE_ADDRESS"
  );

  const balance = await client.getBalance();
  const gasBalance = balance.balance;

  const targetEnergy = getTargetEnergy();
  const serviceChargeType = getServiceChargeType();

  if (Number(gasBalance) <= 0) {
    return {
      ok: false,
      stage: "checked-balance",
      gasBalance,
      tradeNo: null,
      reason: "GasStation balance is empty"
    };
  }

  const requestId = `energy-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

  const order = await client.createEnergyOrder({
    requestId,
    receiveAddress,
    energyNum: targetEnergy,
    serviceChargeType
  });

  return {
    ok: true,
    stage: "order-created",
    gasBalance,
    tradeNo: order.trade_no,
    reason: null
  };
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/jobs/replayDeferredPurchases.ts

```ts
import type { AllocationWorker } from "../index";

export interface ReplayDeferredPurchasesJobOptions {
  limit?: number;
  stopOnFirstFailure?: boolean;
  now?: number;
  feeLimitSun?: number;
  logger?: Pick<Console, "info" | "warn" | "error">;
}

export interface ReplayDeferredPurchaseJobItemResult {
  purchaseId: string;
  status: "allocated" | "skipped" | "failed";
  reason: string | null;
  txid: string | null;
}

export interface ReplayDeferredPurchasesJobResult {
  ok: boolean;
  scanned: number;
  attempted: number;
  allocated: number;
  deferred: number;
  skipped: number;
  failed: number;
  items: ReplayDeferredPurchaseJobItemResult[];
  startedAt: number;
  finishedAt: number;
}

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
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

export async function replayDeferredPurchases(
  worker: AllocationWorker,
  options: ReplayDeferredPurchasesJobOptions = {}
): Promise<ReplayDeferredPurchasesJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 100);
  const stopOnFirstFailure = Boolean(options.stopOnFirstFailure);

  const failures = await worker.store.listReplayableFailures(limit);

  const result: ReplayDeferredPurchasesJobResult = {
    ok: true,
    scanned: failures.length,
    attempted: 0,
    allocated: 0,
    deferred: 0,
    skipped: 0,
    failed: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  if (!failures.length) {
    result.finishedAt = Date.now();
    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "replayDeferredPurchases",
        message: "No deferred purchases found",
        scanned: 0
      })
    );
    return result;
  }

  logger.info?.(
    JSON.stringify({
      ok: true,
      job: "replayDeferredPurchases",
      message: "Starting replay of deferred purchases",
      scanned: failures.length,
      limit
    })
  );

  for (const purchase of failures) {
    result.attempted += 1;

    try {
      const replayResult = await worker.processor.replayFailedAllocation(
        purchase.purchaseId,
        options.feeLimitSun,
        now
      );

      if (replayResult.status === "allocated") {
        result.allocated += 1;
        result.items.push({
          purchaseId: purchase.purchaseId,
          status: "allocated",
          reason: replayResult.reason ?? null,
          txid: replayResult.txid ?? null
        });

        logger.info?.(
          JSON.stringify({
            ok: true,
            job: "replayDeferredPurchases",
            purchaseId: purchase.purchaseId,
            status: "allocated",
            txid: replayResult.txid ?? null
          })
        );

        continue;
      }

      if (replayResult.status === "skipped") {
        result.skipped += 1;
        result.items.push({
          purchaseId: purchase.purchaseId,
          status: "skipped",
          reason: replayResult.reason ?? "Replay skipped",
          txid: replayResult.txid ?? null
        });

        logger.warn?.(
          JSON.stringify({
            ok: true,
            job: "replayDeferredPurchases",
            purchaseId: purchase.purchaseId,
            status: "skipped",
            reason: replayResult.reason ?? "Replay skipped"
          })
        );

        continue;
      }

      result.failed += 1;
      result.items.push({
        purchaseId: purchase.purchaseId,
        status: "failed",
        reason: replayResult.reason ?? "Replay failed",
        txid: replayResult.txid ?? null
      });

      logger.warn?.(
        JSON.stringify({
          ok: false,
          job: "replayDeferredPurchases",
          purchaseId: purchase.purchaseId,
          status: "failed",
          reason: replayResult.reason ?? "Replay failed"
        })
      );

      if (stopOnFirstFailure) {
        result.ok = false;
        break;
      }
    } catch (error) {
      const message = toErrorMessage(error);

      result.ok = false;
      result.failed += 1;
      result.items.push({
        purchaseId: purchase.purchaseId,
        status: "failed",
        reason: message,
        txid: null
      });

      logger.error?.(
        JSON.stringify({
          ok: false,
          job: "replayDeferredPurchases",
          purchaseId: purchase.purchaseId,
          status: "failed",
          error: message
        })
      );

      if (stopOnFirstFailure) {
        break;
      }
    }
  }

  result.finishedAt = Date.now();

  logger.info?.(
    JSON.stringify({
      ok: result.ok,
      job: "replayDeferredPurchases",
      stage: "finished",
      scanned: result.scanned,
      attempted: result.attempted,
      allocated: result.allocated,
      deferred: result.deferred,
      skipped: result.skipped,
      failed: result.failed,
      durationMs: result.finishedAt - result.startedAt
    })
  );

  return result;
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/run-scan.ts

```ts
import { FOURTEEN_TOKEN_CONTRACT } from "../../../shared/config/contracts";
import { AttributionProcessor } from "./app/processAttribution";
import {
  PurchaseStore,
  getAllocationRetryReadyAt,
  isPurchaseReadyForAllocationRetry,
  isRateLimitedAllocationFailure
} from "./db/purchases";

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
  | "deferred"
  | "failed"
  | "skipped-no-local-attribution"
  | "skipped-missing-slug"
  | "skipped-already-final"
  | "skipped-retry-cooldown"
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
    const message = String((error as { message: string }).message).trim();

    if (message) {
      return message;
    }
  }

  return "Unknown error";
}

function normalizeTxHashFromEvent(event: any): string {
  const value =
    pickObjectValue(event, ["transaction_id", "transactionId", "txHash", "txid"]) ?? "";

  return assertNonEmpty(String(value), "event.txHash").toLowerCase();
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
    return tronWeb.address.fromHex(`41${raw.slice(2)}`);
  }

  if (/^[0-9a-fA-F]{40}$/.test(raw)) {
    return tronWeb.address.fromHex(`41${raw}`);
  }

  throw new Error(`Unsupported buyer address format: ${raw}`);
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
  const directFingerprint = pickObjectValue(payload, ["fingerprint"]);

  if (typeof directFingerprint === "string" && directFingerprint.trim()) {
    return directFingerprint.trim();
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

function isFinalPurchaseStatus(status: string): boolean {
  return (
    status === "allocated" ||
    status === "withdraw_included" ||
    status === "withdraw_completed" ||
    status === "ignored" ||
    status === "allocation_failed_final"
  );
}

function shouldApplyRetryCooldown(status: string): boolean {
  return status === "deferred" || status === "allocation_failed_retryable";
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
        scope: "scan",
        stage: "getEventResult",
        tokenContractAddress: this.tokenContractAddress,
        eventName: this.eventName,
        pageSize: this.pageSize,
        fingerprint: cursor.fingerprint ?? null,
        rawEventsType: Array.isArray(rawEvents) ? "array" : typeof rawEvents,
        rawEventsLength: Array.isArray(rawEvents)
          ? rawEvents.length
          : Array.isArray(rawEvents?.data)
            ? rawEvents.data.length
            : null
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
          processed.push({
            status: "event-processing-failed",
            event,
            purchaseId: null,
            reason: toErrorMessage(error),
            rawResult: error
          });
        }
      } catch (error) {
        processed.push({
          status: "event-parse-failed",
          event: null,
          purchaseId: null,
          reason: toErrorMessage(error),
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

    if (isFinalPurchaseStatus(localPurchase.status)) {
      return {
        status: "skipped-already-final",
        event,
        purchaseId: localPurchase.purchaseId,
        reason: `Purchase already finalized with status: ${localPurchase.status}`
      };
    }

    const now = Date.now();

    if (
      shouldApplyRetryCooldown(localPurchase.status) &&
      !isPurchaseReadyForAllocationRetry(localPurchase, now)
    ) {
      const retryAt = getAllocationRetryReadyAt(localPurchase);
      const retryInMs = Math.max(0, retryAt - now);

      return {
        status: "skipped-retry-cooldown",
        event,
        purchaseId: localPurchase.purchaseId,
        reason: isRateLimitedAllocationFailure(localPurchase)
          ? `Allocation retry cooldown active after rate limit. Retry in ${retryInMs}ms`
          : `Allocation retry cooldown active. Retry in ${retryInMs}ms`
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

    if (!result.allocation) {
      return {
        status: "allocation-failed",
        event,
        purchaseId: result.purchaseId,
        reason: "Allocation result is missing",
        rawResult: result
      };
    }

    if (result.allocation.status === "allocated") {
      return {
        status: "allocated",
        event,
        purchaseId: result.purchaseId,
        reason: null,
        rawResult: result
      };
    }

    if (result.allocation.status === "deferred") {
      return {
        status: "deferred",
        event,
        purchaseId: result.purchaseId,
        reason: result.allocation.reason,
        rawResult: result
      };
    }

    return {
      status: "failed",
      event,
      purchaseId: result.purchaseId,
      reason: result.allocation.reason ?? "Allocation did not complete",
      rawResult: result
    };
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/server.ts

```ts
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
import { prepareAmbassadorWithdrawal } from "./jobs/prepareAmbassadorWithdrawal";
import { processAmbassadorPendingQueue } from "./jobs/processAmbassadorPendingQueue";
import { finalizeAmbassadorWithdrawal } from "./jobs/finalizeAmbassadorWithdrawal";

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
    gasStationServiceChargeType:
      String(process.env.GASSTATION_SERVICE_CHARGE_TYPE || "10010").trim() || "10010"
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

function extractErrorCode(error: unknown): string | null {
  if (!error || typeof error !== "object") {
    return null;
  }

  const candidates = [
    (error as any).code,
    (error as any).errorCode,
    (error as any).status,
    (error as any).statusCode
  ];

  for (const candidate of candidates) {
    if (candidate == null) {
      continue;
    }

    const code = String(candidate).trim();

    if (code) {
      return code;
    }
  }

  return null;
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

function toNumberSafe(value: unknown): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function isResourceLikeError(error: unknown): boolean {
  const message = toErrorMessage(error).toLowerCase();
  const code = String(extractErrorCode(error) || "").toLowerCase();

  return (
    message.includes("resource insufficient") ||
    message.includes("out of energy") ||
    message.includes("insufficient energy") ||
    message.includes("insufficient bandwidth") ||
    message.includes("bandwidth") ||
    message.includes("energy") ||
    code.includes("resource") ||
    code.includes("energy") ||
    code.includes("bandwidth") ||
    code.includes("gasstation_operator_balance_low") ||
    code.includes("gasstation_service_balance_low")
  );
}

function classifyHttpStatus(error: unknown): number {
  const code = String(extractErrorCode(error) || "").toLowerCase();

  if (code === "invalid_status") return 400;
  if (code.includes("not_found")) return 404;
  if (code.includes("rate_limit") || code === "429") return 429;
  if (isResourceLikeError(error)) return 409;

  return 500;
}

function parseAllocationMode(value: unknown): "eager" | "claim-first" | undefined {
  const normalized = String(value ?? "").trim().toLowerCase();

  if (normalized === "eager") {
    return "eager";
  }

  if (normalized === "claim-first" || normalized === "claim") {
    return "claim-first";
  }

  return undefined;
}

function createGasStationClientOrThrow(env: EnvConfig) {
  if (!env.gasStationEnabled) {
    throw new Error("GasStation is disabled");
  }

  return createGasStationClientFromEnv();
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
        const client = createGasStationClientOrThrow(env);
        const gasBalance = await client.getBalance();

        const operatorAddress = normalizeAddress(
          tronWeb?.defaultAddress?.base58 || tronWeb?.defaultAddress?.hex || "",
          "operatorAddress"
        );

        const operatorBalanceSun = Number(await tronWeb.trx.getBalance(operatorAddress));
        const serviceBalanceSun = parseTrxAmountToSun(gasBalance.balance, "gasStation.balance");
        const depositAddress = normalizeAddress(
          String((gasBalance as any).deposit_address || ""),
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

      if (method === "GET" && pathname === "/debug/gasstation/order-check") {
        const client = createGasStationClientOrThrow(env);

        const operatorAddress = normalizeAddress(
          tronWeb?.defaultAddress?.base58 || tronWeb?.defaultAddress?.hex || "",
          "operatorAddress"
        );

        const [gasBalance, accountResources, accountInfo, operatorBalanceSunRaw] =
          await Promise.all([
            client.getBalance(),
            tronWeb.trx.getAccountResources(operatorAddress),
            tronWeb.trx.getAccount(operatorAddress),
            tronWeb.trx.getBalance(operatorAddress)
          ]);

        const operatorBalanceSun = toNumberSafe(operatorBalanceSunRaw);
        const serviceBalanceSun = parseTrxAmountToSun(gasBalance.balance, "gasStation.balance");
        const depositAddress = normalizeAddress(
          String((gasBalance as any).deposit_address || ""),
          "deposit_address"
        );

        const energyLimit = toNumberSafe(accountResources?.EnergyLimit);
        const energyUsed = toNumberSafe(accountResources?.EnergyUsed);
        const energyAvailable = Math.max(0, energyLimit - energyUsed);

        const freeNetLimit = Math.max(
          toNumberSafe(accountInfo?.freeNetLimit),
          toNumberSafe(accountInfo?.free_net_limit)
        );
        const freeNetUsed = Math.max(
          toNumberSafe(accountInfo?.freeNetUsed),
          toNumberSafe(accountInfo?.free_net_used)
        );
        const netLimit = Math.max(
          toNumberSafe(accountInfo?.NetLimit),
          toNumberSafe(accountInfo?.net_limit)
        );
        const netUsed = Math.max(
          toNumberSafe(accountInfo?.NetUsed),
          toNumberSafe(accountInfo?.net_used)
        );

        const freeBandwidthAvailable = Math.max(0, freeNetLimit - freeNetUsed);
        const paidBandwidthAvailable = Math.max(0, netLimit - netUsed);
        const bandwidthAvailable = freeBandwidthAvailable + paidBandwidthAvailable;

        const missingEnergy = Math.max(0, env.allocationMinEnergy - energyAvailable);
        const missingBandwidth = Math.max(0, env.allocationMinBandwidth - bandwidthAvailable);

        const energyToBuy =
          missingEnergy > 0
            ? Math.max(env.gasStationMinEnergy, missingEnergy, 64400)
            : 0;

        const bandwidthToBuy =
          missingBandwidth > 0
            ? Math.max(env.gasStationMinBandwidth, missingBandwidth, 5000)
            : 0;

        const availableForTopUpSun = Math.max(
          0,
          operatorBalanceSun - OPERATOR_REMAINING_RESERVE_SUN
        );

        const needsTopUp = serviceBalanceSun < GASSTATION_LOW_BALANCE_SUN;
        const canTopUp =
          operatorBalanceSun >= OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN &&
          availableForTopUpSun >= GASSTATION_LOW_BALANCE_SUN;

        let energyEstimate: unknown = null;
        let energyEstimateError: { message: string; code: string | null } | null = null;

        if (energyToBuy > 0) {
          try {
            energyEstimate = await client.estimateEnergyOrder({
              receiveAddress: operatorAddress,
              addressTo: operatorAddress,
              contractAddress: resolvedControllerContractAddress,
              serviceChargeType: env.gasStationServiceChargeType
            });
          } catch (error) {
            energyEstimateError = {
              message: toErrorMessage(error),
              code: extractErrorCode(error)
            };
          }
        }

        let bandwidthPrice: unknown = null;
        let bandwidthPriceError: { message: string; code: string | null } | null = null;

        if (bandwidthToBuy > 0) {
          try {
            bandwidthPrice = await client.getPrice({
              serviceChargeType: env.gasStationServiceChargeType,
              resourceValue: bandwidthToBuy
            });
          } catch (error) {
            bandwidthPriceError = {
              message: toErrorMessage(error),
              code: extractErrorCode(error)
            };
          }
        }

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
              canTopUp
            },
            resources: {
              current: {
                energyAvailable,
                bandwidthAvailable
              },
              thresholds: {
                allocationMinEnergy: env.allocationMinEnergy,
                allocationMinBandwidth: env.allocationMinBandwidth,
                gasStationMinEnergy: env.gasStationMinEnergy,
                gasStationMinBandwidth: env.gasStationMinBandwidth
              },
              missing: {
                missingEnergy,
                missingBandwidth
              },
              buyPlan: {
                energyToBuy,
                bandwidthToBuy
              }
            },
            checks: {
              energyEstimate: energyEstimate
                ? { ok: true, result: energyEstimate }
                : energyToBuy > 0
                  ? { ok: false, error: energyEstimateError }
                  : { ok: true, skipped: true, reason: "Energy is already sufficient" },
              bandwidthPrice: bandwidthPrice
                ? { ok: true, result: bandwidthPrice }
                : bandwidthToBuy > 0
                  ? { ok: false, error: bandwidthPriceError }
                  : { ok: true, skipped: true, reason: "Bandwidth is already sufficient" }
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
        const feeLimitSun =
          body.feeLimitSun !== undefined
            ? parsePositiveInteger(String(body.feeLimitSun), 1, "feeLimitSun")
            : undefined;

        const result = await cabinetService.replayPendingByWallet(
          wallet,
          Date.now(),
          feeLimitSun
        );

        sendJson(req, res, env, 200, {
          ok: true,
          result
        });
        return;
      }

      if (method === "POST" && pathname === "/cabinet/prepare-withdrawal") {
        const body = await readJsonBody(req);
        const wallet = normalizeIncomingWallet(body.wallet);
        const limit =
          body.limit !== undefined
            ? parsePositiveInteger(String(body.limit), 500, "limit")
            : 500;

        const record = await getAmbassadorRegistryRecordByWallet(wallet);

        if (!record?.privateIdentity?.wallet) {
          sendJson(req, res, env, 404, {
            ok: false,
            error: "Ambassador not found for wallet"
          });
          return;
        }

        const result = await prepareAmbassadorWithdrawal(worker, {
          ambassadorWallet: record.privateIdentity.wallet,
          ambassadorSlug: record.publicProfile.slug,
          limit,
          now: Date.now(),
          logger: console
        });

        sendJson(req, res, env, 200, {
          ok: true,
          result
        });
        return;
      }

      if (method === "POST" && pathname === "/cabinet/process-withdrawal-queue") {
        const body = await readJsonBody(req);
        const wallet = normalizeIncomingWallet(body.wallet);
        const limit =
          body.limit !== undefined
            ? parsePositiveInteger(String(body.limit), 100, "limit")
            : 100;
        const feeLimitSun =
          body.feeLimitSun !== undefined
            ? parsePositiveInteger(String(body.feeLimitSun), 1, "feeLimitSun")
            : undefined;

        const record = await getAmbassadorRegistryRecordByWallet(wallet);

        if (!record?.privateIdentity?.wallet) {
          sendJson(req, res, env, 404, {
            ok: false,
            error: "Ambassador not found for wallet"
          });
          return;
        }

        const result = await processAmbassadorPendingQueue(worker, {
          ambassadorWallet: record.privateIdentity.wallet,
          ambassadorSlug: record.publicProfile.slug,
          limit,
          now: Date.now(),
          feeLimitSun,
          logger: console
        });

        sendJson(req, res, env, 200, {
          ok: true,
          result
        });
        return;
      }

      if (method === "POST" && pathname === "/cabinet/confirm-withdrawal") {
        const body = await readJsonBody(req);
        const wallet = normalizeIncomingWallet(body.wallet);
        const withdrawSessionId = normalizeOptionalString(body.withdrawSessionId);
        const txid = normalizeOptionalString(body.txid);
        const limit =
          body.limit !== undefined
            ? parsePositiveInteger(String(body.limit), 1000, "limit")
            : 1000;

        const record = await getAmbassadorRegistryRecordByWallet(wallet);

        if (!record?.privateIdentity?.wallet) {
          sendJson(req, res, env, 404, {
            ok: false,
            error: "Ambassador not found for wallet"
          });
          return;
        }

        const result = await finalizeAmbassadorWithdrawal(worker, {
          ambassadorWallet: record.privateIdentity.wallet,
          ambassadorSlug: record.publicProfile.slug,
          withdrawSessionId,
          txid,
          limit,
          now: Date.now(),
          logger: console
        });

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
        const allocationMode = parseAllocationMode(body.allocationMode);

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
      const status = classifyHttpStatus(error);

      sendJson(req, res, env, status, {
        ok: false,
        error: toErrorMessage(error),
        code: extractErrorCode(error)
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
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/services/cabinet.ts

```ts
import { getAmbassadorRegistryRecordByWallet } from "../db/ambassadors";
import type {
  CabinetStatsRecord,
  PurchaseProcessingStatus,
  PurchaseStore
} from "../db/purchases";
import {
  getAllocationRetryReadyAt,
  isPurchaseReadyForAllocationRetry,
  isRateLimitedAllocationFailure
} from "../db/purchases";

export interface CabinetReplayResultItem {
  purchaseId: string;
  ok: boolean;
  skipped?: boolean;
  error?: string;
  result?: unknown;
}

export interface CabinetReplayPendingResult {
  wallet: string;
  totalFound: number;
  attempted: number;
  succeeded: number;
  failed: number;
  skipped: number;
  items: CabinetReplayResultItem[];
}

export interface CabinetServiceDependencies {
  store: PurchaseStore;
  tronWeb: any;
  controllerContractAddress: string;
  processor: {
    replayFailedAllocation: (
      purchaseId: string,
      feeLimitSun?: number,
      now?: number
    ) => Promise<unknown>;
  };
}

export interface CabinetProfileIdentity {
  wallet: string;
  exists: boolean;
  active: boolean;
  selfRegistered: boolean;
  manualAssigned: boolean;
  overrideEnabled: boolean;
  level: number;
  effectiveLevel: number;
  currentLevel: number;
  overrideLevel: number;
  rewardPercent: number;
  createdAt: number;
  slugHash: string;
  metaHash: string | null;
}

export interface CabinetProfileStats {
  totalBuyers: number;

  trackedVolumeSun: string;
  trackedVolumeTrx: string;

  claimableRewardsSun: string;
  claimableRewardsTrx: string;

  lifetimeRewardsSun: string;
  lifetimeRewardsTrx: string;

  withdrawnRewardsSun: string;
  withdrawnRewardsTrx: string;

  /**
   * Backward-compatible aliases for older UI consumers.
   */
  totalVolumeSun: string;
  totalVolumeTrx: string;
  totalRewardsAccruedSun: string;
  totalRewardsAccruedTrx: string;
  totalRewardsClaimedSun: string;
  totalRewardsClaimedTrx: string;
}

export interface CabinetProfileWithdrawalQueue {
  availableOnChainSun: string;
  availableOnChainTrx: string;
  availableOnChainCount: number;

  allocatedInDbSun: string;
  allocatedInDbTrx: string;
  allocatedInDbCount: number;

  pendingBackendSyncSun: string;
  pendingBackendSyncTrx: string;
  pendingBackendSyncCount: number;

  requestedForProcessingSun: string;
  requestedForProcessingTrx: string;
  requestedForProcessingCount: number;

  hasProcessingWithdrawal: boolean;
}

export interface CabinetProfileProgress {
  currentLevel: number;
  buyersCount: number;
  nextThreshold: number;
  remainingToNextLevel: number;
}

export interface CabinetProfileRegisteredResult {
  registered: true;
  wallet: string;
  slug: string;
  status: string;
  referralLink: string;
  identity: CabinetProfileIdentity;
  stats: CabinetProfileStats;
  withdrawalQueue: CabinetProfileWithdrawalQueue;
  progress: CabinetProfileProgress;
}

export interface CabinetProfileNotRegisteredResult {
  registered: false;
  wallet: string;
}

export type CabinetProfileResult =
  | CabinetProfileRegisteredResult
  | CabinetProfileNotRegisteredResult;

const DEFAULT_PENDING_STATUSES: PurchaseProcessingStatus[] = [
  "verified",
  "deferred",
  "allocation_failed_retryable"
];

const ZERO_BYTES32 =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function safeNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "bigint") {
    return Number(value);
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return fallback;
}

function safeBoolean(value: unknown): boolean {
  return Boolean(value);
}

function safeString(value: unknown, fallback = "0"): string {
  if (value == null) {
    return fallback;
  }

  const normalized = String(value).trim();
  return normalized || fallback;
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

function sunToTrxString(value: string | number | bigint | null | undefined): string {
  const raw = String(value ?? "0").trim();

  if (!raw || raw === "0") {
    return "0";
  }

  const negative = raw.startsWith("-");
  const digits = negative ? raw.slice(1) : raw;

  if (!/^\d+$/.test(digits)) {
    return "0";
  }

  const padded = digits.padStart(7, "0");
  const whole = padded.slice(0, -6) || "0";
  const fraction = padded.slice(-6).replace(/0+$/, "");
  const result = fraction ? `${whole}.${fraction}` : whole;

  return negative ? `-${result}` : result;
}

function levelToLabel(level: number): string {
  if (level === 0) return "Bronze";
  if (level === 1) return "Silver";
  if (level === 2) return "Gold";
  if (level === 3) return "Platinum";
  return `Unknown (${level})`;
}

function buildReferralLink(slug: string): string {
  return `https://4teen.me/?r=${encodeURIComponent(slug)}`;
}

function normalizeHex32(value: unknown): string {
  const raw = String(value ?? "").trim().toLowerCase();
  return raw || ZERO_BYTES32;
}

function normalizeMetaHash(value: unknown): string | null {
  const raw = String(value ?? "").trim().toLowerCase();

  if (!raw || raw === ZERO_BYTES32) {
    return null;
  }

  return raw;
}

function pickTupleValue(source: any, index: number, key?: string): unknown {
  if (Array.isArray(source)) {
    return source[index];
  }

  if (source && typeof source === "object") {
    if (key && key in source) {
      return source[key];
    }

    const numericKey = String(index);
    if (numericKey in source) {
      return source[numericKey];
    }

    const values = Object.values(source);
    return values[index];
  }

  return undefined;
}

function toSunString(value: unknown): string {
  if (typeof value === "bigint") {
    return value.toString();
  }

  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return Math.floor(value).toString();
  }

  if (typeof value === "string" && /^\d+$/.test(value.trim())) {
    return value.trim();
  }

  return "0";
}

function mapStats(input: {
  onChainStats: {
    totalBuyers: string;
    trackedVolumeSun: string;
    claimableRewardsSun: string;
    lifetimeRewardsSun: string;
    withdrawnRewardsSun: string;
  };
  dbStats: CabinetStatsRecord;
}): {
  stats: CabinetProfileStats;
  withdrawalQueue: CabinetProfileWithdrawalQueue;
} {
  const { onChainStats, dbStats } = input;

  const trackedVolumeSun = onChainStats.trackedVolumeSun;
  const claimableRewardsSun = onChainStats.claimableRewardsSun;
  const lifetimeRewardsSun = onChainStats.lifetimeRewardsSun;
  const withdrawnRewardsSun = onChainStats.withdrawnRewardsSun;

  return {
    stats: {
      totalBuyers: safeNumber(onChainStats.totalBuyers),

      trackedVolumeSun,
      trackedVolumeTrx: sunToTrxString(trackedVolumeSun),

      claimableRewardsSun,
      claimableRewardsTrx: sunToTrxString(claimableRewardsSun),

      lifetimeRewardsSun,
      lifetimeRewardsTrx: sunToTrxString(lifetimeRewardsSun),

      withdrawnRewardsSun,
      withdrawnRewardsTrx: sunToTrxString(withdrawnRewardsSun),

      totalVolumeSun: trackedVolumeSun,
      totalVolumeTrx: sunToTrxString(trackedVolumeSun),
      totalRewardsAccruedSun: lifetimeRewardsSun,
      totalRewardsAccruedTrx: sunToTrxString(lifetimeRewardsSun),
      totalRewardsClaimedSun: withdrawnRewardsSun,
      totalRewardsClaimedTrx: sunToTrxString(withdrawnRewardsSun)
    },
    withdrawalQueue: {
      /**
       * Real withdrawable amount.
       * Must come from on-chain contract state only.
       */
      availableOnChainSun: claimableRewardsSun,
      availableOnChainTrx: sunToTrxString(claimableRewardsSun),
      availableOnChainCount: dbStats.availableOnChainCount,

      /**
       * Backend accounting only.
       */
      allocatedInDbSun: dbStats.allocatedInDbSun,
      allocatedInDbTrx: sunToTrxString(dbStats.allocatedInDbSun),
      allocatedInDbCount: dbStats.allocatedInDbCount,

      /**
       * Verified / deferred / retryable queue in backend.
       */
      pendingBackendSyncSun: dbStats.pendingBackendSyncSun,
      pendingBackendSyncTrx: sunToTrxString(dbStats.pendingBackendSyncSun),
      pendingBackendSyncCount: dbStats.pendingBackendSyncCount,

      /**
       * Already included into withdrawal processing queue.
       */
      requestedForProcessingSun: dbStats.requestedForProcessingSun,
      requestedForProcessingTrx: sunToTrxString(dbStats.requestedForProcessingSun),
      requestedForProcessingCount: dbStats.requestedForProcessingCount,

      hasProcessingWithdrawal: dbStats.hasProcessingWithdrawal
    }
  };
}

function extractReplayStatus(result: unknown): string {
  if (
    result &&
    typeof result === "object" &&
    "status" in result &&
    typeof (result as { status?: unknown }).status === "string"
  ) {
    return String((result as { status: string }).status).trim().toLowerCase();
  }

  return "";
}

function extractReplayReason(result: unknown): string | null {
  if (
    result &&
    typeof result === "object" &&
    "reason" in result &&
    typeof (result as { reason?: unknown }).reason === "string"
  ) {
    const reason = String((result as { reason: string }).reason).trim();
    return reason || null;
  }

  if (
    result &&
    typeof result === "object" &&
    "errorMessage" in result &&
    typeof (result as { errorMessage?: unknown }).errorMessage === "string"
  ) {
    const errorMessage = String(
      (result as { errorMessage: string }).errorMessage
    ).trim();
    return errorMessage || null;
  }

  return null;
}

export class CabinetService {
  private readonly store: PurchaseStore;
  private readonly tronWeb: any;
  private readonly controllerContractAddress: string;
  private readonly processor: CabinetServiceDependencies["processor"];
  private contractInstance: any | null = null;

  constructor(deps: CabinetServiceDependencies) {
    if (!deps?.store) {
      throw new Error("store is required");
    }

    if (!deps?.tronWeb) {
      throw new Error("tronWeb is required");
    }

    if (!deps?.processor) {
      throw new Error("processor is required");
    }

    this.store = deps.store;
    this.tronWeb = deps.tronWeb;
    this.processor = deps.processor;
    this.controllerContractAddress = assertNonEmpty(
      deps.controllerContractAddress,
      "controllerContractAddress"
    );
  }

  private async contract(): Promise<any> {
    if (!this.contractInstance) {
      this.contractInstance = await this.tronWeb.contract().at(this.controllerContractAddress);
    }

    return this.contractInstance;
  }

  private async readOnChainDashboard(wallet: string): Promise<{
    identity: CabinetProfileIdentity;
    progress: CabinetProfileProgress;
    stats: {
      totalBuyers: string;
      trackedVolumeSun: string;
      claimableRewardsSun: string;
      lifetimeRewardsSun: string;
      withdrawnRewardsSun: string;
    };
  }> {
    const contract = await this.contract();

    const [coreRaw, profileRaw, progressRaw, statsRaw] = await Promise.all([
      contract.getDashboardCore(wallet).call(),
      contract.getDashboardProfile(wallet).call(),
      contract.getAmbassadorLevelProgress(wallet).call(),
      contract.getDashboardStats(wallet).call()
    ]);

    const exists = safeBoolean(pickTupleValue(coreRaw, 0, "exists"));
    const active = safeBoolean(pickTupleValue(coreRaw, 1, "active"));
    const effectiveLevel = safeNumber(pickTupleValue(coreRaw, 2, "effectiveLevel"));
    const rewardPercent = safeNumber(pickTupleValue(coreRaw, 3, "rewardPercent"));
    const createdAt = safeNumber(pickTupleValue(coreRaw, 4, "createdAt"));

    const selfRegistered = safeBoolean(pickTupleValue(profileRaw, 0, "selfRegistered"));
    const manualAssigned = safeBoolean(pickTupleValue(profileRaw, 1, "manualAssigned"));
    const overrideEnabled = safeBoolean(pickTupleValue(profileRaw, 2, "overrideEnabled"));
    const currentLevel = safeNumber(pickTupleValue(profileRaw, 3, "currentLevel"));
    const overrideLevel = safeNumber(pickTupleValue(profileRaw, 4, "overrideLevel"));
    const slugHash = normalizeHex32(pickTupleValue(profileRaw, 5, "slugHash"));
    const metaHash = normalizeMetaHash(pickTupleValue(profileRaw, 6, "metaHash"));

    const progressCurrentLevel = safeNumber(
      pickTupleValue(progressRaw, 0, "currentLevel"),
      currentLevel
    );
    const buyersCount = safeNumber(pickTupleValue(progressRaw, 1, "buyersCount"));
    const nextThreshold = safeNumber(pickTupleValue(progressRaw, 2, "nextThreshold"));
    const remainingToNextLevel = safeNumber(
      pickTupleValue(progressRaw, 3, "remainingToNextLevel")
    );

    const totalBuyers = toSunString(pickTupleValue(statsRaw, 0, "totalBuyers"));
    const trackedVolumeSun = toSunString(
      pickTupleValue(statsRaw, 1, "trackedVolumeSun") ??
        pickTupleValue(statsRaw, 1, "totalVolumeSun")
    );
    const lifetimeRewardsSun = toSunString(
      pickTupleValue(statsRaw, 2, "lifetimeRewardsSun") ??
        pickTupleValue(statsRaw, 2, "totalRewardsAccruedSun")
    );
    const withdrawnRewardsSun = toSunString(
      pickTupleValue(statsRaw, 3, "withdrawnRewardsSun") ??
        pickTupleValue(statsRaw, 3, "totalRewardsClaimedSun")
    );
    const claimableRewardsSun = toSunString(
      pickTupleValue(statsRaw, 4, "claimableRewardsSun")
    );

    return {
      identity: {
        wallet,
        exists,
        active,
        selfRegistered,
        manualAssigned,
        overrideEnabled,
        level: effectiveLevel,
        effectiveLevel,
        currentLevel,
        overrideLevel,
        rewardPercent,
        createdAt,
        slugHash,
        metaHash
      },
      progress: {
        currentLevel: progressCurrentLevel,
        buyersCount,
        nextThreshold,
        remainingToNextLevel
      },
      stats: {
        totalBuyers,
        trackedVolumeSun,
        claimableRewardsSun,
        lifetimeRewardsSun,
        withdrawnRewardsSun
      }
    };
  }

  private buildFallbackProfile(
    wallet: string,
    slug: string,
    status: string,
    dbStatsRecord: CabinetStatsRecord
  ): CabinetProfileRegisteredResult {
    const mapped = mapStats({
      onChainStats: {
        totalBuyers: String(dbStatsRecord.totalBuyers || 0),
        trackedVolumeSun: dbStatsRecord.trackedVolumeSun || "0",
        claimableRewardsSun: "0",
        lifetimeRewardsSun: dbStatsRecord.lifetimeRewardsSun || "0",
        withdrawnRewardsSun: dbStatsRecord.withdrawnRewardsSun || "0"
      },
      dbStats: dbStatsRecord
    });

    return {
      registered: true,
      wallet,
      slug,
      status,
      referralLink: buildReferralLink(slug),
      identity: {
        wallet,
        exists: true,
        active: status === "active",
        selfRegistered: false,
        manualAssigned: false,
        overrideEnabled: false,
        level: 0,
        effectiveLevel: 0,
        currentLevel: 0,
        overrideLevel: 0,
        rewardPercent: 0,
        createdAt: 0,
        slugHash: ZERO_BYTES32,
        metaHash: null
      },
      stats: mapped.stats,
      withdrawalQueue: mapped.withdrawalQueue,
      progress: {
        currentLevel: 0,
        buyersCount: dbStatsRecord.totalBuyers || 0,
        nextThreshold: 0,
        remainingToNextLevel: 0
      }
    };
  }

  async getProfileByWallet(wallet: string): Promise<CabinetProfileResult> {
    const normalizedWallet = assertNonEmpty(wallet, "wallet");
    const record = await getAmbassadorRegistryRecordByWallet(normalizedWallet);

    if (!record) {
      return {
        registered: false,
        wallet: normalizedWallet
      };
    }

    const registryWallet = assertNonEmpty(record.privateIdentity.wallet, "registryWallet");
    const slug = record.publicProfile.slug;
    const status = record.publicProfile.status;

    const dbStatsRecord = await this.store.getCabinetStatsByAmbassadorWallet(registryWallet);

    try {
      const onChain = await this.readOnChainDashboard(registryWallet);
      const mapped = mapStats({
        onChainStats: onChain.stats,
        dbStats: dbStatsRecord
      });

      return {
        registered: true,
        wallet: registryWallet,
        slug,
        status,
        referralLink: buildReferralLink(slug),
        identity: {
          ...onChain.identity,
          active: status === "active" ? onChain.identity.active : false
        },
        stats: mapped.stats,
        withdrawalQueue: mapped.withdrawalQueue,
        progress: onChain.progress
      };
    } catch {
      return this.buildFallbackProfile(
        registryWallet,
        slug,
        status,
        dbStatsRecord
      );
    }
  }

  async replayPendingByWallet(
    wallet: string,
    now: number = Date.now(),
    feeLimitSun?: number
  ): Promise<CabinetReplayPendingResult> {
    const normalizedWallet = assertNonEmpty(wallet, "wallet");
    const record = await getAmbassadorRegistryRecordByWallet(normalizedWallet);

    if (!record) {
      throw new Error("Ambassador not found for wallet");
    }

    const registryWallet = assertNonEmpty(record.privateIdentity.wallet, "registryWallet");

    const pending = await this.store.listPendingByAmbassador({
      ambassadorWallet: registryWallet,
      statuses: DEFAULT_PENDING_STATUSES
    });

    const items: CabinetReplayResultItem[] = [];

    for (const purchase of pending) {
      if (!isPurchaseReadyForAllocationRetry(purchase, now)) {
        const retryAt = getAllocationRetryReadyAt(purchase);
        const retryInMs = Math.max(0, retryAt - now);

        items.push({
          purchaseId: purchase.purchaseId,
          ok: true,
          skipped: true,
          error: isRateLimitedAllocationFailure(purchase)
            ? `Cooldown active after rate limit. Retry in ${retryInMs}ms`
            : `Cooldown active. Retry in ${retryInMs}ms`
        });
        continue;
      }

      try {
        const result = await this.processor.replayFailedAllocation(
          purchase.purchaseId,
          feeLimitSun,
          now
        );

        const replayStatus = extractReplayStatus(result);
        const isAllocated = replayStatus === "allocated";
        const isSkipped = replayStatus === "skipped";
        const replayReason =
          extractReplayReason(result) ??
          (isSkipped ? "Replay skipped" : "Allocation failed");

        items.push({
          purchaseId: purchase.purchaseId,
          ok: isAllocated,
          skipped: isSkipped,
          error: isAllocated ? undefined : replayReason,
          result
        });
      } catch (error) {
        items.push({
          purchaseId: purchase.purchaseId,
          ok: false,
          error: toErrorMessage(error)
        });
      }
    }

    const succeeded = items.filter((item) => item.ok && !item.skipped).length;
    const skipped = items.filter((item) => item.skipped).length;
    const failed = items.filter((item) => !item.ok && !item.skipped).length;

    return {
      wallet: registryWallet,
      totalFound: pending.length,
      attempted: succeeded + failed,
      succeeded,
      failed,
      skipped,
      items
    };
  }
}

export function createCabinetService(deps: CabinetServiceDependencies): CabinetService {
  return new CabinetService(deps);
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/services/gasStation.ts

```ts
import crypto from "node:crypto";
import { ProxyAgent } from "undici";

export interface GasStationConfig {
  appId: string;
  secretKey: string;
  baseUrl?: string;
  proxyUrl?: string;
  timeoutMs?: number;
}

export interface GasStationBalanceResult {
  symbol: string;
  balance: string;
  deposit_address?: string;
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
  expire_min: string | number;
  service_charge_type: string;
  price: string | number;
  remaining_number: string | number;
}

export interface GasStationPriceResult {
  list?: GasStationPriceItem[];
  price_builder_list?: GasStationPriceItem[];
  resource_type?: string;
  min_number?: number;
  max_number?: number;
}

export interface GasStationCreateOrderResult {
  trade_no: string;
}

const DEFAULT_BASE_URL = "https://openapi.gasstation.ai";
const DEFAULT_TIMEOUT_MS = 15_000;
const DEFAULT_SERVICE_CHARGE_TYPE = "10010";
const MIN_ENERGY_ORDER = 64_400;
const MIN_BANDWIDTH_ORDER = 5_000;

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
  return String(value || DEFAULT_BASE_URL).trim().replace(/\/+$/, "");
}

function normalizeTimeoutMs(value?: number): number {
  const parsed = Number(value ?? DEFAULT_TIMEOUT_MS);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_TIMEOUT_MS;
  }

  return Math.max(Math.floor(parsed), 1_000);
}

function normalizePositiveInteger(value: number, fieldName: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${fieldName} must be a positive number`);
  }

  return Math.ceil(value);
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

async function requestJson<T>(params: {
  url: string;
  method?: "GET" | "POST";
  proxyUrl?: string;
  timeoutMs?: number;
}): Promise<T> {
  const { url, method = "GET", proxyUrl, timeoutMs = DEFAULT_TIMEOUT_MS } = params;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  const dispatcher = proxyUrl ? new ProxyAgent(proxyUrl) : undefined;

  try {
    const response = await fetch(url, {
      method,
      headers: {
        Accept: "application/json"
      },
      signal: controller.signal,
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
    if ((error as any)?.name === "AbortError") {
      throw createTaggedError("GasStation request timed out", {
        code: "GASSTATION_TIMEOUT",
        cause: error
      });
    }

    if (error instanceof Error) {
      throw error;
    }

    throw createTaggedError(`GasStation fetch failed: ${toErrorMessage(error)}`, {
      code: "GASSTATION_FETCH_FAILED",
      cause: error
    });
  } finally {
    clearTimeout(timer);

    try {
      await dispatcher?.close();
    } catch {
      // ignore proxy close errors
    }
  }
}

export class GasStationClient {
  private readonly appId: string;
  private readonly secretKey: string;
  private readonly baseUrl: string;
  private readonly proxyUrl?: string;
  private readonly timeoutMs: number;

  constructor(config: GasStationConfig) {
    this.appId = assertNonEmpty(config.appId, "appId");
    this.secretKey = assertNonEmpty(config.secretKey, "secretKey");
    this.baseUrl = normalizeBaseUrl(config.baseUrl);
    this.proxyUrl = normalizeOptionalString(config.proxyUrl);
    this.timeoutMs = normalizeTimeoutMs(config.timeoutMs);
  }

  private buildEncryptedUrl(path: string, payload: Record<string, unknown>): string {
    const plainText = JSON.stringify(payload);
    const encrypted = encryptAesEcbPkcs7Base64(plainText, this.secretKey);

    const url = new URL(`${this.baseUrl}${path}`);
    url.searchParams.set("app_id", this.appId);
    url.searchParams.set("data", encrypted);

    return url.toString();
  }

  private async getJson<T>(
    path: string,
    payload: Record<string, unknown>,
    method: "GET" | "POST" = "GET"
  ): Promise<T> {
    const url = this.buildEncryptedUrl(path, payload);

    return requestJson<T>({
      url,
      method,
      proxyUrl: this.proxyUrl,
      timeoutMs: this.timeoutMs
    });
  }

  async getBalance(time?: string): Promise<GasStationBalanceResult> {
    const payload = {
      time: time ?? String(Math.floor(Date.now() / 1000))
    };

    return this.getJson<GasStationBalanceResult>(
      "/api/mpc/tron/gas/balance",
      payload,
      "GET"
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

    return this.getJson<GasStationPriceResult>(
      "/api/tron/gas/order/price",
      payload,
      "GET"
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
        input.serviceChargeType ?? DEFAULT_SERVICE_CHARGE_TYPE,
        "serviceChargeType"
      )
    };

    return this.getJson<GasStationEstimateResult>(
      "/api/tron/gas/estimate",
      payload,
      "GET"
    );
  }

  async createEnergyOrder(input: {
    requestId: string;
    receiveAddress: string;
    energyNum: number;
    serviceChargeType?: string;
  }): Promise<GasStationCreateOrderResult> {
    const energyNum = normalizePositiveInteger(input.energyNum, "energyNum");

    if (energyNum < MIN_ENERGY_ORDER) {
      throw new Error(`energyNum must be at least ${MIN_ENERGY_ORDER}`);
    }

    const payload = {
      request_id: assertNonEmpty(input.requestId, "requestId"),
      receive_address: assertNonEmpty(input.receiveAddress, "receiveAddress"),
      buy_type: 0,
      service_charge_type: assertNonEmpty(
        input.serviceChargeType ?? DEFAULT_SERVICE_CHARGE_TYPE,
        "serviceChargeType"
      ),
      energy_num: energyNum
    };

    return this.getJson<GasStationCreateOrderResult>(
      "/api/tron/gas/create_order",
      payload,
      "POST"
    );
  }

  async createBandwidthOrder(input: {
    requestId: string;
    receiveAddress: string;
    netNum: number;
    serviceChargeType?: string;
  }): Promise<GasStationCreateOrderResult> {
    const netNum = normalizePositiveInteger(input.netNum, "netNum");

    if (netNum < MIN_BANDWIDTH_ORDER) {
      throw new Error(`netNum must be at least ${MIN_BANDWIDTH_ORDER}`);
    }

    const payload = {
      request_id: assertNonEmpty(input.requestId, "requestId"),
      receive_address: assertNonEmpty(input.receiveAddress, "receiveAddress"),
      buy_type: 0,
      service_charge_type: assertNonEmpty(
        input.serviceChargeType ?? DEFAULT_SERVICE_CHARGE_TYPE,
        "serviceChargeType"
      ),
      net_num: netNum
    };

    return this.getJson<GasStationCreateOrderResult>(
      "/api/tron/gas/create_order",
      payload,
      "POST"
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
      process.env.FIXIE_URL,
    timeoutMs: Number(process.env.GASSTATION_TIMEOUT_MS || DEFAULT_TIMEOUT_MS)
  });
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/tron/controller.ts

```ts
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

interface ResourceRequirement {
  requiredEnergy: number;
  requiredBandwidth: number;
  targetEnergy: number;
  targetBandwidth: number;
}

const TRON_HEX_ZERO_ADDRESS = "410000000000000000000000000000000000000000";
const DEFAULT_SERVICE_CHARGE_TYPE = "10010";
const DEFAULT_TRON_RETRY_ATTEMPTS = 4;
const DEFAULT_FEE_LIMIT_SUN = 300_000_000;

const SUN_PER_TRX = 1_000_000;

const GASSTATION_LOW_BALANCE_SUN = 8_500_000;
const OPERATOR_MIN_BALANCE_FOR_TOPUP_SUN = 11_000_000;
const OPERATOR_REMAINING_RESERVE_SUN = 2_000_000;

const GASSTATION_TOPUP_POLL_INTERVAL_MS = 4000;
const GASSTATION_TOPUP_POLL_ATTEMPTS = 12;

const RESOURCE_DELIVERY_POLL_INTERVAL_MS = 5000;
const RESOURCE_DELIVERY_POLL_ATTEMPTS = 18;

const RESOURCE_STABILIZATION_DELAY_MS = 2500;
const RESOURCE_RECHECK_BEFORE_SEND_DELAY_MS = 1500;

const MIN_ENERGY_ORDER_FLOOR = 64_400;
const MIN_BANDWIDTH_ORDER_FLOOR = 5_000;

const ENERGY_MARGIN_PERCENT = 20;
const BANDWIDTH_MARGIN_PERCENT = 20;
const MIN_ENERGY_MARGIN = 12_000;
const MIN_BANDWIDTH_MARGIN = 1_500;

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
  const resolved = value ?? DEFAULT_FEE_LIMIT_SUN;

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

      if (energyOk && bandwidthOk) {
        await delay(RESOURCE_STABILIZATION_DELAY_MS);

        const stableSnapshot = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);
        const stableEnergyOk =
          requirement.targetEnergy <= 0 ||
          stableSnapshot.energyAvailable >= requirement.targetEnergy;
        const stableBandwidthOk =
          requirement.targetBandwidth <= 0 ||
          stableSnapshot.bandwidthAvailable >= requirement.targetBandwidth;

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

  private buildCurrentRequirement(): ResourceRequirement {
    return buildResourceRequirement(this.allocationMinEnergy, this.allocationMinBandwidth);
  }

  private async ensureResourcesForAllocation(purchaseId: string): Promise<void> {
    const operatorAddress = this.getOperatorAddress();
    const requirement = this.buildCurrentRequirement();
    const before = await getAccountResourceSnapshot(this.tronWeb, operatorAddress);

    const missingEnergy = Math.max(0, requirement.targetEnergy - before.energyAvailable);
    const missingBandwidth = Math.max(0, requirement.targetBandwidth - before.bandwidthAvailable);

    if (missingEnergy <= 0 && missingBandwidth <= 0) {
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

    await this.waitForRequiredResources(requirement);
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

    if (!energyOk || !bandwidthOk) {
      throw createTaggedError(
        `Account resources dropped before contract send. Energy=${snapshot.energyAvailable}, Bandwidth=${snapshot.bandwidthAvailable}, RequiredEnergy=${requirement.requiredEnergy}, RequiredBandwidth=${requirement.requiredBandwidth}`,
        {
          code: "ACCOUNT_RESOURCE_CONSUMED_BEFORE_SEND"
        }
      );
    }
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

    return this.runWithOperatorLock(async () => {
      await this.ensureResourcesForAllocation(purchaseId);
      await this.verifyResourcesStillReadyBeforeSend();

      const contract = await this.contract();

      try {
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

        return {
          txid: assertNonEmpty(txid || "", "txid")
        };
      } catch (error) {
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
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/tron/hashing.ts

```ts
import { keccak_256 } from "@noble/hashes/sha3";
import { utf8ToBytes } from "@noble/hashes/utils";

export interface PurchaseIdInput {
  txHash: string;
  buyerWallet: string;
}

export interface AttributionHashing {
  hashSlugToBytes32Hex(slug: string): string;
  derivePurchaseId(input: PurchaseIdInput): string;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function stripHexPrefix(value: string): string {
  return value.startsWith("0x") ? value.slice(2) : value;
}

function isHex(value: string): boolean {
  return /^[0-9a-fA-F]+$/.test(value);
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function toBytes32HexFromUtf8(value: string): string {
  const bytes = utf8ToBytes(value);
  const hash = keccak_256(bytes);
  return `0x${bytesToHex(hash)}`;
}

function normalizeSlugForHashing(slug: string): string {
  return assertNonEmpty(slug, "slug").trim().toLowerCase();
}

function normalizeTxHash(txHash: string): string {
  const normalized = assertNonEmpty(txHash, "txHash").trim().toLowerCase();
  const stripped = stripHexPrefix(normalized);

  if (!isHex(stripped)) {
    throw new Error("txHash must be a hex string");
  }

  return stripped;
}

function normalizeWalletForPurchaseId(wallet: string): string {
  return assertNonEmpty(wallet, "buyerWallet").trim();
}

export class TronHashing implements AttributionHashing {
  hashSlugToBytes32Hex(slug: string): string {
    const normalizedSlug = normalizeSlugForHashing(slug);
    return toBytes32HexFromUtf8(normalizedSlug);
  }

  derivePurchaseId(input: PurchaseIdInput): string {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWalletForPurchaseId(input.buyerWallet);

    const combined = `${txHash}:${buyerWallet}`;
    return toBytes32HexFromUtf8(combined);
  }
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/tron/resources.ts

```ts
export interface AccountResourceSnapshot {
  address: string;
  bandwidth: {
    freeNetLimit: number;
    freeNetUsed: number;
    netLimit: number;
    netUsed: number;
    totalLimit: number;
    totalUsed: number;
    available: number;
  };
  energy: {
    energyLimit: number;
    energyUsed: number;
    available: number;
  };
  latestOperationTime?: number;
  trxBalanceSun: number;
  raw: {
    account: any;
    resources: any;
    bandwidthRaw: any;
    balanceSunRaw: any;
  };
}

export interface AllocationResourcePolicy {
  minEnergyRequired: number;
  minBandwidthRequired: number;
  safetyEnergyBuffer: number;
  safetyBandwidthBuffer: number;
  minEnergyOrderFloor: number;
  minBandwidthOrderFloor: number;
}

export interface AllocationResourceCheckResult {
  ok: boolean;
  address: string;
  availableEnergy: number;
  availableBandwidth: number;
  requiredEnergy: number;
  requiredBandwidth: number;
  targetEnergy: number;
  targetBandwidth: number;
  shortEnergy: number;
  shortBandwidth: number;
  energyToBuy: number;
  bandwidthToBuy: number;
  reason: string | null;
  snapshot: AccountResourceSnapshot;
}

export interface ResourceGateway {
  getAccountResourceSnapshot(address: string): Promise<AccountResourceSnapshot>;
  checkAllocationReadiness(
    address: string,
    policy: AllocationResourcePolicy
  ): Promise<AllocationResourceCheckResult>;
}

function toSafeNumber(value: unknown): number {
  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }

  return parsed;
}

function normalizeAddressFromTronWeb(tronWeb: any, address: string): string {
  const raw = String(address || "").trim();

  if (!raw) {
    throw new Error("address is required");
  }

  if (typeof tronWeb?.address?.fromHex === "function" && raw.startsWith("41")) {
    try {
      return tronWeb.address.fromHex(raw);
    } catch {
      return raw;
    }
  }

  return raw;
}

function sumBandwidth(resources: any, account: any, bandwidthRaw: any) {
  const freeNetLimit = Math.max(
    toSafeNumber(account?.freeNetLimit),
    toSafeNumber(resources?.freeNetLimit),
    toSafeNumber(account?.free_net_limit),
    toSafeNumber(resources?.free_net_limit),
    toSafeNumber(account?.freeNetLimitV2),
    toSafeNumber(resources?.freeNetLimitV2)
  );

  const freeNetUsed = Math.max(
    toSafeNumber(account?.freeNetUsed),
    toSafeNumber(resources?.freeNetUsed),
    toSafeNumber(account?.free_net_used),
    toSafeNumber(resources?.free_net_used),
    toSafeNumber(account?.freeNetUsedV2),
    toSafeNumber(resources?.freeNetUsedV2)
  );

  const netLimit = Math.max(
    toSafeNumber(account?.NetLimit),
    toSafeNumber(resources?.NetLimit),
    toSafeNumber(account?.netLimit),
    toSafeNumber(resources?.netLimit),
    toSafeNumber(account?.net_limit),
    toSafeNumber(resources?.net_limit)
  );

  const netUsed = Math.max(
    toSafeNumber(account?.NetUsed),
    toSafeNumber(resources?.NetUsed),
    toSafeNumber(account?.netUsed),
    toSafeNumber(resources?.netUsed),
    toSafeNumber(account?.net_used),
    toSafeNumber(resources?.net_used)
  );

  const totalLimit = freeNetLimit + netLimit;
  const totalUsed = freeNetUsed + netUsed;
  const calculatedAvailable = Math.max(totalLimit - totalUsed, 0);

  const available = Math.max(calculatedAvailable, toSafeNumber(bandwidthRaw));

  return {
    freeNetLimit,
    freeNetUsed,
    netLimit,
    netUsed,
    totalLimit,
    totalUsed,
    available
  };
}

function sumEnergy(resources: any, account: any) {
  const energyLimit = Math.max(
    toSafeNumber(resources?.EnergyLimit),
    toSafeNumber(account?.EnergyLimit),
    toSafeNumber(resources?.energyLimit),
    toSafeNumber(account?.energyLimit),
    toSafeNumber(resources?.energy_limit),
    toSafeNumber(account?.energy_limit)
  );

  const energyUsed = Math.max(
    toSafeNumber(resources?.EnergyUsed),
    toSafeNumber(account?.EnergyUsed),
    toSafeNumber(resources?.energyUsed),
    toSafeNumber(account?.energyUsed),
    toSafeNumber(resources?.energy_used),
    toSafeNumber(account?.energy_used)
  );

  const available = Math.max(energyLimit - energyUsed, 0);

  return {
    energyLimit,
    energyUsed,
    available
  };
}

function calculateTarget(baseRequired: number, buffer: number): number {
  return Math.max(toSafeNumber(baseRequired) + toSafeNumber(buffer), 0);
}

export function buildDefaultAllocationResourcePolicy(
  overrides?: Partial<AllocationResourcePolicy>
): AllocationResourcePolicy {
  return {
    minEnergyRequired: overrides?.minEnergyRequired ?? 180_000,
    minBandwidthRequired: overrides?.minBandwidthRequired ?? 1_000,
    safetyEnergyBuffer: overrides?.safetyEnergyBuffer ?? 20_000,
    safetyBandwidthBuffer: overrides?.safetyBandwidthBuffer ?? 300,
    minEnergyOrderFloor: overrides?.minEnergyOrderFloor ?? 64_400,
    minBandwidthOrderFloor: overrides?.minBandwidthOrderFloor ?? 5_000
  };
}

export function createResourceGateway(tronWeb: any): ResourceGateway {
  if (!tronWeb) {
    throw new Error("tronWeb is required");
  }

  async function getAccountResourceSnapshot(address: string): Promise<AccountResourceSnapshot> {
    const normalizedAddress = normalizeAddressFromTronWeb(tronWeb, address);
    const accountAddress =
      typeof tronWeb?.address?.toHex === "function"
        ? tronWeb.address.toHex(normalizedAddress)
        : normalizedAddress;

    const [account, resources, bandwidthRaw, balanceSunRaw] = await Promise.all([
      tronWeb.trx.getAccount(accountAddress),
      tronWeb.trx.getAccountResources(accountAddress),
      typeof tronWeb?.trx?.getBandwidth === "function"
        ? tronWeb.trx.getBandwidth(accountAddress)
        : 0,
      typeof tronWeb?.trx?.getBalance === "function"
        ? tronWeb.trx.getBalance(accountAddress)
        : 0
    ]);

    const bandwidth = sumBandwidth(resources, account, bandwidthRaw);
    const energy = sumEnergy(resources, account);

    return {
      address: normalizedAddress,
      bandwidth,
      energy,
      latestOperationTime:
        toSafeNumber(account?.latest_opration_time) ||
        toSafeNumber(account?.latestOperationTime) ||
        undefined,
      trxBalanceSun: toSafeNumber(balanceSunRaw),
      raw: {
        account,
        resources,
        bandwidthRaw,
        balanceSunRaw
      }
    };
  }

  async function checkAllocationReadiness(
    address: string,
    policy: AllocationResourcePolicy
  ): Promise<AllocationResourceCheckResult> {
    const snapshot = await getAccountResourceSnapshot(address);

    const requiredEnergy = Math.max(toSafeNumber(policy.minEnergyRequired), 0);
    const requiredBandwidth = Math.max(toSafeNumber(policy.minBandwidthRequired), 0);

    const targetEnergy = calculateTarget(requiredEnergy, policy.safetyEnergyBuffer);
    const targetBandwidth = calculateTarget(requiredBandwidth, policy.safetyBandwidthBuffer);

    const availableEnergy = snapshot.energy.available;
    const availableBandwidth = snapshot.bandwidth.available;

    const shortEnergy = Math.max(targetEnergy - availableEnergy, 0);
    const shortBandwidth = Math.max(targetBandwidth - availableBandwidth, 0);

    const energyToBuy =
      shortEnergy > 0
        ? Math.max(shortEnergy, toSafeNumber(policy.minEnergyOrderFloor))
        : 0;

    const bandwidthToBuy =
      shortBandwidth > 0
        ? Math.max(shortBandwidth, toSafeNumber(policy.minBandwidthOrderFloor))
        : 0;

    let reason: string | null = null;

    if (shortEnergy > 0 && shortBandwidth > 0) {
      reason =
        `Insufficient energy and bandwidth. ` +
        `Need +${shortEnergy} energy and +${shortBandwidth} bandwidth to reach buffered target.`;
    } else if (shortEnergy > 0) {
      reason = `Insufficient energy. Need +${shortEnergy} energy to reach buffered target.`;
    } else if (shortBandwidth > 0) {
      reason = `Insufficient bandwidth. Need +${shortBandwidth} bandwidth to reach buffered target.`;
    }

    return {
      ok: !reason,
      address: snapshot.address,
      availableEnergy,
      availableBandwidth,
      requiredEnergy,
      requiredBandwidth,
      targetEnergy,
      targetBandwidth,
      shortEnergy,
      shortBandwidth,
      energyToBuy,
      bandwidthToBuy,
      reason,
      snapshot
    };
  }

  return {
    getAccountResourceSnapshot,
    checkAllocationReadiness
  };
}

export interface GasStationBalanceSnapshot {
  ok: boolean;
  availableEnergy?: number;
  availableBandwidth?: number;
  raw: unknown;
}

export interface GasStationBalanceReader {
  getBalance(): Promise<GasStationBalanceSnapshot>;
}

export interface EffectiveAllocationResourceDecision {
  ok: boolean;
  reason: string | null;
  wallet: AllocationResourceCheckResult;
  gasStation?: {
    balance: GasStationBalanceSnapshot;
    energySatisfied: boolean;
    bandwidthSatisfied: boolean;
    shortEnergyCovered: number;
    shortBandwidthCovered: number;
  };
}

export async function evaluateEffectiveAllocationReadiness(params: {
  gateway: ResourceGateway;
  address: string;
  policy: AllocationResourcePolicy;
  gasStationClient?: GasStationBalanceReader;
  requireGasStationReserve?: boolean;
}): Promise<EffectiveAllocationResourceDecision> {
  const wallet = await params.gateway.checkAllocationReadiness(params.address, params.policy);

  if (!params.gasStationClient || !params.requireGasStationReserve) {
    return {
      ok: wallet.ok,
      reason: wallet.reason,
      wallet
    };
  }

  const balance = await params.gasStationClient.getBalance();

  const gasEnergy = toSafeNumber(balance.availableEnergy);
  const gasBandwidth = toSafeNumber(balance.availableBandwidth);

  const shortEnergyCovered = Math.min(gasEnergy, wallet.shortEnergy);
  const shortBandwidthCovered = Math.min(gasBandwidth, wallet.shortBandwidth);

  const energySatisfied = wallet.shortEnergy <= 0 || gasEnergy >= wallet.shortEnergy;
  const bandwidthSatisfied = wallet.shortBandwidth <= 0 || gasBandwidth >= wallet.shortBandwidth;

  if (wallet.ok) {
    return {
      ok: true,
      reason: null,
      wallet,
      gasStation: {
        balance,
        energySatisfied: true,
        bandwidthSatisfied: true,
        shortEnergyCovered: 0,
        shortBandwidthCovered: 0
      }
    };
  }

  if (energySatisfied && bandwidthSatisfied) {
    return {
      ok: true,
      reason: null,
      wallet,
      gasStation: {
        balance,
        energySatisfied,
        bandwidthSatisfied,
        shortEnergyCovered,
        shortBandwidthCovered
      }
    };
  }

  const reasons: string[] = [];

  if (wallet.shortEnergy > 0 && !energySatisfied) {
    reasons.push(
      `Gas Station reserve energy is insufficient to cover shortfall. Need ${wallet.shortEnergy}, got ${gasEnergy}.`
    );
  }

  if (wallet.shortBandwidth > 0 && !bandwidthSatisfied) {
    reasons.push(
      `Gas Station reserve bandwidth is insufficient to cover shortfall. Need ${wallet.shortBandwidth}, got ${gasBandwidth}.`
    );
  }

  if (!reasons.length && wallet.reason) {
    reasons.push(wallet.reason);
  }

  return {
    ok: false,
    reason: reasons.join(" "),
    wallet,
    gasStation: {
      balance,
      energySatisfied,
      bandwidthSatisfied,
      shortEnergyCovered,
      shortBandwidthCovered
    }
  };
}
```

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/tsconfig.json

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2022"],
    "module": "CommonJS",
    "moduleResolution": "Node",
    "outDir": "dist",
    "rootDir": "../..",
    "strict": true,
    "noImplicitAny": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": false,
    "esModuleInterop": true,
    "allowSyntheticDefaultImports": true,
    "skipLibCheck": true,
    "resolveJsonModule": true,
    "declaration": false,
    "sourceMap": true,
    "types": ["node"]
  },
  "include": [
    "src/**/*.ts",
    "../../shared/**/*.ts"
  ],
  "exclude": ["dist", "node_modules"]
}
```
