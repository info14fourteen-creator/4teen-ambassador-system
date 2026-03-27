# 4teen-ambassador-system — ALLOCATION WORKER

Generated: 2026-03-27T17:45:44.226Z
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
    "tronweb": "^6.0.4"
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
  claimableRewardsSun: string;
  lifetimeRewardsSun: string;
  withdrawnRewardsSun: string;
  availableOnChainSun: string;
  pendingBackendSyncSun: string;
  requestedForProcessingSun: string;
  availableOnChainCount: number;
  pendingBackendSyncCount: number;
  requestedForProcessingCount: number;
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
    pendingBackendSyncSun: "0",
    requestedForProcessingSun: "0",
    availableOnChainCount: 0,
    pendingBackendSyncCount: 0,
    requestedForProcessingCount: 0,
    hasProcessingWithdrawal: false
  };
}

function createRecord(input: CreatePurchaseRecordInput): PurchaseRecord {
  const now = input.now ?? Date.now();
  const status = normalizeStatus(input.status);
  const allocatedAt =
    input.allocatedAt !== undefined
      ? normalizeTimestamp(input.allocatedAt, "allocatedAt")
      : status === "allocated"
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
      : nextStatus === "allocated"
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
    claimableRewardsSun: String(row.claimable_rewards_sun || "0"),
    lifetimeRewardsSun: String(row.lifetime_rewards_sun || "0"),
    withdrawnRewardsSun: String(row.withdrawn_rewards_sun || "0"),
    availableOnChainSun: String(row.available_on_chain_sun || "0"),
    pendingBackendSyncSun: String(row.pending_backend_sync_sun || "0"),
    requestedForProcessingSun: String(row.requested_for_processing_sun || "0"),
    availableOnChainCount: Number(row.available_on_chain_count || 0),
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
              'allocation_failed_final'
            ) THEN purchase_amount_sun::numeric
            ELSE 0
          END)::text, '0') AS tracked_volume_sun,

          COALESCE(SUM(CASE
            WHEN status = 'allocated' AND withdraw_session_id IS NULL THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS claimable_rewards_sun,

          COALESCE(SUM(CASE
            WHEN status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocated',
              'allocation_failed_retryable',
              'allocation_failed_final'
            ) THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS lifetime_rewards_sun,

          COALESCE(SUM(CASE
            WHEN withdraw_session_id IS NOT NULL THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS withdrawn_rewards_sun,

          COALESCE(SUM(CASE
            WHEN status = 'allocated' AND withdraw_session_id IS NULL THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS available_on_chain_sun,

          COALESCE(SUM(CASE
            WHEN status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocation_failed_retryable'
            ) AND withdraw_session_id IS NULL THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS pending_backend_sync_sun,

          COALESCE(SUM(CASE
            WHEN withdraw_session_id IS NOT NULL THEN owner_share_sun::numeric
            ELSE 0
          END)::text, '0') AS requested_for_processing_sun,

          COUNT(*) FILTER (
            WHERE status = 'allocated' AND withdraw_session_id IS NULL
          ) AS available_on_chain_count,

          COUNT(*) FILTER (
            WHERE status IN (
              'verified',
              'deferred',
              'allocation_in_progress',
              'allocation_failed_retryable'
            ) AND withdraw_session_id IS NULL
          ) AS pending_backend_sync_count,

          COUNT(*) FILTER (
            WHERE withdraw_session_id IS NOT NULL
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
    let claimableRewardsSun = "0";
    let lifetimeRewardsSun = "0";
    let withdrawnRewardsSun = "0";
    let availableOnChainSun = "0";
    let pendingBackendSyncSun = "0";
    let requestedForProcessingSun = "0";

    let availableOnChainCount = 0;
    let pendingBackendSyncCount = 0;
    let requestedForProcessingCount = 0;

    for (const row of rows) {
      if (row.status !== "received" && row.buyerWallet) {
        buyers.add(row.buyerWallet);
      }

      const contributesVolume =
        row.status === "verified" ||
        row.status === "deferred" ||
        row.status === "allocation_in_progress" ||
        row.status === "allocated" ||
        row.status === "allocation_failed_retryable" ||
        row.status === "allocation_failed_final";

      if (contributesVolume) {
        trackedVolumeSun = sumSunStrings(trackedVolumeSun, row.purchaseAmountSun);
        lifetimeRewardsSun = sumSunStrings(lifetimeRewardsSun, row.ownerShareSun);
      }

      const isAvailableOnChain =
        row.status === "allocated" && !row.withdrawSessionId;

      if (isAvailableOnChain) {
        claimableRewardsSun = sumSunStrings(claimableRewardsSun, row.ownerShareSun);
        availableOnChainSun = sumSunStrings(availableOnChainSun, row.ownerShareSun);
        availableOnChainCount += 1;
      }

      const isPendingBackendSync =
        (row.status === "verified" ||
          row.status === "deferred" ||
          row.status === "allocation_in_progress" ||
          row.status === "allocation_failed_retryable") &&
        !row.withdrawSessionId;

      if (isPendingBackendSync) {
        pendingBackendSyncSun = sumSunStrings(pendingBackendSyncSun, row.ownerShareSun);
        pendingBackendSyncCount += 1;
      }

      if (row.withdrawSessionId) {
        withdrawnRewardsSun = sumSunStrings(withdrawnRewardsSun, row.ownerShareSun);
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
      claimableRewardsSun,
      lifetimeRewardsSun,
      withdrawnRewardsSun,
      availableOnChainSun,
      pendingBackendSyncSun,
      requestedForProcessingSun,
      availableOnChainCount,
      pendingBackendSyncCount,
      requestedForProcessingCount,
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
  allocate(
    input: AllocationExecutorInput
  ): Promise<AllocationExecutorResult>;
}

export type AllocationAttemptStatus =
  | "allocated"
  | "deferred"
  | "retryable-failed"
  | "final-failed"
  | "skipped-already-final"
  | "skipped-no-ambassador-wallet";

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
}

export interface AllocatePendingBatchResult {
  ambassadorWallet: string;
  processed: AllocationAttemptResult[];
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
    (error as any).name
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
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
    value.includes("not enough bandwidth")
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
    value.includes("rate limit") ||
    value.includes("too many requests")
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
    value.includes("permission denied") ||
    value.includes("bad request")
  );
}

function classifyAllocationError(error: unknown): ClassifiedAllocationError {
  const message = toErrorMessage(error);
  const code = extractErrorCode(error);
  const lowerCode = toLowerSafe(code);

  if (
    isResourceInsufficientMessage(message) ||
    lowerCode.includes("out_of_energy") ||
    lowerCode.includes("insufficient")
  ) {
    return {
      kind: "resource",
      code,
      reason: "Account resource insufficient error.",
      message
    };
  }

  if (
    isRetryableTransportMessage(message) ||
    lowerCode.includes("timeout") ||
    lowerCode.includes("network")
  ) {
    return {
      kind: "retryable",
      code,
      reason: "Temporary allocation transport error.",
      message
    };
  }

  if (isFinalMessage(message)) {
    return {
      kind: "final",
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

    for (const purchase of pending) {
      const result = await this.tryAllocatePurchaseRecord(purchase, {
        feeLimitSun: input.feeLimitSun,
        allocationMode: input.allocationMode ?? "claim-first"
      });

      processed.push(result);
    }

    return {
      ambassadorWallet,
      processed
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
          status: "deferred",
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
    status === "ignored" ||
    status === "allocation_failed_final"
  );
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

  if (result.status === "deferred") {
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

      const linkedPurchase = await this.store.attachAmbassadorToPurchase({
        purchaseId: receivedPurchase.purchase.purchaseId,
        ambassadorSlug: slug,
        ambassadorWallet: ambassador.wallet,
        purchaseAmountSun: "0",
        ownerShareSun: "0",
        now
      });

      if (linkedPurchase) {
        receivedPurchase.purchase = linkedPurchase;
      }
    }

    const currentPurchase = await this.store.getByPurchaseId(
      receivedPurchase.purchase.purchaseId
    );

    if (!currentPurchase) {
      throw new Error("Failed to reload purchase after frontend attribution");
    }

    if (!ambassador || ambassador.status !== "active" || !ambassador.wallet) {
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
          reason: attributionReason,
          canAllocate: false
        }
      };
    }

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
        reason: null,
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

    if (isFinalPurchaseStatus(verifiedPurchase.status)) {
      return {
        stage: "verified-purchase",
        purchaseId: verifiedPurchase.purchaseId,
        attribution: {
          status: "matched-local-record",
          purchase: verifiedPurchase,
          slug: verifiedPurchase.ambassadorSlug,
          slugHash,
          ambassadorWallet: verifiedPurchase.ambassadorWallet,
          reason: null
        },
        verification: {
          status: "already-finalized",
          purchase: verifiedPurchase,
          slug: verifiedPurchase.ambassadorSlug,
          slugHash,
          ambassadorWallet: verifiedPurchase.ambassadorWallet,
          reason: `Purchase already finalized with status: ${verifiedPurchase.status}`,
          canAllocate: false
        }
      };
    }

    if (!verifiedPurchase.ambassadorWallet) {
      return {
        stage: "verified-purchase",
        purchaseId: verifiedPurchase.purchaseId,
        attribution: {
          status: "matched-local-record",
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
        status: purchase.status === "received" ? "matched-local-record" : "duplicate-local-record",
        purchase: allocationResult.purchase,
        slug: allocationResult.purchase.ambassadorSlug,
        slugHash,
        ambassadorWallet: allocationResult.purchase.ambassadorWallet,
        reason: purchase.status === "received" ? null : "Purchase already exists in local store"
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

    return {
      stage: "verified-purchase",
      purchaseId: result.purchaseId,
      attribution: result.attribution,
      verification: result.verification,
      allocation:
        result.allocation == null
          ? null
          : ({
              status:
                result.allocation.status === "allocated"
                  ? "allocated"
                  : result.allocation.status === "deferred"
                    ? "deferred"
                    : result.allocation.status === "skipped"
                      ? "skipped-no-ambassador-wallet"
                      : "retryable-failed",
              purchase: result.allocation.purchase,
              txid: result.allocation.txid,
              reason: result.allocation.reason,
              errorCode: null,
              errorMessage: result.allocation.reason
            } satisfies AllocationDecision)
    };
  }

  async replayFailedAllocation(
    purchaseId: string,
    feeLimitSun?: number,
    now?: number
  ): Promise<ReplayFailedAllocationApiResult> {
    return this.allocation.replayFailedAllocation(purchaseId, feeLimitSun, now);
  }
}

export function createAllocationWorker(
  options: CreateAllocationWorkerOptions
): AllocationWorker {
  const store = createPurchaseStore();

  const executorConfig: TronControllerAllocationExecutorConfig = {
    tronWeb: options.tronWeb,
    controllerContractAddress: options.controllerContractAddress
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

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/jobs/prepareAmbassadorWithdrawal.ts

```ts
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
  reason: string | null;
}

export interface PrepareAmbassadorWithdrawalJobResult {
  ok: boolean;
  ambassadorSlug: string | null;
  ambassadorWallet: string | null;
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

async function loadCandidatePurchases(
  worker: AllocationWorker,
  options: {
    ambassadorSlug?: string;
    ambassadorWallet?: string;
    limit: number;
  }
): Promise<any[]> {
  const storeAny = worker.store as any;

  if (typeof storeAny.listAllocatedPurchasesForWithdrawalPreparation === "function") {
    return storeAny.listAllocatedPurchasesForWithdrawalPreparation({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  if (typeof storeAny.listAllocatedPurchasesByAmbassador === "function") {
    return storeAny.listAllocatedPurchasesByAmbassador({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  if (typeof storeAny.listAllocatedPurchases === "function") {
    return storeAny.listAllocatedPurchases({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  throw new Error(
    "Purchase store does not support listing allocated purchases for withdrawal preparation"
  );
}

async function markPreparedForWithdrawal(
  worker: AllocationWorker,
  purchaseId: string,
  now: number
): Promise<void> {
  const storeAny = worker.store as any;

  if (typeof storeAny.markPurchasePendingAmbassadorWithdrawal === "function") {
    await storeAny.markPurchasePendingAmbassadorWithdrawal(purchaseId, now);
    return;
  }

  if (typeof storeAny.markPurchaseQueuedForWithdrawal === "function") {
    await storeAny.markPurchaseQueuedForWithdrawal(purchaseId, now);
    return;
  }

  if (typeof storeAny.updatePurchaseStatus === "function") {
    await storeAny.updatePurchaseStatus(purchaseId, "pending_ambassador_withdrawal", null, now);
    return;
  }

  throw new Error(
    "Purchase store does not support marking purchase as pending ambassador withdrawal"
  );
}

export async function prepareAmbassadorWithdrawal(
  worker: AllocationWorker,
  options: PrepareAmbassadorWithdrawalJobOptions = {}
): Promise<PrepareAmbassadorWithdrawalJobResult> {
  const startedAt = Date.now();
  const logger = options.logger ?? console;
  const now = options.now ?? Date.now();
  const limit = toPositiveInteger(options.limit, 500);

  const ambassadorSlug = normalizeOptionalString(options.ambassadorSlug) ?? null;
  const ambassadorWallet = normalizeOptionalString(options.ambassadorWallet) ?? null;

  const result: PrepareAmbassadorWithdrawalJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    scanned: 0,
    prepared: 0,
    skipped: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    const purchases = await loadCandidatePurchases(worker, {
      ambassadorSlug: ambassadorSlug ?? undefined,
      ambassadorWallet: ambassadorWallet ?? undefined,
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

    for (const purchase of purchases) {
      const currentStatus = String(purchase.status || "").trim();

      if (
        currentStatus !== "allocated" &&
        currentStatus !== "ready_for_withdrawal" &&
        currentStatus !== "queued_for_withdrawal"
      ) {
        result.skipped += 1;
        result.items.push({
          purchaseId: purchase.purchaseId,
          txHash: purchase.txHash,
          buyerWallet: purchase.buyerWallet,
          ambassadorSlug: purchase.ambassadorSlug,
          ambassadorWallet: purchase.ambassadorWallet,
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus || "unknown",
          queuedForWithdrawal: false,
          reason: `Unsupported status for withdrawal preparation: ${currentStatus || "unknown"}`
        });
        continue;
      }

      if (currentStatus === "queued_for_withdrawal") {
        result.skipped += 1;
        result.items.push({
          purchaseId: purchase.purchaseId,
          txHash: purchase.txHash,
          buyerWallet: purchase.buyerWallet,
          ambassadorSlug: purchase.ambassadorSlug,
          ambassadorWallet: purchase.ambassadorWallet,
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: currentStatus,
          queuedForWithdrawal: true,
          reason: "Already queued for withdrawal"
        });
        continue;
      }

      await markPreparedForWithdrawal(worker, purchase.purchaseId, now);

      result.prepared += 1;
      result.items.push({
        purchaseId: purchase.purchaseId,
        txHash: purchase.txHash,
        buyerWallet: purchase.buyerWallet,
        ambassadorSlug: purchase.ambassadorSlug,
        ambassadorWallet: purchase.ambassadorWallet,
        purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
        ownerShareSun: String(purchase.ownerShareSun ?? "0"),
        status: "pending_ambassador_withdrawal",
        queuedForWithdrawal: true,
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
  action: "allocated" | "deferred" | "skipped" | "failed";
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

function isResourceError(message: string): boolean {
  const normalized = message.toLowerCase();

  return (
    normalized.includes("resource insufficient") ||
    normalized.includes("account resource insufficient") ||
    normalized.includes("out of energy") ||
    normalized.includes("insufficient energy") ||
    normalized.includes("insufficient bandwidth") ||
    normalized.includes("bandwidth") ||
    normalized.includes("energy")
  );
}

async function listPendingQueuePurchases(
  worker: AllocationWorker,
  options: {
    ambassadorSlug?: string;
    ambassadorWallet?: string;
    limit: number;
  }
): Promise<any[]> {
  const storeAny = worker.store as any;

  if (typeof storeAny.listPendingAmbassadorWithdrawalPurchases === "function") {
    return storeAny.listPendingAmbassadorWithdrawalPurchases({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  if (typeof storeAny.listQueuedWithdrawalPurchases === "function") {
    return storeAny.listQueuedWithdrawalPurchases({
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  if (typeof storeAny.listPurchasesByStatus === "function") {
    return storeAny.listPurchasesByStatus("pending_ambassador_withdrawal", {
      ambassadorSlug: options.ambassadorSlug,
      ambassadorWallet: options.ambassadorWallet,
      limit: options.limit
    });
  }

  throw new Error(
    "Purchase store does not support listing pending ambassador withdrawal queue"
  );
}

async function markDeferred(
  worker: AllocationWorker,
  purchaseId: string,
  reason: string,
  now: number
): Promise<void> {
  const storeAny = worker.store as any;

  if (typeof storeAny.markPurchaseDeferred === "function") {
    await storeAny.markPurchaseDeferred(purchaseId, reason, now);
    return;
  }

  if (typeof storeAny.updatePurchaseStatus === "function") {
    await storeAny.updatePurchaseStatus(
      purchaseId,
      "deferred",
      reason,
      now
    );
    return;
  }

  throw new Error("Purchase store does not support deferred status updates");
}

async function markFailed(
  worker: AllocationWorker,
  purchaseId: string,
  reason: string,
  now: number
): Promise<void> {
  const storeAny = worker.store as any;

  if (typeof storeAny.markPurchaseFailed === "function") {
    await storeAny.markPurchaseFailed(purchaseId, reason, now);
    return;
  }

  if (typeof storeAny.updatePurchaseStatus === "function") {
    await storeAny.updatePurchaseStatus(
      purchaseId,
      "failed",
      reason,
      now
    );
    return;
  }

  throw new Error("Purchase store does not support failure status updates");
}

async function canAllocateNow(
  worker: AllocationWorker,
  purchase: any,
  feeLimitSun: number | undefined
): Promise<{ ok: boolean; reason: string | null }> {
  const processorAny = worker.processor as any;

  if (typeof processorAny.checkAllocationResources === "function") {
    const result = await processorAny.checkAllocationResources({
      purchaseId: purchase.purchaseId,
      buyerWallet: purchase.buyerWallet,
      ambassadorWallet: purchase.ambassadorWallet,
      purchaseAmountSun: purchase.purchaseAmountSun,
      ownerShareSun: purchase.ownerShareSun,
      feeLimitSun
    });

    if (result && typeof result === "object") {
      return {
        ok: Boolean((result as any).ok),
        reason: normalizeOptionalString((result as any).reason) ?? null
      };
    }
  }

  return { ok: true, reason: null };
}

async function replayOnePurchase(
  worker: AllocationWorker,
  purchaseId: string,
  feeLimitSun: number | undefined,
  now: number
): Promise<any> {
  const processorAny = worker.processor as any;

  if (typeof processorAny.replayFailedAllocation === "function") {
    return processorAny.replayFailedAllocation(purchaseId, feeLimitSun, now);
  }

  if (typeof processorAny.processPendingPurchaseAllocation === "function") {
    return processorAny.processPendingPurchaseAllocation({
      purchaseId,
      feeLimitSun,
      now
    });
  }

  throw new Error("Allocation processor does not support replaying pending purchases");
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

  const ambassadorSlug = normalizeOptionalString(options.ambassadorSlug) ?? null;
  const ambassadorWallet = normalizeOptionalString(options.ambassadorWallet) ?? null;

  const result: ProcessAmbassadorPendingQueueJobResult = {
    ok: true,
    ambassadorSlug,
    ambassadorWallet,
    scanned: 0,
    allocated: 0,
    deferred: 0,
    skipped: 0,
    failed: 0,
    items: [],
    startedAt,
    finishedAt: startedAt
  };

  try {
    const purchases = await listPendingQueuePurchases(worker, {
      ambassadorSlug: ambassadorSlug ?? undefined,
      ambassadorWallet: ambassadorWallet ?? undefined,
      limit
    });

    result.scanned = purchases.length;

    logger.info?.(
      JSON.stringify({
        ok: true,
        job: "processAmbassadorPendingQueue",
        message: "Loaded pending ambassador withdrawal queue",
        ambassadorSlug,
        ambassadorWallet,
        scanned: result.scanned,
        limit
      })
    );

    for (const purchase of purchases) {
      const purchaseId = String(purchase.purchaseId || "").trim();
      const status = String(purchase.status || "").trim();

      if (!purchaseId) {
        result.skipped += 1;
        result.items.push({
          purchaseId: "",
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: status || "unknown",
          action: "skipped",
          reason: "Missing purchaseId",
          txid: null
        });
        continue;
      }

      if (
        status !== "pending_ambassador_withdrawal" &&
        status !== "queued_for_withdrawal" &&
        status !== "deferred"
      ) {
        result.skipped += 1;
        result.items.push({
          purchaseId,
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: status || "unknown",
          action: "skipped",
          reason: `Unsupported queue status: ${status || "unknown"}`,
          txid: null
        });
        continue;
      }

      const resourceCheck = await canAllocateNow(worker, purchase, feeLimitSun);

      if (!resourceCheck.ok) {
        const reason = resourceCheck.reason || "Not enough resources for allocation";

        await markDeferred(worker, purchaseId, reason, now);

        result.deferred += 1;
        result.items.push({
          purchaseId,
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: "deferred",
          action: "deferred",
          reason,
          txid: null
        });

        continue;
      }

      try {
        const replayResult = await replayOnePurchase(worker, purchaseId, feeLimitSun, now);
        const replayStatus = String(replayResult?.status || "").trim().toLowerCase();
        const txid = normalizeOptionalString(replayResult?.txid) ?? null;
        const reason = normalizeOptionalString(replayResult?.reason) ?? null;

        if (replayStatus === "allocated") {
          result.allocated += 1;
          result.items.push({
            purchaseId,
            txHash: String(purchase.txHash || ""),
            buyerWallet: String(purchase.buyerWallet || ""),
            ambassadorSlug: String(purchase.ambassadorSlug || ""),
            ambassadorWallet: String(purchase.ambassadorWallet || ""),
            purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
            ownerShareSun: String(purchase.ownerShareSun ?? "0"),
            status: "allocated",
            action: "allocated",
            reason: null,
            txid
          });
          continue;
        }

        if (replayStatus === "deferred") {
          result.deferred += 1;
          result.items.push({
            purchaseId,
            txHash: String(purchase.txHash || ""),
            buyerWallet: String(purchase.buyerWallet || ""),
            ambassadorSlug: String(purchase.ambassadorSlug || ""),
            ambassadorWallet: String(purchase.ambassadorWallet || ""),
            purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
            ownerShareSun: String(purchase.ownerShareSun ?? "0"),
            status: "deferred",
            action: "deferred",
            reason: reason || "Deferred by allocation processor",
            txid
          });
          continue;
        }

        if (replayStatus === "failed") {
          const failureReason = reason || "Allocation processor returned failed status";

          if (isResourceError(failureReason)) {
            await markDeferred(worker, purchaseId, failureReason, now);

            result.deferred += 1;
            result.items.push({
              purchaseId,
              txHash: String(purchase.txHash || ""),
              buyerWallet: String(purchase.buyerWallet || ""),
              ambassadorSlug: String(purchase.ambassadorSlug || ""),
              ambassadorWallet: String(purchase.ambassadorWallet || ""),
              purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
              ownerShareSun: String(purchase.ownerShareSun ?? "0"),
              status: "deferred",
              action: "deferred",
              reason: failureReason,
              txid
            });
          } else {
            await markFailed(worker, purchaseId, failureReason, now);

            result.failed += 1;
            result.items.push({
              purchaseId,
              txHash: String(purchase.txHash || ""),
              buyerWallet: String(purchase.buyerWallet || ""),
              ambassadorSlug: String(purchase.ambassadorSlug || ""),
              ambassadorWallet: String(purchase.ambassadorWallet || ""),
              purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
              ownerShareSun: String(purchase.ownerShareSun ?? "0"),
              status: "failed",
              action: "failed",
              reason: failureReason,
              txid
            });
          }

          continue;
        }

        result.skipped += 1;
        result.items.push({
          purchaseId,
          txHash: String(purchase.txHash || ""),
          buyerWallet: String(purchase.buyerWallet || ""),
          ambassadorSlug: String(purchase.ambassadorSlug || ""),
          ambassadorWallet: String(purchase.ambassadorWallet || ""),
          purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
          ownerShareSun: String(purchase.ownerShareSun ?? "0"),
          status: replayStatus || "unknown",
          action: "skipped",
          reason: reason || `Unexpected replay status: ${replayStatus || "unknown"}`,
          txid
        });
      } catch (error) {
        const reason = toErrorMessage(error);

        if (isResourceError(reason)) {
          await markDeferred(worker, purchaseId, reason, now);

          result.deferred += 1;
          result.items.push({
            purchaseId,
            txHash: String(purchase.txHash || ""),
            buyerWallet: String(purchase.buyerWallet || ""),
            ambassadorSlug: String(purchase.ambassadorSlug || ""),
            ambassadorWallet: String(purchase.ambassadorWallet || ""),
            purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
            ownerShareSun: String(purchase.ownerShareSun ?? "0"),
            status: "deferred",
            action: "deferred",
            reason,
            txid: null
          });
        } else {
          await markFailed(worker, purchaseId, reason, now);

          result.failed += 1;
          result.items.push({
            purchaseId,
            txHash: String(purchase.txHash || ""),
            buyerWallet: String(purchase.buyerWallet || ""),
            ambassadorSlug: String(purchase.ambassadorSlug || ""),
            ambassadorWallet: String(purchase.ambassadorWallet || ""),
            purchaseAmountSun: String(purchase.purchaseAmountSun ?? "0"),
            ownerShareSun: String(purchase.ownerShareSun ?? "0"),
            status: "failed",
            action: "failed",
            reason,
            txid: null
          });
        }
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

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/server.ts

```ts
import http from "node:http";
import { URL } from "node:url";
import TronWebModule from "tronweb";
import { assertValidSlug, normalizeSlug } from "../../../shared/utils/slug";
import { createAllocationWorker } from "./index";
import { BuyTokensScanner } from "./run-scan";
import { createCabinetService } from "./services/cabinet";
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
  const config: EnvConfig = {
    port: parsePositiveInteger(process.env.PORT, 3000, "PORT"),
    tronFullHost: assertNonEmpty(process.env.TRON_FULL_HOST, "TRON_FULL_HOST"),
    tronPrivateKey: assertNonEmpty(process.env.TRON_PRIVATE_KEY, "TRON_PRIVATE_KEY"),
    scanPageSize: parsePositiveInteger(process.env.SCAN_PAGE_SIZE, 50, "SCAN_PAGE_SIZE"),
    allowedOrigins: parseAllowedOrigins(process.env.ALLOWED_ORIGINS)
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
    controllerContractAddress: env.controllerContractAddress,
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
    store: worker.store
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
          timestamp: Date.now()
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
            ? parsePositiveInteger(String(body.feeLimitSun), 0, "feeLimitSun")
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
        allowedOrigins: env.allowedOrigins
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
import type { CabinetStatsRecord, PurchaseStore } from "../db/purchases";

export interface CabinetServiceDependencies {
  store: PurchaseStore;
  tronWeb: any;
  controllerContractAddress: string;
}

export interface CabinetProfileIdentity {
  active: boolean;
  level: number;
  levelLabel: string;
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
}

export interface CabinetProfileWithdrawalQueue {
  availableOnChainSun: string;
  availableOnChainTrx: string;
  pendingBackendSyncSun: string;
  pendingBackendSyncTrx: string;
  requestedForProcessingSun: string;
  requestedForProcessingTrx: string;
  availableOnChainCount: number;
  pendingBackendSyncCount: number;
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

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function safeString(value: unknown, fallback = "0"): string {
  if (value == null) return fallback;
  const normalized = String(value).trim();
  return normalized || fallback;
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
  const raw = String(value ?? "").trim();
  return raw || "0x0000000000000000000000000000000000000000000000000000000000000000";
}

function normalizeMetaHash(value: unknown): string | null {
  const raw = String(value ?? "").trim().toLowerCase();

  if (!raw) {
    return null;
  }

  if (raw === "0x0000000000000000000000000000000000000000000000000000000000000000") {
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

function mapStats(stats: CabinetStatsRecord): {
  stats: CabinetProfileStats;
  withdrawalQueue: CabinetProfileWithdrawalQueue;
} {
  return {
    stats: {
      totalBuyers: stats.totalBuyers,
      trackedVolumeSun: stats.trackedVolumeSun,
      trackedVolumeTrx: sunToTrxString(stats.trackedVolumeSun),
      claimableRewardsSun: stats.claimableRewardsSun,
      claimableRewardsTrx: sunToTrxString(stats.claimableRewardsSun),
      lifetimeRewardsSun: stats.lifetimeRewardsSun,
      lifetimeRewardsTrx: sunToTrxString(stats.lifetimeRewardsSun),
      withdrawnRewardsSun: stats.withdrawnRewardsSun,
      withdrawnRewardsTrx: sunToTrxString(stats.withdrawnRewardsSun)
    },
    withdrawalQueue: {
      availableOnChainSun: stats.availableOnChainSun,
      availableOnChainTrx: sunToTrxString(stats.availableOnChainSun),
      pendingBackendSyncSun: stats.pendingBackendSyncSun,
      pendingBackendSyncTrx: sunToTrxString(stats.pendingBackendSyncSun),
      requestedForProcessingSun: stats.requestedForProcessingSun,
      requestedForProcessingTrx: sunToTrxString(stats.requestedForProcessingSun),
      availableOnChainCount: stats.availableOnChainCount,
      pendingBackendSyncCount: stats.pendingBackendSyncCount,
      requestedForProcessingCount: stats.requestedForProcessingCount,
      hasProcessingWithdrawal: stats.hasProcessingWithdrawal
    }
  };
}

export class CabinetService {
  private readonly store: PurchaseStore;
  private readonly tronWeb: any;
  private readonly controllerContractAddress: string;
  private contractInstance: any | null = null;

  constructor(deps: CabinetServiceDependencies) {
    if (!deps?.store) {
      throw new Error("store is required");
    }

    if (!deps?.tronWeb) {
      throw new Error("tronWeb is required");
    }

    this.store = deps.store;
    this.tronWeb = deps.tronWeb;
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
  }> {
    const contract = await this.contract();

    const [
      coreRaw,
      profileRaw,
      progressRaw
    ] = await Promise.all([
      contract.getDashboardCore(wallet).call(),
      contract.getDashboardProfile(wallet).call(),
      contract.getAmbassadorLevelProgress(wallet).call()
    ]);

    const active = safeBoolean(pickTupleValue(coreRaw, 1, "active"));
    const effectiveLevel = safeNumber(pickTupleValue(coreRaw, 2, "effectiveLevel"));
    const rewardPercent = safeNumber(pickTupleValue(coreRaw, 3, "rewardPercent"));
    const createdAt = safeNumber(pickTupleValue(coreRaw, 4, "createdAt"));

    const currentLevel = safeNumber(pickTupleValue(profileRaw, 3, "currentLevel"));
    const slugHash = normalizeHex32(pickTupleValue(profileRaw, 5, "slugHash"));
    const metaHash = normalizeMetaHash(pickTupleValue(profileRaw, 6, "metaHash"));

    const buyersCount = safeNumber(pickTupleValue(progressRaw, 1, "buyersCount"));
    const nextThreshold = safeNumber(pickTupleValue(progressRaw, 2, "nextThreshold"));
    const remainingToNextLevel = safeNumber(
      pickTupleValue(progressRaw, 3, "remainingToNextLevel")
    );

    return {
      identity: {
        active,
        level: effectiveLevel,
        levelLabel: levelToLabel(effectiveLevel),
        rewardPercent,
        createdAt,
        slugHash,
        metaHash
      },
      progress: {
        currentLevel,
        buyersCount,
        nextThreshold,
        remainingToNextLevel
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

    const registryWallet = record.privateIdentity.wallet;
    const statsRecord = await this.store.getCabinetStatsByAmbassadorWallet(registryWallet);
    const mapped = mapStats(statsRecord);
    const onChain = await this.readOnChainDashboard(registryWallet);

    return {
      registered: true,
      wallet: registryWallet,
      slug: record.publicProfile.slug,
      status: record.publicProfile.status,
      referralLink: buildReferralLink(record.publicProfile.slug),
      identity: onChain.identity,
      stats: mapped.stats,
      withdrawalQueue: mapped.withdrawalQueue,
      progress: onChain.progress
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

---

## FILE: 4teen-ambassador-system :: services/allocation-worker/src/tron/controller.ts

```ts
import { FOURTEEN_CONTROLLER_CONTRACT } from "../../../../shared/config/contracts";
import type {
  AllocationExecutor,
  AllocationExecutorInput,
  AllocationExecutorResult
} from "../domain/allocation";

export interface ControllerClientConfig {
  tronWeb: any;
  contractAddress?: string;
}

export interface TronControllerAllocationExecutorConfig {
  tronWeb: any;
  controllerContractAddress?: string;
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

const TRON_HEX_ZERO_ADDRESS = "410000000000000000000000000000000000000000";

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
  const resolved = value ?? 300_000_000;

  if (!Number.isInteger(resolved) || resolved <= 0) {
    throw new Error("feeLimitSun must be a positive integer");
  }

  return resolved;
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

async function getContract(tronWeb: any, contractAddress: string): Promise<any> {
  if (!tronWeb || typeof tronWeb.contract !== "function") {
    throw new Error("Valid tronWeb instance is required");
  }

  return await tronWeb.contract().at(contractAddress);
}

export class TronControllerClient implements ControllerClient {
  private readonly tronWeb: any;
  private readonly contractAddress: string;
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
  }

  private async contract(): Promise<any> {
    if (!this.contractInstance) {
      this.contractInstance = await getContract(this.tronWeb, this.contractAddress);
    }

    return this.contractInstance;
  }

  async getAmbassadorBySlugHash(slugHash: string): Promise<ResolveAmbassadorBySlugHashResult> {
    const normalizedSlugHash = normalizeBytes32Hex(slugHash, "slugHash");
    const contract = await this.contract();

    const result = await contract.getAmbassadorBySlugHash(normalizedSlugHash).call();
    const ambassadorWallet = normalizeReturnedAddress(this.tronWeb, result);

    return {
      slugHash: normalizedSlugHash,
      ambassadorWallet
    };
  }

  async getBuyerAmbassador(buyerWallet: string): Promise<string | null> {
    const normalizedBuyerWallet = normalizeAddress(buyerWallet, "buyerWallet");
    const contract = await this.contract();

    const result = await contract.getBuyerAmbassador(normalizedBuyerWallet).call();
    return normalizeReturnedAddress(this.tronWeb, result);
  }

  async isPurchaseProcessed(purchaseId: string): Promise<boolean> {
    const normalizedPurchaseId = normalizeBytes32Hex(purchaseId, "purchaseId");
    const contract = await this.contract();

    const result = await contract.isPurchaseProcessed(normalizedPurchaseId).call();
    return Boolean(result);
  }

  async canBindBuyerToAmbassador(
    buyerWallet: string,
    ambassadorWallet: string
  ): Promise<boolean> {
    const normalizedBuyerWallet = normalizeAddress(buyerWallet, "buyerWallet");
    const normalizedAmbassadorWallet = normalizeAddress(ambassadorWallet, "ambassadorWallet");
    const contract = await this.contract();

    const result = await contract
      .canBindBuyerToAmbassador(normalizedBuyerWallet, normalizedAmbassadorWallet)
      .call();

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

    const contract = await this.contract();

    const txid = await contract
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

    return {
      txid: assertNonEmpty(txid, "txid")
    };
  }
}

export class TronControllerAllocationExecutor implements AllocationExecutor {
  private readonly client: TronControllerClient;

  constructor(config: TronControllerAllocationExecutorConfig) {
    this.client = new TronControllerClient({
      tronWeb: config.tronWeb,
      contractAddress: config.controllerContractAddress
    });
  }

  async allocate(
    input: AllocationExecutorInput
  ): Promise<AllocationExecutorResult> {
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
  raw: {
    account: any;
    resources: any;
  };
}

export interface AllocationResourcePolicy {
  minEnergyRequired: number;
  minBandwidthRequired: number;
  safetyEnergyBuffer: number;
  safetyBandwidthBuffer: number;
}

export interface AllocationResourceCheckResult {
  ok: boolean;
  address: string;
  availableEnergy: number;
  availableBandwidth: number;
  requiredEnergy: number;
  requiredBandwidth: number;
  shortEnergy: number;
  shortBandwidth: number;
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

function toAddressHex(address: string): string {
  return String(address || "").trim();
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

function sumBandwidth(resources: any, account: any): {
  freeNetLimit: number;
  freeNetUsed: number;
  netLimit: number;
  netUsed: number;
  totalLimit: number;
  totalUsed: number;
  available: number;
} {
  const freeNetLimit = toSafeNumber(account?.free_net_limit);
  const freeNetUsed = toSafeNumber(account?.free_net_used);

  const netLimit =
    toSafeNumber(account?.net_limit) ||
    toSafeNumber(resources?.NetLimit) ||
    toSafeNumber(resources?.netLimit);

  const netUsed =
    toSafeNumber(account?.net_used) ||
    toSafeNumber(resources?.NetUsed) ||
    toSafeNumber(resources?.netUsed);

  const totalLimit = freeNetLimit + netLimit;
  const totalUsed = freeNetUsed + netUsed;
  const available = Math.max(totalLimit - totalUsed, 0);

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

function sumEnergy(resources: any): {
  energyLimit: number;
  energyUsed: number;
  available: number;
} {
  const energyLimit =
    toSafeNumber(resources?.EnergyLimit) ||
    toSafeNumber(resources?.energyLimit);

  const energyUsed =
    toSafeNumber(resources?.EnergyUsed) ||
    toSafeNumber(resources?.energyUsed);

  const available = Math.max(energyLimit - energyUsed, 0);

  return {
    energyLimit,
    energyUsed,
    available
  };
}

export function buildDefaultAllocationResourcePolicy(
  overrides?: Partial<AllocationResourcePolicy>
): AllocationResourcePolicy {
  return {
    minEnergyRequired: overrides?.minEnergyRequired ?? 180_000,
    minBandwidthRequired: overrides?.minBandwidthRequired ?? 1_000,
    safetyEnergyBuffer: overrides?.safetyEnergyBuffer ?? 20_000,
    safetyBandwidthBuffer: overrides?.safetyBandwidthBuffer ?? 300
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
        : toAddressHex(normalizedAddress);

    const [account, resources] = await Promise.all([
      tronWeb.trx.getAccount(accountAddress),
      tronWeb.trx.getAccountResources(accountAddress)
    ]);

    const bandwidth = sumBandwidth(resources, account);
    const energy = sumEnergy(resources);

    return {
      address: normalizedAddress,
      bandwidth,
      energy,
      latestOperationTime: toSafeNumber(account?.latest_opration_time) || undefined,
      raw: {
        account,
        resources
      }
    };
  }

  async function checkAllocationReadiness(
    address: string,
    policy: AllocationResourcePolicy
  ): Promise<AllocationResourceCheckResult> {
    const snapshot = await getAccountResourceSnapshot(address);

    const requiredEnergy = Math.max(
      toSafeNumber(policy.minEnergyRequired) + toSafeNumber(policy.safetyEnergyBuffer),
      0
    );

    const requiredBandwidth = Math.max(
      toSafeNumber(policy.minBandwidthRequired) + toSafeNumber(policy.safetyBandwidthBuffer),
      0
    );

    const availableEnergy = snapshot.energy.available;
    const availableBandwidth = snapshot.bandwidth.available;

    const shortEnergy = Math.max(requiredEnergy - availableEnergy, 0);
    const shortBandwidth = Math.max(requiredBandwidth - availableBandwidth, 0);

    let reason: string | null = null;

    if (shortEnergy > 0 && shortBandwidth > 0) {
      reason = `Insufficient energy and bandwidth. Need +${shortEnergy} energy and +${shortBandwidth} bandwidth.`;
    } else if (shortEnergy > 0) {
      reason = `Insufficient energy. Need +${shortEnergy} energy.`;
    } else if (shortBandwidth > 0) {
      reason = `Insufficient bandwidth. Need +${shortBandwidth} bandwidth.`;
    }

    return {
      ok: !reason,
      address: snapshot.address,
      availableEnergy,
      availableBandwidth,
      requiredEnergy,
      requiredBandwidth,
      shortEnergy,
      shortBandwidth,
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

export interface GasStationClient {
  getBalance(): Promise<GasStationBalanceSnapshot>;
}

export interface GasStationClientConfig {
  endpoint: string;
  apiKey?: string;
  projectId?: string;
  timeoutMs?: number;
  staticIpProxyUrl?: string;
}

function buildGasStationHeaders(config: GasStationClientConfig): Record<string, string> {
  const headers: Record<string, string> = {
    Accept: "application/json"
  };

  if (config.apiKey) {
    headers.Authorization = `Bearer ${config.apiKey}`;
  }

  if (config.projectId) {
    headers["X-Project-Id"] = config.projectId;
  }

  return headers;
}

export function createGasStationClient(config: GasStationClientConfig): GasStationClient {
  if (!config?.endpoint?.trim()) {
    throw new Error("Gas Station endpoint is required");
  }

  const endpoint = config.endpoint.trim();
  const timeoutMs = Math.max(toSafeNumber(config.timeoutMs) || 10_000, 1_000);

  async function getBalance(): Promise<GasStationBalanceSnapshot> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(endpoint, {
        method: "GET",
        headers: buildGasStationHeaders(config),
        signal: controller.signal
      });

      const text = await response.text();

      let parsed: any = null;
      try {
        parsed = text ? JSON.parse(text) : null;
      } catch {
        parsed = { rawText: text };
      }

      if (!response.ok) {
        return {
          ok: false,
          raw: parsed
        };
      }

      const availableEnergy =
        toSafeNumber(parsed?.availableEnergy) ||
        toSafeNumber(parsed?.energy) ||
        toSafeNumber(parsed?.data?.availableEnergy) ||
        toSafeNumber(parsed?.data?.energy) ||
        undefined;

      const availableBandwidth =
        toSafeNumber(parsed?.availableBandwidth) ||
        toSafeNumber(parsed?.bandwidth) ||
        toSafeNumber(parsed?.data?.availableBandwidth) ||
        toSafeNumber(parsed?.data?.bandwidth) ||
        undefined;

      return {
        ok: true,
        availableEnergy,
        availableBandwidth,
        raw: parsed
      };
    } catch (error) {
      return {
        ok: false,
        raw: {
          message:
            error instanceof Error ? error.message : typeof error === "string" ? error : "Unknown error"
        }
      };
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    getBalance
  };
}

export interface EffectiveAllocationResourceDecision {
  ok: boolean;
  reason: string | null;
  wallet: AllocationResourceCheckResult;
  gasStation?: {
    balance: GasStationBalanceSnapshot;
    energySatisfied: boolean;
    bandwidthSatisfied: boolean;
  };
}

export async function evaluateEffectiveAllocationReadiness(params: {
  gateway: ResourceGateway;
  address: string;
  policy: AllocationResourcePolicy;
  gasStationClient?: GasStationClient;
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

  const requiredEnergy = wallet.requiredEnergy;
  const requiredBandwidth = wallet.requiredBandwidth;

  const gasEnergy = toSafeNumber(balance.availableEnergy);
  const gasBandwidth = toSafeNumber(balance.availableBandwidth);

  const energySatisfied = gasEnergy >= requiredEnergy;
  const bandwidthSatisfied = gasBandwidth >= requiredBandwidth;

  if (wallet.ok && energySatisfied && bandwidthSatisfied) {
    return {
      ok: true,
      reason: null,
      wallet,
      gasStation: {
        balance,
        energySatisfied,
        bandwidthSatisfied
      }
    };
  }

  const reasons: string[] = [];

  if (!wallet.ok && wallet.reason) {
    reasons.push(wallet.reason);
  }

  if (!energySatisfied) {
    reasons.push(
      `Gas Station reserve energy is insufficient. Need at least ${requiredEnergy}, got ${gasEnergy}.`
    );
  }

  if (!bandwidthSatisfied) {
    reasons.push(
      `Gas Station reserve bandwidth is insufficient. Need at least ${requiredBandwidth}, got ${gasBandwidth}.`
    );
  }

  return {
    ok: false,
    reason: reasons.join(" "),
    wallet,
    gasStation: {
      balance,
      energySatisfied,
      bandwidthSatisfied
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
