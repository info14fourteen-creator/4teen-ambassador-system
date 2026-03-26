# REPOSITORY: 4teen-ambassador-system
# SECTION: WORKER DOMAIN DB SERVER
# GENERATED_AT: 2026-03-26T06:50:47.719Z

## INCLUDED FILES

- services/allocation-worker/package.json
- services/allocation-worker/src/db/ambassadors.ts
- services/allocation-worker/src/db/postgres.ts
- services/allocation-worker/src/db/purchases.ts
- services/allocation-worker/src/domain/allocation.ts
- services/allocation-worker/src/domain/attribution.ts
- services/allocation-worker/src/index.ts
- services/allocation-worker/src/server.ts
- services/allocation-worker/tsconfig.json

## REPOSITORY LINK BASE

- https://raw.githubusercontent.com/info14fourteen-creator/4teen-ambassador-system/main/ai/latest/4teen-ambassador-system

---

## FILE: services/allocation-worker/package.json

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

## FILE: services/allocation-worker/src/db/ambassadors.ts

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

## FILE: services/allocation-worker/src/db/postgres.ts

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

## FILE: services/allocation-worker/src/db/purchases.ts

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

  listReplayableFailures(): Promise<PurchaseRecord[]>;

  listPendingByAmbassador(
    input: PendingPurchaseQuery
  ): Promise<PurchaseRecord[]>;

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

function normalizeAllocationMode(value?: AllocationMode): AllocationMode {
  return value ?? null;
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
    txHash: assertNonEmpty(input.txHash, "txHash").toLowerCase(),
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
    allocationMode: normalizeAllocationMode(
      normalizeOptionalString(row.allocation_mode)
    ),
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
    const normalizedTxHash = assertNonEmpty(txHash, "txHash").toLowerCase();

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

  async listReplayableFailures(): Promise<PurchaseRecord[]> {
    const result = await query(
      `
        ${buildSelectSql()}
        WHERE status IN ('deferred', 'allocation_failed_retryable')
        ORDER BY created_at ASC
      `
    );

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

  async getByPurchaseId(purchaseId: string): Promise<PurchaseRecord | null> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");
    return this.byPurchaseId.get(normalizedPurchaseId) ?? null;
  }

  async getByTxHash(txHash: string): Promise<PurchaseRecord | null> {
    const normalizedTxHash = assertNonEmpty(txHash, "txHash").toLowerCase();
    const purchaseId = this.purchaseIdByTxHash.get(normalizedTxHash);

    if (!purchaseId) {
      return null;
    }

    return this.byPurchaseId.get(purchaseId) ?? null;
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

  async listReplayableFailures(): Promise<PurchaseRecord[]> {
    return Array.from(this.byPurchaseId.values())
      .filter(
        (record) =>
          record.status === "deferred" ||
          record.status === "allocation_failed_retryable"
      )
      .sort((left, right) => left.createdAt - right.createdAt);
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
```

---

## FILE: services/allocation-worker/src/domain/allocation.ts

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

export interface ReplayFailedAllocationResult {
  status: "allocated" | "failed" | "skipped";
  purchase: PurchaseRecord;
  ambassadorWallet: string | null;
  txid: string | null;
  reason: string | null;
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
        reason: `Purchase already finalized with status: ${purchase.status}`
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
      reason: result.reason
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

## FILE: services/allocation-worker/src/domain/attribution.ts

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

## FILE: services/allocation-worker/src/index.ts

```ts
import type TronWeb from "tronweb";
import { AllocationService } from "./domain/allocation";
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
  tronWeb: TronWeb;
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

export interface AllocationWorkerProcessor {
  processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<FrontendAttributionResult>;

  processVerifiedChainEvent(
    input: ProcessChainEventInput
  ): Promise<ProcessChainEventResult>;

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
    this.logger = options.logger;
  }

  async processFrontendAttribution(
    input: FrontendAttributionInput
  ): Promise<FrontendAttributionResult> {
    const txHash = normalizeTxHash(input.txHash);
    const buyerWallet = normalizeWallet(input.buyerWallet, "buyerWallet");
    const slug = assertNonEmpty(input.slug, "slug");
    const now = input.now;
    const allocationMode = input.allocationMode ?? "eager";

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

## FILE: services/allocation-worker/src/server.ts

```ts
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

## FILE: services/allocation-worker/tsconfig.json

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
