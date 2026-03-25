import { getClient, query } from "./postgres";

export type PurchaseProcessingStatus =
  | "received"
  | "verified"
  | "allocated"
  | "failed"
  | "ignored";

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
  source: "frontend-attribution" | "event-scan" | "manual-replay";
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
  source?: PurchaseRecord["source"];
  status?: PurchaseProcessingStatus;
  failureReason?: string | null;
  now?: number;
}

export interface UpdatePurchaseRecordInput {
  purchaseAmountSun?: string;
  ownerShareSun?: string;
  ambassadorSlug?: string | null;
  ambassadorWallet?: string | null;
  status?: PurchaseProcessingStatus;
  failureReason?: string | null;
  allocatedAt?: number | null;
  now?: number;
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
      now?: number;
    }
  ): Promise<PurchaseRecord>;
  markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
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
  listReplayableFailures(): Promise<PurchaseRecord[]>;
  hasProcessedPurchase(purchaseId: string): Promise<boolean>;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeWallet(value: string | null | undefined): string | null {
  if (value == null) return null;

  const normalized = String(value).trim();
  return normalized ? normalized : null;
}

function normalizeOptionalString(value: string | null | undefined): string | null {
  if (value == null) return null;

  const normalized = String(value).trim();
  return normalized ? normalized : null;
}

function normalizeSunAmount(value: string | number | bigint | undefined): string {
  if (value == null) return "0";

  const normalized = String(value).trim();

  if (!/^\d+$/.test(normalized)) {
    throw new Error("SUN amount must be a non-negative integer string");
  }

  return normalized;
}

function normalizeStatus(status?: PurchaseProcessingStatus): PurchaseProcessingStatus {
  return status ?? "received";
}

function createRecord(input: CreatePurchaseRecordInput): PurchaseRecord {
  const now = input.now ?? Date.now();
  const status = normalizeStatus(input.status);

  return {
    purchaseId: assertNonEmpty(input.purchaseId, "purchaseId"),
    txHash: assertNonEmpty(input.txHash, "txHash"),
    buyerWallet: assertNonEmpty(input.buyerWallet, "buyerWallet"),
    ambassadorSlug: normalizeOptionalString(input.ambassadorSlug ?? null),
    ambassadorWallet: normalizeWallet(input.ambassadorWallet ?? null),
    purchaseAmountSun: normalizeSunAmount(input.purchaseAmountSun),
    ownerShareSun: normalizeSunAmount(input.ownerShareSun),
    status,
    failureReason: normalizeOptionalString(input.failureReason ?? null),
    source: input.source ?? "frontend-attribution",
    createdAt: now,
    updatedAt: now,
    allocatedAt: status === "allocated" ? now : null
  };
}

function mergeRecord(
  current: PurchaseRecord,
  input: UpdatePurchaseRecordInput
): PurchaseRecord {
  const nextStatus = input.status ?? current.status;
  const nextAllocatedAt =
    input.allocatedAt !== undefined
      ? input.allocatedAt
      : nextStatus === "allocated"
        ? current.allocatedAt ?? (input.now ?? Date.now())
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
    allocatedAt: nextAllocatedAt,
    updatedAt: input.now ?? Date.now()
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
    source: String(row.source) as PurchaseRecord["source"],
    createdAt: Number(row.created_at_ms),
    updatedAt: Number(row.updated_at_ms),
    allocatedAt: row.allocated_at_ms == null ? null : Number(row.allocated_at_ms)
  };
}

function mapPgConflict(error: unknown): Error {
  if (
    error &&
    typeof error === "object" &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505"
  ) {
    const constraint =
      typeof (error as { constraint?: unknown }).constraint === "string"
        ? (error as { constraint: string }).constraint
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
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      allocated_at TIMESTAMPTZ NULL
    )
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
}

export class PostgresPurchaseStore implements PurchaseStore {
  async getByPurchaseId(purchaseId: string): Promise<PurchaseRecord | null> {
    const normalizedPurchaseId = assertNonEmpty(purchaseId, "purchaseId");

    const result = await query(
      `
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
          FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
          FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms,
          CASE
            WHEN allocated_at IS NULL THEN NULL
            ELSE FLOOR(EXTRACT(EPOCH FROM allocated_at) * 1000)
          END AS allocated_at_ms
        FROM purchases
        WHERE purchase_id = $1
        LIMIT 1
      `,
      [normalizedPurchaseId]
    );

    const row = result.rows[0];
    return row ? rowToPurchaseRecord(row) : null;
  }

  async getByTxHash(txHash: string): Promise<PurchaseRecord | null> {
    const normalizedTxHash = assertNonEmpty(txHash, "txHash");

    const result = await query(
      `
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
          FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
          FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms,
          CASE
            WHEN allocated_at IS NULL THEN NULL
            ELSE FLOOR(EXTRACT(EPOCH FROM allocated_at) * 1000)
          END AS allocated_at_ms
        FROM purchases
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
            created_at,
            updated_at,
            allocated_at
          )
          VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            TO_TIMESTAMP($11 / 1000.0),
            TO_TIMESTAMP($12 / 1000.0),
            CASE WHEN $13::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($13 / 1000.0) END
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
          updated_at = TO_TIMESTAMP($8 / 1000.0),
          allocated_at = CASE WHEN $9::BIGINT IS NULL THEN NULL ELSE TO_TIMESTAMP($9 / 1000.0) END
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
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      status: "verified",
      failureReason: null
    };

    if (input.ambassadorSlug !== undefined) {
      updateInput.ambassadorSlug = input.ambassadorSlug;
    }

    if (input.ambassadorWallet !== undefined) {
      updateInput.ambassadorWallet = input.ambassadorWallet;
    }

    if (input.now !== undefined) {
      updateInput.now = input.now;
    }

    return this.update(purchaseId, updateInput);
  }

  async markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      status: "allocated",
      failureReason: null,
      allocatedAt: input?.now ?? Date.now()
    };

    if (input?.ambassadorWallet !== undefined) {
      updateInput.ambassadorWallet = input.ambassadorWallet;
    }

    if (input?.now !== undefined) {
      updateInput.now = input.now;
    }

    return this.update(purchaseId, updateInput);
  }

  async markFailed(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      status: "failed",
      failureReason: assertNonEmpty(reason, "reason")
    };

    if (now !== undefined) {
      updateInput.now = now;
    }

    return this.update(purchaseId, updateInput);
  }

  async markIgnored(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      status: "ignored",
      failureReason: assertNonEmpty(reason, "reason")
    };

    if (now !== undefined) {
      updateInput.now = now;
    }

    return this.update(purchaseId, updateInput);
  }

  async listReplayableFailures(): Promise<PurchaseRecord[]> {
    const result = await query(
      `
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
          FLOOR(EXTRACT(EPOCH FROM created_at) * 1000) AS created_at_ms,
          FLOOR(EXTRACT(EPOCH FROM updated_at) * 1000) AS updated_at_ms,
          CASE
            WHEN allocated_at IS NULL THEN NULL
            ELSE FLOOR(EXTRACT(EPOCH FROM allocated_at) * 1000)
          END AS allocated_at_ms
        FROM purchases
        WHERE status = 'failed'
        ORDER BY updated_at DESC
      `
    );

    return result.rows.map(rowToPurchaseRecord);
  }

  async hasProcessedPurchase(purchaseId: string): Promise<boolean> {
    const record = await this.getByPurchaseId(purchaseId);

    if (!record) {
      return false;
    }

    return record.status === "allocated" || record.status === "ignored";
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
    const normalizedTxHash = assertNonEmpty(txHash, "txHash");
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
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      status: "verified",
      failureReason: null
    };

    if (input.ambassadorSlug !== undefined) {
      updateInput.ambassadorSlug = input.ambassadorSlug;
    }

    if (input.ambassadorWallet !== undefined) {
      updateInput.ambassadorWallet = input.ambassadorWallet;
    }

    if (input.now !== undefined) {
      updateInput.now = input.now;
    }

    return this.update(purchaseId, updateInput);
  }

  async markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      status: "allocated",
      failureReason: null,
      allocatedAt: input?.now ?? Date.now()
    };

    if (input?.ambassadorWallet !== undefined) {
      updateInput.ambassadorWallet = input.ambassadorWallet;
    }

    if (input?.now !== undefined) {
      updateInput.now = input.now;
    }

    return this.update(purchaseId, updateInput);
  }

  async markFailed(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      status: "failed",
      failureReason: assertNonEmpty(reason, "reason")
    };

    if (now !== undefined) {
      updateInput.now = now;
    }

    return this.update(purchaseId, updateInput);
  }

  async markIgnored(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      status: "ignored",
      failureReason: assertNonEmpty(reason, "reason")
    };

    if (now !== undefined) {
      updateInput.now = now;
    }

    return this.update(purchaseId, updateInput);
  }

  async listReplayableFailures(): Promise<PurchaseRecord[]> {
    return Array.from(this.byPurchaseId.values()).filter(
      (record) => record.status === "failed"
    );
  }

  async hasProcessedPurchase(purchaseId: string): Promise<boolean> {
    const record = await this.getByPurchaseId(purchaseId);

    if (!record) {
      return false;
    }

    return record.status === "allocated" || record.status === "ignored";
  }
}
