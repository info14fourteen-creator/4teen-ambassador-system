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

function rowToRegistryRecord(row: any): AmbassadorRegistryRecord {
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

  if (constraint.includes("slug_hash")) {
    return new Error("Slug hash is already taken");
  }

  if (constraint.includes("slug")) {
    return new Error("Slug is already taken");
  }

  if (constraint.includes("wallet")) {
    return new Error("Wallet is already registered");
  }

  return new Error("Ambassador registration conflict");
}

function buildRegistryJoinSelect(whereClause: string): string {
  return `
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
    ${whereClause}
    LIMIT 1
  `;
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
    buildRegistryJoinSelect("WHERE i.wallet = $1"),
    [normalizedWallet]
  );

  const row = result.rows[0];
  return row ? rowToRegistryRecord(row) : null;
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
