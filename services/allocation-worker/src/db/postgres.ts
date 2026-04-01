import { Pool, PoolClient, QueryResult, QueryResultRow } from "pg";

let pool: Pool | null = null;

function assertNonEmpty(value: string | undefined, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function parseBoolean(value: string | undefined, fallback: boolean): boolean {
  if (value == null || String(value).trim() === "") {
    return fallback;
  }

  const normalized = String(value).trim().toLowerCase();

  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }

  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }

  throw new Error(`Invalid boolean value: ${value}`);
}

function shouldUseSsl(): boolean {
  if (process.env.PGSSLMODE?.trim().toLowerCase() === "disable") {
    return false;
  }

  if (process.env.DATABASE_SSL?.trim().toLowerCase() === "false") {
    return false;
  }

  if (process.env.NODE_ENV === "test" && !process.env.DATABASE_URL) {
    return false;
  }

  return true;
}

export function getDatabaseUrl(): string {
  return assertNonEmpty(process.env.DATABASE_URL, "DATABASE_URL");
}

export function getPool(): Pool {
  if (pool) {
    return pool;
  }

  const sslEnabled = shouldUseSsl();
  const rejectUnauthorized = parseBoolean(
    process.env.DATABASE_SSL_REJECT_UNAUTHORIZED,
    false
  );

  pool = new Pool({
    connectionString: getDatabaseUrl(),
    max: Number(process.env.PG_POOL_MAX || 10),
    idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30_000),
    connectionTimeoutMillis: Number(process.env.PG_CONNECT_TIMEOUT_MS || 10_000),
    ssl: sslEnabled
      ? {
          rejectUnauthorized
        }
      : false
  });

  pool.on("error", (error) => {
    console.error(
      JSON.stringify({
        level: "error",
        scope: "postgres",
        stage: "pool-error",
        error: error?.message || "Unknown pool error"
      })
    );
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

  const currentPool = pool;
  pool = null;
  await currentPool.end();
}
