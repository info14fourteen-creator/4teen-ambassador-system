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
