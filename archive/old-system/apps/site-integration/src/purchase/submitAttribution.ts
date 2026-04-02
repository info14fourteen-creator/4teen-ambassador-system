import { assertValidSlug, normalizeSlug } from "../../../../shared/utils/slug";

export interface AttributionPayload {
  txHash: string;
  buyerWallet: string;
  slug: string;
}

export interface SubmitAttributionOptions {
  endpoint: string;
  signal?: AbortSignal;
  headers?: Record<string, string>;
  fetchImpl?: typeof fetch;
}

export interface SubmitAttributionResponse<T = unknown> {
  ok: boolean;
  status: number;
  data: T | null;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeEndpoint(endpoint: string): string {
  return assertNonEmpty(endpoint, "endpoint").replace(/\/+$/, "");
}

function normalizeWallet(wallet: string): string {
  return assertNonEmpty(wallet, "buyerWallet");
}

function normalizeTxHash(txHash: string): string {
  return assertNonEmpty(txHash, "txHash");
}

function normalizeAttributionSlug(slug: string): string {
  return assertValidSlug(normalizeSlug(assertNonEmpty(slug, "slug")));
}

async function parseResponseBody<T>(response: Response): Promise<T | null> {
  const responseText = await response.text();

  if (!responseText) {
    return null;
  }

  try {
    return JSON.parse(responseText) as T;
  } catch {
    return null;
  }
}

function extractErrorMessage(data: unknown, status: number): string {
  if (
    data &&
    typeof data === "object" &&
    "error" in data &&
    typeof (data as { error?: unknown }).error === "string" &&
    (data as { error: string }).error.trim()
  ) {
    return (data as { error: string }).error.trim();
  }

  return `Attribution request failed with status ${status}`;
}

export async function submitAttribution<T = unknown>(
  payload: AttributionPayload,
  options: SubmitAttributionOptions
): Promise<SubmitAttributionResponse<T>> {
  const endpoint = normalizeEndpoint(options.endpoint);
  const txHash = normalizeTxHash(payload.txHash);
  const buyerWallet = normalizeWallet(payload.buyerWallet);
  const slug = normalizeAttributionSlug(payload.slug);

  const fetchImpl = options.fetchImpl ?? fetch;

  if (typeof fetchImpl !== "function") {
    throw new Error("Fetch implementation is not available");
  }

  const response = await fetchImpl(endpoint, {
    method: "POST",
    signal: options.signal,
    headers: {
      "content-type": "application/json",
      ...(options.headers ?? {})
    },
    body: JSON.stringify({
      txHash,
      buyerWallet,
      slug
    })
  });

  const data = await parseResponseBody<T>(response);

  if (!response.ok) {
    throw new Error(extractErrorMessage(data, response.status));
  }

  return {
    ok: true,
    status: response.status,
    data
  };
}
