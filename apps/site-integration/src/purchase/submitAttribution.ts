import { assertValidSlug } from "../../../../shared/utils/slug";

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

function normalizeWallet(wallet: string): string {
  return assertNonEmpty(wallet, "buyerWallet");
}

function normalizeTxHash(txHash: string): string {
  return assertNonEmpty(txHash, "txHash");
}

export async function submitAttribution<T = unknown>(
  payload: AttributionPayload,
  options: SubmitAttributionOptions
): Promise<SubmitAttributionResponse<T>> {
  const endpoint = assertNonEmpty(options.endpoint, "endpoint");
  const txHash = normalizeTxHash(payload.txHash);
  const buyerWallet = normalizeWallet(payload.buyerWallet);
  const slug = assertValidSlug(payload.slug);

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

  let data: T | null = null;

  const responseText = await response.text();

  if (responseText) {
    try {
      data = JSON.parse(responseText) as T;
    } catch {
      data = null;
    }
  }

  if (!response.ok) {
    throw new Error(`Attribution request failed with status ${response.status}`);
  }

  return {
    ok: true,
    status: response.status,
    data
  };
}
