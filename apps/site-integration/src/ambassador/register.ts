import { FOURTEEN_CONTROLLER_CONTRACT } from "../../../../shared/config/contracts";
import { buildAmbassadorRegistrationHashes } from "./hash";

declare global {
  interface Window {
    tronWeb?: any;
    tronLink?: any;
  }
}

export interface AmbassadorRegistrationResult {
  slug: string;
  slugHash: string;
  metaHash: string;
  txid: string;
  referralLink: string;
}

export interface RegisterAmbassadorInput {
  slug: string;
  backendBaseUrl: string;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

async function getTronWeb(): Promise<any> {
  const tronWeb = window.tronWeb;

  if (!tronWeb || !tronWeb.defaultAddress?.base58) {
    throw new Error("Tron wallet is not connected");
  }

  return tronWeb;
}

async function getConnectedWallet(): Promise<string> {
  const tronWeb = await getTronWeb();
  return assertNonEmpty(tronWeb.defaultAddress.base58, "wallet");
}

async function getControllerContract(): Promise<any> {
  const tronWeb = await getTronWeb();
  return tronWeb.contract().at(FOURTEEN_CONTROLLER_CONTRACT);
}

function normalizeBaseUrl(value: string): string {
  return assertNonEmpty(value, "backendBaseUrl").replace(/\/+$/, "");
}

async function ensureSlugAvailable(
  backendBaseUrl: string,
  slug: string
): Promise<void> {
  const response = await fetch(
    `${normalizeBaseUrl(backendBaseUrl)}/slug/check?slug=${encodeURIComponent(slug)}`,
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json"
      }
    }
  );

  const payload = await response.json();

  if (!response.ok || !payload?.ok) {
    throw new Error(payload?.error || "Failed to check slug availability");
  }

  if (!payload.available) {
    throw new Error("Slug is already taken");
  }
}

async function completeRegistration(
  backendBaseUrl: string,
  input: {
    slug: string;
    slugHash: string;
    wallet: string;
  }
): Promise<{ referralLink: string }> {
  const response = await fetch(
    `${normalizeBaseUrl(backendBaseUrl)}/ambassador/register-complete`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(input)
    }
  );

  const payload = await response.json();

  if (!response.ok || !payload?.ok) {
    throw new Error(payload?.error || "Failed to complete ambassador registration");
  }

  return {
    referralLink: assertNonEmpty(payload.result?.referralLink, "referralLink")
  };
}

export async function registerAmbassador(
  input: RegisterAmbassadorInput
): Promise<AmbassadorRegistrationResult> {
  const backendBaseUrl = normalizeBaseUrl(input.backendBaseUrl);
  const wallet = await getConnectedWallet();

  const hashes = buildAmbassadorRegistrationHashes({
    slug: input.slug
  });

  await ensureSlugAvailable(backendBaseUrl, hashes.slug);

  const contract = await getControllerContract();
  const txid = await contract
    .registerAsAmbassador(hashes.slugHash, hashes.metaHash)
    .send();

  const completed = await completeRegistration(backendBaseUrl, {
    slug: hashes.slug,
    slugHash: hashes.slugHash,
    wallet
  });

  return {
    slug: hashes.slug,
    slugHash: hashes.slugHash,
    metaHash: hashes.metaHash,
    txid: assertNonEmpty(txid, "txid"),
    referralLink: completed.referralLink
  };
}
