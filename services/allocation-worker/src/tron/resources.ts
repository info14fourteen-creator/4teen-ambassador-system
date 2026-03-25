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
