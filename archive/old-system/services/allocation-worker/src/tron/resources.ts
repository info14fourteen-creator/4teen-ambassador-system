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
  trxBalanceSun: number;
  raw: {
    account: any;
    resources: any;
    bandwidthRaw: any;
    balanceSunRaw: any;
  };
}

export interface AllocationResourcePolicy {
  minEnergyRequired: number;
  minBandwidthRequired: number;
  safetyEnergyBuffer: number;
  safetyBandwidthBuffer: number;
  minEnergyOrderFloor: number;
  minBandwidthOrderFloor: number;
}

export interface AllocationResourceCheckResult {
  ok: boolean;
  address: string;
  availableEnergy: number;
  availableBandwidth: number;
  requiredEnergy: number;
  requiredBandwidth: number;
  targetEnergy: number;
  targetBandwidth: number;
  shortEnergy: number;
  shortBandwidth: number;
  energyToBuy: number;
  bandwidthToBuy: number;
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

export interface GasStationBalanceSnapshot {
  ok: boolean;
  availableEnergy?: number;
  availableBandwidth?: number;
  raw: unknown;
}

export interface GasStationBalanceReader {
  getBalance(): Promise<GasStationBalanceSnapshot>;
}

export interface EffectiveAllocationResourceDecision {
  ok: boolean;
  reason: string | null;
  wallet: AllocationResourceCheckResult;
  gasStation?: {
    balance: GasStationBalanceSnapshot;
    energySatisfied: boolean;
    bandwidthSatisfied: boolean;
    shortEnergyCovered: number;
    shortBandwidthCovered: number;
  };
}

function toSafeNumber(value: unknown): number {
  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }

  return Math.floor(parsed);
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function isHexAddress(value: string): boolean {
  return /^41[0-9a-fA-F]{40}$/.test(value);
}

function isBase58Address(value: string): boolean {
  return /^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(value);
}

function normalizeAddressFromTronWeb(tronWeb: any, address: string): string {
  const raw = assertNonEmpty(address, "address");

  if (isBase58Address(raw)) {
    return raw;
  }

  if (isHexAddress(raw)) {
    if (typeof tronWeb?.address?.fromHex === "function") {
      try {
        return tronWeb.address.fromHex(raw);
      } catch {
        return raw;
      }
    }

    return raw;
  }

  throw new Error("address must be a valid TRON address");
}

function toAccountQueryAddress(tronWeb: any, normalizedAddress: string): string {
  if (isHexAddress(normalizedAddress)) {
    return normalizedAddress;
  }

  if (typeof tronWeb?.address?.toHex === "function") {
    try {
      return tronWeb.address.toHex(normalizedAddress);
    } catch {
      return normalizedAddress;
    }
  }

  return normalizedAddress;
}

function sumBandwidth(resources: any, account: any, bandwidthRaw: any) {
  const freeNetLimit = Math.max(
    toSafeNumber(account?.freeNetLimit),
    toSafeNumber(resources?.freeNetLimit),
    toSafeNumber(account?.free_net_limit),
    toSafeNumber(resources?.free_net_limit),
    toSafeNumber(account?.freeNetLimitV2),
    toSafeNumber(resources?.freeNetLimitV2)
  );

  const freeNetUsed = Math.max(
    toSafeNumber(account?.freeNetUsed),
    toSafeNumber(resources?.freeNetUsed),
    toSafeNumber(account?.free_net_used),
    toSafeNumber(resources?.free_net_used),
    toSafeNumber(account?.freeNetUsedV2),
    toSafeNumber(resources?.freeNetUsedV2)
  );

  const netLimit = Math.max(
    toSafeNumber(account?.NetLimit),
    toSafeNumber(resources?.NetLimit),
    toSafeNumber(account?.netLimit),
    toSafeNumber(resources?.netLimit),
    toSafeNumber(account?.net_limit),
    toSafeNumber(resources?.net_limit)
  );

  const netUsed = Math.max(
    toSafeNumber(account?.NetUsed),
    toSafeNumber(resources?.NetUsed),
    toSafeNumber(account?.netUsed),
    toSafeNumber(resources?.netUsed),
    toSafeNumber(account?.net_used),
    toSafeNumber(resources?.net_used)
  );

  const totalLimit = freeNetLimit + netLimit;
  const totalUsed = freeNetUsed + netUsed;
  const calculatedAvailable = Math.max(totalLimit - totalUsed, 0);
  const available = Math.max(calculatedAvailable, toSafeNumber(bandwidthRaw));

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

function sumEnergy(resources: any, account: any) {
  const energyLimit = Math.max(
    toSafeNumber(resources?.EnergyLimit),
    toSafeNumber(account?.EnergyLimit),
    toSafeNumber(resources?.energyLimit),
    toSafeNumber(account?.energyLimit),
    toSafeNumber(resources?.energy_limit),
    toSafeNumber(account?.energy_limit)
  );

  const energyUsed = Math.max(
    toSafeNumber(resources?.EnergyUsed),
    toSafeNumber(account?.EnergyUsed),
    toSafeNumber(resources?.energyUsed),
    toSafeNumber(account?.energyUsed),
    toSafeNumber(resources?.energy_used),
    toSafeNumber(account?.energy_used)
  );

  const available = Math.max(energyLimit - energyUsed, 0);

  return {
    energyLimit,
    energyUsed,
    available
  };
}

function calculateTarget(baseRequired: number, buffer: number): number {
  return Math.max(toSafeNumber(baseRequired) + toSafeNumber(buffer), 0);
}

function buildReason(input: {
  shortEnergy: number;
  shortBandwidth: number;
}): string | null {
  const { shortEnergy, shortBandwidth } = input;

  if (shortEnergy > 0 && shortBandwidth > 0) {
    return `Insufficient energy and bandwidth. Need +${shortEnergy} energy and +${shortBandwidth} bandwidth to reach buffered target.`;
  }

  if (shortEnergy > 0) {
    return `Insufficient energy. Need +${shortEnergy} energy to reach buffered target.`;
  }

  if (shortBandwidth > 0) {
    return `Insufficient bandwidth. Need +${shortBandwidth} bandwidth to reach buffered target.`;
  }

  return null;
}

export function buildDefaultAllocationResourcePolicy(
  overrides?: Partial<AllocationResourcePolicy>
): AllocationResourcePolicy {
  return {
    minEnergyRequired: overrides?.minEnergyRequired ?? 180_000,
    minBandwidthRequired: overrides?.minBandwidthRequired ?? 1_000,
    safetyEnergyBuffer: overrides?.safetyEnergyBuffer ?? 20_000,
    safetyBandwidthBuffer: overrides?.safetyBandwidthBuffer ?? 300,
    minEnergyOrderFloor: overrides?.minEnergyOrderFloor ?? 64_400,
    minBandwidthOrderFloor: overrides?.minBandwidthOrderFloor ?? 5_000
  };
}

export function createResourceGateway(tronWeb: any): ResourceGateway {
  if (!tronWeb) {
    throw new Error("tronWeb is required");
  }

  async function getAccountResourceSnapshot(address: string): Promise<AccountResourceSnapshot> {
    const normalizedAddress = normalizeAddressFromTronWeb(tronWeb, address);
    const accountAddress = toAccountQueryAddress(tronWeb, normalizedAddress);

    const [account, resources, bandwidthRaw, balanceSunRaw] = await Promise.all([
      tronWeb.trx.getAccount(accountAddress),
      tronWeb.trx.getAccountResources(accountAddress),
      typeof tronWeb?.trx?.getBandwidth === "function"
        ? tronWeb.trx.getBandwidth(accountAddress)
        : 0,
      typeof tronWeb?.trx?.getBalance === "function"
        ? tronWeb.trx.getBalance(accountAddress)
        : 0
    ]);

    const bandwidth = sumBandwidth(resources, account, bandwidthRaw);
    const energy = sumEnergy(resources, account);

    return {
      address: normalizedAddress,
      bandwidth,
      energy,
      latestOperationTime:
        toSafeNumber(account?.latest_opration_time) ||
        toSafeNumber(account?.latestOperationTime) ||
        undefined,
      trxBalanceSun: toSafeNumber(balanceSunRaw),
      raw: {
        account,
        resources,
        bandwidthRaw,
        balanceSunRaw
      }
    };
  }

  async function checkAllocationReadiness(
    address: string,
    policy: AllocationResourcePolicy
  ): Promise<AllocationResourceCheckResult> {
    const snapshot = await getAccountResourceSnapshot(address);

    const requiredEnergy = Math.max(toSafeNumber(policy.minEnergyRequired), 0);
    const requiredBandwidth = Math.max(toSafeNumber(policy.minBandwidthRequired), 0);

    const targetEnergy = calculateTarget(requiredEnergy, policy.safetyEnergyBuffer);
    const targetBandwidth = calculateTarget(requiredBandwidth, policy.safetyBandwidthBuffer);

    const availableEnergy = snapshot.energy.available;
    const availableBandwidth = snapshot.bandwidth.available;

    const shortEnergy = Math.max(targetEnergy - availableEnergy, 0);
    const shortBandwidth = Math.max(targetBandwidth - availableBandwidth, 0);

    const energyToBuy =
      shortEnergy > 0
        ? Math.max(shortEnergy, toSafeNumber(policy.minEnergyOrderFloor))
        : 0;

    const bandwidthToBuy =
      shortBandwidth > 0
        ? Math.max(shortBandwidth, toSafeNumber(policy.minBandwidthOrderFloor))
        : 0;

    const reason = buildReason({
      shortEnergy,
      shortBandwidth
    });

    return {
      ok: reason == null,
      address: snapshot.address,
      availableEnergy,
      availableBandwidth,
      requiredEnergy,
      requiredBandwidth,
      targetEnergy,
      targetBandwidth,
      shortEnergy,
      shortBandwidth,
      energyToBuy,
      bandwidthToBuy,
      reason,
      snapshot
    };
  }

  return {
    getAccountResourceSnapshot,
    checkAllocationReadiness
  };
}

export async function evaluateEffectiveAllocationReadiness(params: {
  gateway: ResourceGateway;
  address: string;
  policy: AllocationResourcePolicy;
  gasStationClient?: GasStationBalanceReader;
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
  const gasEnergy = toSafeNumber(balance.availableEnergy);
  const gasBandwidth = toSafeNumber(balance.availableBandwidth);

  const shortEnergyCovered = Math.min(gasEnergy, wallet.shortEnergy);
  const shortBandwidthCovered = Math.min(gasBandwidth, wallet.shortBandwidth);

  const energySatisfied = wallet.shortEnergy <= 0 || gasEnergy >= wallet.shortEnergy;
  const bandwidthSatisfied = wallet.shortBandwidth <= 0 || gasBandwidth >= wallet.shortBandwidth;

  if (wallet.ok) {
    return {
      ok: true,
      reason: null,
      wallet,
      gasStation: {
        balance,
        energySatisfied: true,
        bandwidthSatisfied: true,
        shortEnergyCovered: 0,
        shortBandwidthCovered: 0
      }
    };
  }

  if (energySatisfied && bandwidthSatisfied) {
    return {
      ok: true,
      reason: null,
      wallet,
      gasStation: {
        balance,
        energySatisfied,
        bandwidthSatisfied,
        shortEnergyCovered,
        shortBandwidthCovered
      }
    };
  }

  const reasons: string[] = [];

  if (wallet.shortEnergy > 0 && !energySatisfied) {
    reasons.push(
      `Gas Station reserve energy is insufficient to cover shortfall. Need ${wallet.shortEnergy}, got ${gasEnergy}.`
    );
  }

  if (wallet.shortBandwidth > 0 && !bandwidthSatisfied) {
    reasons.push(
      `Gas Station reserve bandwidth is insufficient to cover shortfall. Need ${wallet.shortBandwidth}, got ${gasBandwidth}.`
    );
  }

  if (!reasons.length && wallet.reason) {
    reasons.push(wallet.reason);
  }

  return {
    ok: false,
    reason: reasons.join(" "),
    wallet,
    gasStation: {
      balance,
      energySatisfied,
      bandwidthSatisfied,
      shortEnergyCovered,
      shortBandwidthCovered
    }
  };
}
