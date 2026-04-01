import { createGasStationClientFromEnv } from "../services/gasStation";

function assertNonEmpty(value: string | undefined, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
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

function getTargetEnergy(): number {
  const raw = Number(process.env.GASSTATION_TARGET_ENERGY || "220000");
  return Number.isFinite(raw) && raw >= 64_400 ? Math.ceil(raw) : 220_000;
}

function getServiceChargeType(): string {
  return String(process.env.GASSTATION_SERVICE_CHARGE_TYPE || "10010").trim() || "10010";
}

export interface RentEnergyJobResult {
  ok: boolean;
  stage: "checked-balance" | "order-created" | "skipped" | "failed";
  gasBalance: string | null;
  tradeNo: string | null;
  reason: string | null;
}

export async function rentDailyEnergy(): Promise<RentEnergyJobResult> {
  try {
    const client = createGasStationClientFromEnv();

    const receiveAddress = assertNonEmpty(
      process.env.TRON_RESOURCE_ADDRESS || process.env.CONTROLLER_OWNER_WALLET,
      "TRON_RESOURCE_ADDRESS"
    );

    const balance = await client.getBalance();
    const gasBalance = String(balance.balance || "").trim() || "0";

    const targetEnergy = getTargetEnergy();
    const serviceChargeType = getServiceChargeType();

    if (Number(gasBalance) <= 0) {
      return {
        ok: false,
        stage: "checked-balance",
        gasBalance,
        tradeNo: null,
        reason: "GasStation balance is empty"
      };
    }

    const requestId = `energy-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

    const order = await client.createEnergyOrder({
      requestId,
      receiveAddress,
      energyNum: targetEnergy,
      serviceChargeType
    });

    return {
      ok: true,
      stage: "order-created",
      gasBalance,
      tradeNo: order.trade_no,
      reason: null
    };
  } catch (error) {
    return {
      ok: false,
      stage: "failed",
      gasBalance: null,
      tradeNo: null,
      reason: toErrorMessage(error)
    };
  }
}
