"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AmbassadorDashboard,
  WithdrawResult,
  getConnectedWalletAddress,
  readAmbassadorDashboard,
  withdrawRewards
} from "../lib/blockchain/controller";

export interface AmbassadorDashboardStatusCards {
  availableOnChainSun: string;
  pendingBackendSyncSun: string;
  requestedForProcessingSun: string;
  availableOnChainCount: number;
  pendingBackendSyncCount: number;
  requestedForProcessingCount: number;
  hasAvailableOnChain: boolean;
  hasPendingBackendSync: boolean;
  hasRequestedForProcessing: boolean;
}

export interface AmbassadorDashboardState {
  wallet: string;
  dashboard: AmbassadorDashboard | null;
  statusCards: AmbassadorDashboardStatusCards;
  isConnected: boolean;
  isRegistered: boolean;
  isLoading: boolean;
  isRefreshing: boolean;
  isWithdrawing: boolean;
  hasProcessingWithdrawal: boolean;
  lastWithdrawTxid: string | null;
  error: string | null;
}

export interface UseAmbassadorDashboardResult extends AmbassadorDashboardState {
  refresh: () => Promise<void>;
  handleWithdrawRewards: () => Promise<WithdrawResult>;
  clearError: () => void;
}

const EMPTY_STATUS_CARDS: AmbassadorDashboardStatusCards = {
  availableOnChainSun: "0",
  pendingBackendSyncSun: "0",
  requestedForProcessingSun: "0",
  availableOnChainCount: 0,
  pendingBackendSyncCount: 0,
  requestedForProcessingCount: 0,
  hasAvailableOnChain: false,
  hasPendingBackendSync: false,
  hasRequestedForProcessing: false
};

const INITIAL_STATE: AmbassadorDashboardState = {
  wallet: "",
  dashboard: null,
  statusCards: EMPTY_STATUS_CARDS,
  isConnected: false,
  isRegistered: false,
  isLoading: true,
  isRefreshing: false,
  isWithdrawing: false,
  hasProcessingWithdrawal: false,
  lastWithdrawTxid: null,
  error: null
};

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

function toSafeStringNumber(value: unknown): string {
  if (typeof value === "string" && value.trim()) {
    return value.trim();
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return String(Math.trunc(value));
  }

  return "0";
}

function toSafeInteger(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return Math.trunc(value);
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed >= 0) {
      return Math.trunc(parsed);
    }
  }

  return 0;
}

function isPositiveSun(value: string): boolean {
  try {
    return BigInt(value) > 0n;
  } catch {
    return false;
  }
}

function buildStatusCards(dashboard: AmbassadorDashboard | null): AmbassadorDashboardStatusCards {
  if (!dashboard) {
    return EMPTY_STATUS_CARDS;
  }

  const availableOnChainSun = toSafeStringNumber(
    (dashboard as any).availableOnChainSun ??
      (dashboard as any).balances?.availableOnChainSun ??
      (dashboard as any).withdrawal?.availableOnChainSun
  );

  const pendingBackendSyncSun = toSafeStringNumber(
    (dashboard as any).pendingBackendSyncSun ??
      (dashboard as any).balances?.pendingBackendSyncSun ??
      (dashboard as any).withdrawal?.pendingBackendSyncSun
  );

  const requestedForProcessingSun = toSafeStringNumber(
    (dashboard as any).requestedForProcessingSun ??
      (dashboard as any).balances?.requestedForProcessingSun ??
      (dashboard as any).withdrawal?.requestedForProcessingSun
  );

  const availableOnChainCount = toSafeInteger(
    (dashboard as any).availableOnChainCount ??
      (dashboard as any).counts?.availableOnChainCount ??
      (dashboard as any).withdrawal?.availableOnChainCount
  );

  const pendingBackendSyncCount = toSafeInteger(
    (dashboard as any).pendingBackendSyncCount ??
      (dashboard as any).counts?.pendingBackendSyncCount ??
      (dashboard as any).withdrawal?.pendingBackendSyncCount
  );

  const requestedForProcessingCount = toSafeInteger(
    (dashboard as any).requestedForProcessingCount ??
      (dashboard as any).counts?.requestedForProcessingCount ??
      (dashboard as any).withdrawal?.requestedForProcessingCount
  );

  return {
    availableOnChainSun,
    pendingBackendSyncSun,
    requestedForProcessingSun,
    availableOnChainCount,
    pendingBackendSyncCount,
    requestedForProcessingCount,
    hasAvailableOnChain: isPositiveSun(availableOnChainSun) || availableOnChainCount > 0,
    hasPendingBackendSync: isPositiveSun(pendingBackendSyncSun) || pendingBackendSyncCount > 0,
    hasRequestedForProcessing:
      isPositiveSun(requestedForProcessingSun) || requestedForProcessingCount > 0
  };
}

function detectProcessingWithdrawal(
  dashboard: AmbassadorDashboard | null,
  statusCards: AmbassadorDashboardStatusCards
): boolean {
  if (!dashboard) {
    return false;
  }

  const explicitFlag =
    (dashboard as any).hasProcessingWithdrawal ??
    (dashboard as any).withdrawal?.hasProcessingWithdrawal ??
    (dashboard as any).queue?.hasProcessingWithdrawal;

  if (typeof explicitFlag === "boolean") {
    return explicitFlag;
  }

  return statusCards.hasRequestedForProcessing;
}

export function useAmbassadorDashboard(): UseAmbassadorDashboardResult {
  const [state, setState] = useState<AmbassadorDashboardState>(INITIAL_STATE);

  const load = useCallback(async (mode: "initial" | "refresh" = "initial") => {
    setState((current) => ({
      ...current,
      isLoading: mode === "initial",
      isRefreshing: mode === "refresh",
      error: null
    }));

    try {
      const wallet = await getConnectedWalletAddress();
      const dashboard = await readAmbassadorDashboard(wallet);
      const statusCards = buildStatusCards(dashboard);
      const hasProcessingWithdrawal = detectProcessingWithdrawal(dashboard, statusCards);

      setState((current) => ({
        ...current,
        wallet,
        dashboard,
        statusCards,
        hasProcessingWithdrawal,
        isConnected: true,
        isRegistered: Boolean((dashboard as any)?.identity?.exists),
        isLoading: false,
        isRefreshing: false,
        error: null
      }));
    } catch (error) {
      const message = toErrorMessage(error);

      setState((current) => ({
        ...current,
        wallet: "",
        dashboard: null,
        statusCards: EMPTY_STATUS_CARDS,
        hasProcessingWithdrawal: false,
        isConnected: false,
        isRegistered: false,
        isLoading: false,
        isRefreshing: false,
        error: message
      }));
    }
  }, []);

  useEffect(() => {
    void load("initial");
  }, [load]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleFocus = () => {
      void load("refresh");
    };

    const handleMessage = () => {
      void load("refresh");
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        void load("refresh");
      }
    };

    window.addEventListener("focus", handleFocus);
    window.addEventListener("message", handleMessage);
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      window.removeEventListener("focus", handleFocus);
      window.removeEventListener("message", handleMessage);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [load]);

  const refresh = useCallback(async () => {
    await load("refresh");
  }, [load]);

  const handleWithdrawRewards = useCallback(async (): Promise<WithdrawResult> => {
    setState((current) => ({
      ...current,
      isWithdrawing: true,
      error: null
    }));

    try {
      const result = await withdrawRewards();

      setState((current) => ({
        ...current,
        isWithdrawing: false,
        lastWithdrawTxid: result.txid,
        error: null
      }));

      await load("refresh");

      return result;
    } catch (error) {
      const message = toErrorMessage(error);

      setState((current) => ({
        ...current,
        isWithdrawing: false,
        error: message
      }));

      throw error;
    }
  }, [load]);

  const clearError = useCallback(() => {
    setState((current) => ({
      ...current,
      error: null
    }));
  }, []);

  return useMemo(
    () => ({
      ...state,
      refresh,
      handleWithdrawRewards,
      clearError
    }),
    [state, refresh, handleWithdrawRewards, clearError]
  );
}
