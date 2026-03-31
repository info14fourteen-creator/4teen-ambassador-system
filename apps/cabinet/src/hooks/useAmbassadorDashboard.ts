"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AmbassadorDashboard,
  AmbassadorWithdrawalQueue,
  WithdrawResult,
  getConnectedWalletAddress,
  readAmbassadorDashboard,
  withdrawRewards
} from "../lib/blockchain/controller";

export interface AmbassadorDashboardStatusCards {
  availableOnChainSun: string;
  allocatedInDbSun: string;
  pendingBackendSyncSun: string;
  requestedForProcessingSun: string;

  availableOnChainCount: number;
  allocatedInDbCount: number;
  pendingBackendSyncCount: number;
  requestedForProcessingCount: number;

  hasAvailableOnChain: boolean;
  hasAllocatedInDb: boolean;
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
  allocatedInDbSun: "0",
  pendingBackendSyncSun: "0",
  requestedForProcessingSun: "0",

  availableOnChainCount: 0,
  allocatedInDbCount: 0,
  pendingBackendSyncCount: 0,
  requestedForProcessingCount: 0,

  hasAvailableOnChain: false,
  hasAllocatedInDb: false,
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

function isPositiveSun(value: string): boolean {
  try {
    return BigInt(value) > 0n;
  } catch {
    return false;
  }
}

function safeSun(value: unknown): string {
  const raw = String(value ?? "0").trim();
  return /^\d+$/.test(raw) ? raw : "0";
}

function safeCount(value: unknown): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 0;
}

function buildStatusCards(
  withdrawalQueue: AmbassadorWithdrawalQueue | null | undefined
): AmbassadorDashboardStatusCards {
  if (!withdrawalQueue) {
    return EMPTY_STATUS_CARDS;
  }

  const availableOnChainSun = safeSun(withdrawalQueue.availableOnChainSun);
  const allocatedInDbSun = safeSun((withdrawalQueue as any).allocatedInDbSun);
  const pendingBackendSyncSun = safeSun(withdrawalQueue.pendingBackendSyncSun);
  const requestedForProcessingSun = safeSun(withdrawalQueue.requestedForProcessingSun);

  const availableOnChainCount = safeCount(withdrawalQueue.availableOnChainCount);
  const allocatedInDbCount = safeCount((withdrawalQueue as any).allocatedInDbCount);
  const pendingBackendSyncCount = safeCount(withdrawalQueue.pendingBackendSyncCount);
  const requestedForProcessingCount = safeCount(withdrawalQueue.requestedForProcessingCount);

  return {
    availableOnChainSun,
    allocatedInDbSun,
    pendingBackendSyncSun,
    requestedForProcessingSun,

    availableOnChainCount,
    allocatedInDbCount,
    pendingBackendSyncCount,
    requestedForProcessingCount,

    hasAvailableOnChain:
      isPositiveSun(availableOnChainSun) || availableOnChainCount > 0,

    hasAllocatedInDb:
      isPositiveSun(allocatedInDbSun) || allocatedInDbCount > 0,

    hasPendingBackendSync:
      isPositiveSun(pendingBackendSyncSun) || pendingBackendSyncCount > 0,

    hasRequestedForProcessing:
      isPositiveSun(requestedForProcessingSun) ||
      requestedForProcessingCount > 0
  };
}

function detectProcessingWithdrawal(
  withdrawalQueue: AmbassadorWithdrawalQueue | null | undefined
): boolean {
  if (!withdrawalQueue) {
    return false;
  }

  return Boolean(withdrawalQueue.hasProcessingWithdrawal);
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
      const statusCards = buildStatusCards(dashboard.withdrawalQueue);
      const hasProcessingWithdrawal = detectProcessingWithdrawal(
        dashboard.withdrawalQueue
      );

      setState((current) => ({
        ...current,
        wallet,
        dashboard,
        statusCards,
        hasProcessingWithdrawal,
        isConnected: true,
        isRegistered: Boolean((dashboard.identity as any)?.exists),
        isLoading: false,
        isRefreshing: false,
        error: null
      }));
    } catch (error) {
      const message = toErrorMessage(error);

      setState((current) => {
        if (mode === "refresh") {
          return {
            ...current,
            isLoading: false,
            isRefreshing: false,
            error: message
          };
        }

        return {
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
        };
      });
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
