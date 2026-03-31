"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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

function isWalletConnectionError(message: string): boolean {
  const normalized = message.toLowerCase();

  return (
    normalized.includes("tron wallet is not connected") ||
    normalized.includes("wallet is not connected") ||
    normalized.includes("not connected") ||
    normalized.includes("no wallet") ||
    normalized.includes("browser environment is required")
  );
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
  const allocatedInDbSun = safeSun(withdrawalQueue.allocatedInDbSun);
  const pendingBackendSyncSun = safeSun(withdrawalQueue.pendingBackendSyncSun);
  const requestedForProcessingSun = safeSun(withdrawalQueue.requestedForProcessingSun);

  const availableOnChainCount = safeCount(withdrawalQueue.availableOnChainCount);
  const allocatedInDbCount = safeCount(withdrawalQueue.allocatedInDbCount);
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

function buildDerivedState(
  wallet: string,
  dashboard: AmbassadorDashboard
): Pick<
  AmbassadorDashboardState,
  "wallet" | "dashboard" | "statusCards" | "hasProcessingWithdrawal" | "isConnected" | "isRegistered"
> {
  const statusCards = buildStatusCards(dashboard.withdrawalQueue);
  const hasProcessingWithdrawal = detectProcessingWithdrawal(dashboard.withdrawalQueue);
  const identityExists = Boolean(dashboard.identity?.exists);
  const hasSomeIdentityData =
    Boolean(dashboard.identity?.slugHash) &&
    dashboard.identity.slugHash !==
      "0x0000000000000000000000000000000000000000000000000000000000000000";

  return {
    wallet,
    dashboard,
    statusCards,
    hasProcessingWithdrawal,
    isConnected: true,
    isRegistered: identityExists || hasSomeIdentityData
  };
}

export function useAmbassadorDashboard(): UseAmbassadorDashboardResult {
  const [state, setState] = useState<AmbassadorDashboardState>(INITIAL_STATE);
  const requestIdRef = useRef(0);
  const mountedRef = useRef(true);

  useEffect(() => {
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const load = useCallback(async (mode: "initial" | "refresh" = "initial") => {
    const requestId = ++requestIdRef.current;

    setState((current) => ({
      ...current,
      isLoading: mode === "initial" && current.dashboard == null,
      isRefreshing: mode === "refresh",
      error: null
    }));

    try {
      const wallet = await getConnectedWalletAddress();
      const dashboard = await readAmbassadorDashboard(wallet);

      if (!mountedRef.current || requestId !== requestIdRef.current) {
        return;
      }

      const derived = buildDerivedState(wallet, dashboard);

      setState((current) => ({
        ...current,
        ...derived,
        isLoading: false,
        isRefreshing: false,
        error: null
      }));
    } catch (error) {
      if (!mountedRef.current || requestId !== requestIdRef.current) {
        return;
      }

      const message = toErrorMessage(error);

      setState((current) => {
        const walletDisconnected = isWalletConnectionError(message);

        if (mode === "refresh") {
          if (walletDisconnected) {
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
          }

          return {
            ...current,
            isLoading: false,
            isRefreshing: false,
            error: message
          };
        }

        if (walletDisconnected) {
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
        }

        return {
          ...current,
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

      if (!mountedRef.current) {
        return result;
      }

      setState((current) => ({
        ...current,
        isWithdrawing: false,
        lastWithdrawTxid: result.txid,
        error: null
      }));

      await load("refresh");

      return result;
    } catch (error) {
      if (!mountedRef.current) {
        throw error;
      }

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
