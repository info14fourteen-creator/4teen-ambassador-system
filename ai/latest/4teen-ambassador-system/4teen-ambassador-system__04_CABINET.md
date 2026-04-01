# 4teen-ambassador-system — CABINET

Generated: 2026-04-01T13:04:40.820Z
Repository: info14fourteen-creator/4teen-ambassador-system
Branch: main

## Included files

- 4teen-ambassador-system :: apps/cabinet/src/app/ambassador/page.tsx
- 4teen-ambassador-system :: apps/cabinet/src/hooks/useAmbassadorDashboard.ts
- 4teen-ambassador-system :: apps/cabinet/src/lib/blockchain/controller.ts
- 4teen-ambassador-system :: apps/cabinet/src/lib/referral/storage.ts
- 4teen-ambassador-system :: apps/cabinet/src/lib/telegram/link.ts

---

## FILE PATH

`apps/cabinet/src/app/ambassador/page.tsx`

## FILE CONTENT

```tsx
"use client";

import { useMemo } from "react";
import { buildTronscanTransactionUrl } from "../../../../../shared/config/contracts";
import { useAmbassadorDashboard } from "../../hooks/useAmbassadorDashboard";
import { levelToLabel, sunToTrxString } from "../../lib/blockchain/controller";
import { buildWalletExplorerUrl } from "../../lib/telegram/link";

function ValueCard({
  label,
  value,
  hint
}: {
  label: string;
  value: string;
  hint?: string;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
      <div className="text-xs uppercase tracking-[0.18em] text-white/45">{label}</div>
      <div className="mt-2 break-words text-2xl font-semibold text-white">{value}</div>
      {hint ? <div className="mt-1 text-sm text-white/45">{hint}</div> : null}
    </div>
  );
}

function StatusCard({
  label,
  trxValue,
  sunValue,
  count,
  accentClass,
  hint
}: {
  label: string;
  trxValue: string;
  sunValue: string;
  count: number;
  accentClass: string;
  hint?: string;
}) {
  return (
    <div className={`rounded-2xl border p-4 ${accentClass}`}>
      <div className="text-xs uppercase tracking-[0.18em] text-white/55">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-white">{trxValue} TRX</div>
      <div className="mt-1 text-sm text-white/55">{sunValue} SUN</div>
      <div className="mt-3 text-sm text-white/65">
        {count} {count === 1 ? "purchase" : "purchases"}
      </div>
      {hint ? <div className="mt-2 text-xs text-white/45">{hint}</div> : null}
    </div>
  );
}

function ActionButton({
  children,
  onClick,
  disabled,
  variant = "primary"
}: {
  children: React.ReactNode;
  onClick?: () => void | Promise<void>;
  disabled?: boolean;
  variant?: "primary" | "secondary";
}) {
  const className =
    variant === "primary"
      ? "rounded-2xl bg-[#ff6900] px-4 py-3 text-sm font-semibold text-white transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-50"
      : "rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-semibold text-white transition hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-50";

  return (
    <button
      type="button"
      onClick={() => {
        void onClick?.();
      }}
      disabled={disabled}
      className={className}
    >
      {children}
    </button>
  );
}

function formatDate(timestamp: number): string {
  if (!timestamp) return "—";

  try {
    const normalized = timestamp > 1_000_000_000_000 ? timestamp : timestamp * 1000;
    const date = new Date(normalized);

    if (!Number.isFinite(date.getTime())) {
      return "—";
    }

    return date.toLocaleString();
  } catch {
    return "—";
  }
}

function buildPrimaryActionLabel(params: {
  isRegistered: boolean;
  isWithdrawing: boolean;
  hasProcessingWithdrawal: boolean;
  hasAvailableOnChain: boolean;
  hasRequestedForProcessing: boolean;
}): string {
  const {
    isRegistered,
    isWithdrawing,
    hasProcessingWithdrawal,
    hasAvailableOnChain,
    hasRequestedForProcessing
  } = params;

  if (!isRegistered) {
    return "Ambassador profile required";
  }

  if (isWithdrawing) {
    return "Processing withdrawal...";
  }

  if (hasProcessingWithdrawal || hasRequestedForProcessing) {
    return "Requested for processing";
  }

  if (hasAvailableOnChain) {
    return "Withdraw rewards";
  }

  return "No on-chain rewards available";
}

function buildPrimaryActionHint(params: {
  hasAvailableOnChain: boolean;
  hasAllocatedInDb: boolean;
  hasPendingBackendSync: boolean;
  hasRequestedForProcessing: boolean;
}): string {
  const {
    hasAvailableOnChain,
    hasAllocatedInDb,
    hasPendingBackendSync,
    hasRequestedForProcessing
  } = params;

  if (hasRequestedForProcessing) {
    return "A withdrawal request is already in progress. Wait for backend processing to finish before trying again.";
  }

  if (hasAvailableOnChain && hasPendingBackendSync) {
    return "Part of your rewards is withdrawable on-chain now, and another part is still waiting for backend sync.";
  }

  if (hasAvailableOnChain) {
    return "These rewards are really available on-chain and can be withdrawn now.";
  }

  if (hasPendingBackendSync) {
    return "You have verified rewards in the backend queue, but they are not withdrawable on-chain yet.";
  }

  if (hasAllocatedInDb) {
    return "Some purchases are already allocated in backend accounting, but that does not mean they are withdrawable on-chain yet.";
  }

  return "No rewards are currently available for on-chain withdrawal.";
}

export default function AmbassadorPage() {
  const {
    wallet,
    dashboard,
    statusCards,
    isConnected,
    isRegistered,
    isLoading,
    isRefreshing,
    isWithdrawing,
    hasProcessingWithdrawal,
    error,
    lastWithdrawTxid,
    refresh,
    handleWithdrawRewards
  } = useAmbassadorDashboard();

  const identity = dashboard?.identity ?? null;
  const stats = dashboard?.stats ?? null;
  const progress = dashboard?.progress ?? null;
  const withdrawalQueue = dashboard?.withdrawalQueue ?? null;

  const walletExplorerUrl = useMemo(() => {
    if (!wallet) return "";
    return buildWalletExplorerUrl(wallet);
  }, [wallet]);

  const withdrawExplorerUrl = useMemo(() => {
    if (!lastWithdrawTxid) return "";
    return buildTronscanTransactionUrl(lastWithdrawTxid);
  }, [lastWithdrawTxid]);

  const effectiveLevel = identity?.effectiveLevel ?? identity?.level ?? 0;
  const currentLevel = progress?.currentLevel ?? identity?.currentLevel ?? effectiveLevel;

  const trackedVolumeSun = stats?.trackedVolumeSun ?? "0";
  const trackedVolumeTrx = stats?.trackedVolumeTrx ?? sunToTrxString(trackedVolumeSun);

  const claimableRewardsSun =
    withdrawalQueue?.availableOnChainSun ??
    stats?.claimableRewardsSun ??
    "0";
  const claimableRewardsTrx =
    withdrawalQueue?.availableOnChainTrx ??
    stats?.claimableRewardsTrx ??
    sunToTrxString(claimableRewardsSun);

  const lifetimeRewardsSun = stats?.lifetimeRewardsSun ?? "0";
  const lifetimeRewardsTrx =
    stats?.lifetimeRewardsTrx ?? sunToTrxString(lifetimeRewardsSun);

  const withdrawnRewardsSun = stats?.withdrawnRewardsSun ?? "0";
  const withdrawnRewardsTrx =
    stats?.withdrawnRewardsTrx ?? sunToTrxString(withdrawnRewardsSun);

  const allocatedInDbSun = withdrawalQueue?.allocatedInDbSun ?? "0";
  const allocatedInDbTrx =
    withdrawalQueue?.allocatedInDbTrx ?? sunToTrxString(allocatedInDbSun);

  const availableOnChainSun = withdrawalQueue?.availableOnChainSun ?? "0";
  const availableOnChainTrx =
    withdrawalQueue?.availableOnChainTrx ?? sunToTrxString(availableOnChainSun);

  const pendingBackendSyncSun = withdrawalQueue?.pendingBackendSyncSun ?? "0";
  const pendingBackendSyncTrx =
    withdrawalQueue?.pendingBackendSyncTrx ?? sunToTrxString(pendingBackendSyncSun);

  const requestedForProcessingSun = withdrawalQueue?.requestedForProcessingSun ?? "0";
  const requestedForProcessingTrx =
    withdrawalQueue?.requestedForProcessingTrx ?? sunToTrxString(requestedForProcessingSun);

  const primaryActionLabel = useMemo(
    () =>
      buildPrimaryActionLabel({
        isRegistered,
        isWithdrawing,
        hasProcessingWithdrawal,
        hasAvailableOnChain: statusCards.hasAvailableOnChain,
        hasRequestedForProcessing: statusCards.hasRequestedForProcessing
      }),
    [
      isRegistered,
      isWithdrawing,
      hasProcessingWithdrawal,
      statusCards.hasAvailableOnChain,
      statusCards.hasRequestedForProcessing
    ]
  );

  const primaryActionHint = useMemo(
    () =>
      buildPrimaryActionHint({
        hasAvailableOnChain: statusCards.hasAvailableOnChain,
        hasAllocatedInDb: statusCards.hasAllocatedInDb,
        hasPendingBackendSync: statusCards.hasPendingBackendSync,
        hasRequestedForProcessing: statusCards.hasRequestedForProcessing
      }),
    [
      statusCards.hasAvailableOnChain,
      statusCards.hasAllocatedInDb,
      statusCards.hasPendingBackendSync,
      statusCards.hasRequestedForProcessing
    ]
  );

  const withdrawDisabled = useMemo(() => {
    if (!isRegistered) return true;
    if (isWithdrawing) return true;
    if (hasProcessingWithdrawal) return true;
    if (statusCards.hasRequestedForProcessing) return true;
    return !statusCards.hasAvailableOnChain;
  }, [
    isRegistered,
    isWithdrawing,
    hasProcessingWithdrawal,
    statusCards.hasRequestedForProcessing,
    statusCards.hasAvailableOnChain
  ]);

  if (isLoading) {
    return (
      <main className="min-h-screen bg-[#111] px-6 py-10 text-white">
        <div className="mx-auto max-w-6xl">
          <div className="rounded-3xl border border-white/10 bg-white/5 p-6">
            Loading ambassador dashboard...
          </div>
        </div>
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-[#111] px-6 py-10 text-white">
      <div className="mx-auto max-w-6xl space-y-6">
        <section className="rounded-3xl border border-white/10 bg-white/5 p-6">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <div className="text-xs uppercase tracking-[0.18em] text-white/45">
                4TEEN Ambassador Cabinet
              </div>
              <h1 className="mt-2 text-3xl font-semibold">Your ambassador dashboard</h1>
              <p className="mt-2 max-w-3xl text-sm text-white/60">
                Track your ambassador profile, level, buyer activity, backend reward
                accounting, and the real on-chain amount available for withdrawal.
              </p>
            </div>

            <div className="flex flex-wrap gap-3">
              <ActionButton
                onClick={refresh}
                disabled={isRefreshing || isWithdrawing}
                variant="secondary"
              >
                {isRefreshing ? "Refreshing..." : "Refresh"}
              </ActionButton>

              <ActionButton onClick={handleWithdrawRewards} disabled={withdrawDisabled}>
                {primaryActionLabel}
              </ActionButton>
            </div>
          </div>

          <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4 text-sm text-white/65">
            {primaryActionHint}
          </div>

          {error ? (
            <div className="mt-4 rounded-2xl border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-200">
              {error}
            </div>
          ) : null}
        </section>

        <section className="grid gap-4 md:grid-cols-4">
          <StatusCard
            label="Available on-chain now"
            trxValue={availableOnChainTrx}
            sunValue={availableOnChainSun}
            count={withdrawalQueue?.availableOnChainCount ?? 0}
            accentClass="border-emerald-500/20 bg-emerald-500/10"
            hint="Real withdrawable amount from contract state."
          />

          <StatusCard
            label="Allocated in DB"
            trxValue={allocatedInDbTrx}
            sunValue={allocatedInDbSun}
            count={withdrawalQueue?.allocatedInDbCount ?? 0}
            accentClass="border-violet-500/20 bg-violet-500/10"
            hint="Backend accounting only. Not guaranteed withdrawable now."
          />

          <StatusCard
            label="Pending backend sync"
            trxValue={pendingBackendSyncTrx}
            sunValue={pendingBackendSyncSun}
            count={withdrawalQueue?.pendingBackendSyncCount ?? 0}
            accentClass="border-amber-500/20 bg-amber-500/10"
            hint="Verified rewards that still need backend and on-chain sync."
          />

          <StatusCard
            label="Requested for processing"
            trxValue={requestedForProcessingTrx}
            sunValue={requestedForProcessingSun}
            count={withdrawalQueue?.requestedForProcessingCount ?? 0}
            accentClass="border-sky-500/20 bg-sky-500/10"
            hint="Already included in withdrawal preparation or processing queue."
          />
        </section>

        <section className="grid gap-4 md:grid-cols-3">
          <ValueCard
            label="Wallet"
            value={isConnected ? "Connected" : "Not connected"}
            hint={wallet || "Connect TronLink to continue"}
          />
          <ValueCard
            label="Ambassador status"
            value={isRegistered ? "Registered" : "Not registered"}
            hint={
              identity
                ? `${identity.active ? "Active" : "Inactive"} • ${levelToLabel(effectiveLevel)}`
                : "No ambassador profile found"
            }
          />
          <ValueCard
            label="Reward percent"
            value={`${identity?.rewardPercent ?? 0}%`}
            hint={`Effective level: ${levelToLabel(effectiveLevel)}`}
          />
        </section>

        <section className="grid gap-4 md:grid-cols-3">
          <ValueCard label="Total buyers" value={String(stats?.totalBuyers ?? 0)} />
          <ValueCard
            label="Tracked volume"
            value={`${trackedVolumeTrx} TRX`}
            hint={`${trackedVolumeSun} SUN`}
          />
          <ValueCard
            label="Claimable rewards now"
            value={`${claimableRewardsTrx} TRX`}
            hint={`${claimableRewardsSun} SUN • Source: on-chain`}
          />
        </section>

        <section className="grid gap-4 md:grid-cols-3">
          <ValueCard
            label="Lifetime rewards"
            value={`${lifetimeRewardsTrx} TRX`}
            hint={`${lifetimeRewardsSun} SUN`}
          />
          <ValueCard
            label="Withdrawn rewards"
            value={`${withdrawnRewardsTrx} TRX`}
            hint={`${withdrawnRewardsSun} SUN`}
          />
          <ValueCard
            label="Allocated in DB"
            value={`${allocatedInDbTrx} TRX`}
            hint={`${allocatedInDbSun} SUN • Backend accounting only`}
          />
        </section>

        <section className="grid gap-4 md:grid-cols-4">
          <ValueCard
            label="Current level"
            value={levelToLabel(currentLevel)}
            hint={`Current buyers: ${progress?.buyersCount ?? stats?.totalBuyers ?? 0}`}
          />
          <ValueCard
            label="Next threshold"
            value={String(progress?.nextThreshold ?? 0)}
            hint="Buyers needed for next milestone"
          />
          <ValueCard
            label="Remaining"
            value={String(progress?.remainingToNextLevel ?? 0)}
            hint="Buyers left to next level"
          />
          <ValueCard label="Created at" value={formatDate(identity?.createdAt ?? 0)} />
        </section>

        <section className="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 className="text-xl font-semibold">On-chain profile</h2>

          <div className="mt-4 grid gap-4 md:grid-cols-2">
            <ValueCard label="Slug hash" value={identity?.slugHash || "—"} />
            <ValueCard label="Meta hash" value={identity?.metaHash || "—"} />
            <ValueCard
              label="Profile active"
              value={identity?.active ? "Yes" : "No"}
            />
            <ValueCard
              label="Level label"
              value={levelToLabel(effectiveLevel)}
              hint={`Reward percent: ${identity?.rewardPercent ?? 0}%`}
            />
          </div>
        </section>

        <section className="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 className="text-xl font-semibold">Links</h2>

          <div className="mt-4 flex flex-wrap gap-3">
            {walletExplorerUrl ? (
              <a
                href={walletExplorerUrl}
                target="_blank"
                rel="noreferrer"
                className="rounded-2xl border border-white/10 px-4 py-3 text-sm font-semibold text-white/80 transition hover:bg-white/10"
              >
                Wallet on Tronscan
              </a>
            ) : null}

            {withdrawExplorerUrl ? (
              <a
                href={withdrawExplorerUrl}
                target="_blank"
                rel="noreferrer"
                className="rounded-2xl border border-white/10 px-4 py-3 text-sm font-semibold text-white/80 transition hover:bg-white/10"
              >
                Last withdrawal tx
              </a>
            ) : null}
          </div>
        </section>
      </div>
    </main>
  );
}
```

---

## FILE PATH

`apps/cabinet/src/hooks/useAmbassadorDashboard.ts`

## FILE CONTENT

```ts
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
      const wallet =
        state.wallet && state.wallet.trim()
          ? state.wallet
          : await getConnectedWalletAddress();

      const withdrawSessionId =
        state.dashboard?.withdrawalQueue?.withdrawSessionId ?? null;

      const result = await withdrawRewards({
        wallet,
        withdrawSessionId
      });

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
  }, [load, state.wallet, state.dashboard]);

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
```

---

## FILE PATH

`apps/cabinet/src/lib/blockchain/controller.ts`

## FILE CONTENT

```ts
import { FOURTEEN_CONTROLLER_CONTRACT } from "../../../../../shared/config/contracts";

declare global {
  interface Window {
    tronWeb?: any;
    tronLink?: any;
  }
}

const ZERO_BYTES32 =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

const DEFAULT_CONFIRM_WITHDRAWAL_ENDPOINT = "/cabinet/confirm-withdrawal";

export interface AmbassadorIdentity {
  wallet: string;
  exists: boolean;
  active: boolean;
  selfRegistered: boolean;
  manualAssigned: boolean;
  overrideEnabled: boolean;
  level: number;
  effectiveLevel: number;
  currentLevel: number;
  overrideLevel: number;
  rewardPercent: number;
  createdAt: number;
  slugHash: string;
  metaHash: string;
}

export interface AmbassadorStats {
  totalBuyers: number;
  trackedVolumeSun: string;
  trackedVolumeTrx: string;
  claimableRewardsSun: string;
  claimableRewardsTrx: string;
  lifetimeRewardsSun: string;
  lifetimeRewardsTrx: string;
  withdrawnRewardsSun: string;
  withdrawnRewardsTrx: string;
}

export interface AmbassadorLevelProgress {
  currentLevel: number;
  buyersCount: number;
  nextThreshold: number;
  remainingToNextLevel: number;
}

export interface AmbassadorWithdrawalQueue {
  availableOnChainSun: string;
  availableOnChainTrx: string;
  availableOnChainCount: number;

  allocatedInDbSun: string;
  allocatedInDbTrx: string;
  allocatedInDbCount: number;

  pendingBackendSyncSun: string;
  pendingBackendSyncTrx: string;
  pendingBackendSyncCount: number;

  requestedForProcessingSun: string;
  requestedForProcessingTrx: string;
  requestedForProcessingCount: number;

  hasProcessingWithdrawal: boolean;
  withdrawSessionId: string | null;
}

export interface AmbassadorDashboard {
  identity: AmbassadorIdentity;
  stats: AmbassadorStats;
  progress: AmbassadorLevelProgress;
  withdrawalQueue: AmbassadorWithdrawalQueue;
}

export interface WithdrawResult {
  txid: string;
}

export interface ConfirmWithdrawalInput {
  wallet: string;
  txid: string;
  withdrawSessionId?: string | null;
}

function assertBrowser(): void {
  if (typeof window === "undefined") {
    throw new Error("Browser environment is required");
  }
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function safeString(value: unknown, fallback = "0"): string {
  if (value == null) {
    return fallback;
  }

  return String(value);
}

function safeSunString(value: unknown, fallback = "0"): string {
  const raw = safeString(value, fallback).trim();

  if (!raw) {
    return fallback;
  }

  if (/^\d+$/.test(raw)) {
    return raw;
  }

  if (/^-?\d+$/.test(raw)) {
    return raw.startsWith("-") ? fallback : raw;
  }

  return fallback;
}

function safeNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "bigint") {
    return Number(value);
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return fallback;
}

function safeBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value === "bigint") {
    return value !== 0n;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();

    if (!normalized) return false;
    if (normalized === "true") return true;
    if (normalized === "false") return false;
    if (normalized === "1") return true;
    if (normalized === "0") return false;
  }

  return Boolean(value);
}

function pickTupleValue(source: any, index: number, ...keys: string[]): any {
  if (Array.isArray(source)) {
    if (source[index] !== undefined) {
      return source[index];
    }
  }

  if (source && typeof source === "object") {
    for (const key of keys) {
      if (key && key in source) {
        return source[key];
      }
    }

    const numericKey = String(index);
    if (numericKey in source) {
      return source[numericKey];
    }

    const values = Object.values(source);
    if (values[index] !== undefined) {
      return values[index];
    }
  }

  return undefined;
}

function pickFirstDefined(
  source: any,
  candidates: Array<{ index: number; keys: string[] }>
): any {
  for (const candidate of candidates) {
    const value = pickTupleValue(source, candidate.index, ...candidate.keys);
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return undefined;
}

export function sunToTrxString(value: unknown): string {
  const raw = safeString(value, "0").trim();

  if (!raw || raw === "0") {
    return "0";
  }

  const negative = raw.startsWith("-");
  const digits = negative ? raw.slice(1) : raw;

  if (!/^\d+$/.test(digits)) {
    return "0";
  }

  const padded = digits.padStart(7, "0");
  const whole = padded.slice(0, -6) || "0";
  const fraction = padded.slice(-6).replace(/0+$/, "");
  const result = fraction ? `${whole}.${fraction}` : whole;

  return negative ? `-${result}` : result;
}

function normalizeHex32(value: unknown): string {
  const raw = safeString(value, ZERO_BYTES32).trim().toLowerCase();

  if (!raw) {
    return ZERO_BYTES32;
  }

  if (/^0x[0-9a-f]{64}$/.test(raw)) {
    return raw;
  }

  return ZERO_BYTES32;
}

function normalizeMetaHash(value: unknown): string {
  const raw = normalizeHex32(value);
  return raw === ZERO_BYTES32 ? "—" : raw;
}

function normalizeSlugHash(value: unknown): string {
  const raw = normalizeHex32(value);
  return raw || ZERO_BYTES32;
}

function normalizeOptionalString(value: unknown): string | null {
  if (value == null) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized || null;
}

function getBackendBaseUrl(): string {
  const candidates = [
    process.env.NEXT_PUBLIC_ALLOCATION_WORKER_URL,
    process.env.NEXT_PUBLIC_BACKEND_BASE_URL
  ];

  for (const candidate of candidates) {
    const normalized = String(candidate || "").trim().replace(/\/+$/, "");
    if (normalized) {
      return normalized;
    }
  }

  return "";
}

async function readJson(response: Response): Promise<any> {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

async function getTronWeb(): Promise<any> {
  assertBrowser();

  const tronWeb = window.tronWeb;

  if (!tronWeb || !tronWeb.defaultAddress?.base58) {
    throw new Error("Tron wallet is not connected");
  }

  return tronWeb;
}

export async function getConnectedWalletAddress(): Promise<string> {
  const tronWeb = await getTronWeb();
  return assertNonEmpty(tronWeb.defaultAddress.base58, "wallet");
}

async function getControllerContractInstance(): Promise<any> {
  const tronWeb = await getTronWeb();
  return await tronWeb.contract().at(FOURTEEN_CONTROLLER_CONTRACT);
}

export function levelToLabel(level: number): string {
  if (level === 0) return "Bronze";
  if (level === 1) return "Silver";
  if (level === 2) return "Gold";
  if (level === 3) return "Platinum";
  return `Unknown (${level})`;
}

function mapIdentity(wallet: string, coreRaw: any, profileRaw: any): AmbassadorIdentity {
  const exists = safeBoolean(
    pickFirstDefined(coreRaw, [{ index: 0, keys: ["exists"] }])
  );

  const active = safeBoolean(
    pickFirstDefined(coreRaw, [{ index: 1, keys: ["active"] }])
  );

  const effectiveLevel = safeNumber(
    pickFirstDefined(coreRaw, [{ index: 2, keys: ["effectiveLevel", "level"] }])
  );

  const rewardPercent = safeNumber(
    pickFirstDefined(coreRaw, [{ index: 3, keys: ["rewardPercent"] }])
  );

  const createdAt = safeNumber(
    pickFirstDefined(coreRaw, [{ index: 4, keys: ["createdAt"] }])
  );

  const selfRegistered = safeBoolean(
    pickFirstDefined(profileRaw, [{ index: 0, keys: ["selfRegistered"] }])
  );

  const manualAssigned = safeBoolean(
    pickFirstDefined(profileRaw, [{ index: 1, keys: ["manualAssigned"] }])
  );

  const overrideEnabled = safeBoolean(
    pickFirstDefined(profileRaw, [{ index: 2, keys: ["overrideEnabled"] }])
  );

  const currentLevel = safeNumber(
    pickFirstDefined(profileRaw, [{ index: 3, keys: ["currentLevel"] }])
  );

  const overrideLevel = safeNumber(
    pickFirstDefined(profileRaw, [{ index: 4, keys: ["overrideLevel"] }])
  );

  const slugHash = normalizeSlugHash(
    pickFirstDefined(profileRaw, [{ index: 5, keys: ["slugHash"] }])
  );

  const metaHash = normalizeMetaHash(
    pickFirstDefined(profileRaw, [{ index: 6, keys: ["metaHash"] }])
  );

  return {
    wallet,
    exists,
    active,
    selfRegistered,
    manualAssigned,
    overrideEnabled,
    level: effectiveLevel,
    effectiveLevel,
    currentLevel,
    overrideLevel,
    rewardPercent,
    createdAt,
    slugHash,
    metaHash
  };
}

function mapStats(statsRaw: any): AmbassadorStats {
  const totalBuyers = safeNumber(
    pickFirstDefined(statsRaw, [{ index: 0, keys: ["totalBuyers", "buyersCount"] }])
  );

  const trackedVolumeSun = safeSunString(
    pickFirstDefined(statsRaw, [{ index: 1, keys: ["trackedVolumeSun", "totalVolumeSun"] }]),
    "0"
  );

  const lifetimeRewardsSun = safeSunString(
    pickFirstDefined(statsRaw, [{ index: 2, keys: ["lifetimeRewardsSun", "totalRewardsAccruedSun"] }]),
    "0"
  );

  const withdrawnRewardsSun = safeSunString(
    pickFirstDefined(statsRaw, [{ index: 3, keys: ["withdrawnRewardsSun", "totalRewardsClaimedSun"] }]),
    "0"
  );

  const claimableRewardsSun = safeSunString(
    pickFirstDefined(statsRaw, [{ index: 4, keys: ["claimableRewardsSun", "availableOnChainSun"] }]),
    "0"
  );

  return {
    totalBuyers,
    trackedVolumeSun,
    trackedVolumeTrx: sunToTrxString(trackedVolumeSun),
    claimableRewardsSun,
    claimableRewardsTrx: sunToTrxString(claimableRewardsSun),
    lifetimeRewardsSun,
    lifetimeRewardsTrx: sunToTrxString(lifetimeRewardsSun),
    withdrawnRewardsSun,
    withdrawnRewardsTrx: sunToTrxString(withdrawnRewardsSun)
  };
}

function mapProgress(progressRaw: any, identity?: AmbassadorIdentity): AmbassadorLevelProgress {
  const currentLevel = safeNumber(
    pickFirstDefined(progressRaw, [{ index: 0, keys: ["currentLevel", "level"] }]),
    identity?.currentLevel ?? identity?.effectiveLevel ?? 0
  );

  const buyersCount = safeNumber(
    pickFirstDefined(progressRaw, [{ index: 1, keys: ["buyersCount", "totalBuyers"] }]),
    0
  );

  const nextThreshold = safeNumber(
    pickFirstDefined(progressRaw, [{ index: 2, keys: ["nextThreshold"] }]),
    0
  );

  const remainingToNextLevel = safeNumber(
    pickFirstDefined(progressRaw, [{ index: 3, keys: ["remainingToNextLevel"] }]),
    0
  );

  return {
    currentLevel,
    buyersCount,
    nextThreshold,
    remainingToNextLevel
  };
}

function mapWithdrawalQueue(raw: any, stats: AmbassadorStats): AmbassadorWithdrawalQueue {
  const availableOnChainSun = safeSunString(
    pickFirstDefined(raw, [{ index: 0, keys: ["availableOnChainSun", "claimableRewardsSun"] }]),
    stats.claimableRewardsSun
  );

  const pendingBackendSyncSun = safeSunString(
    pickFirstDefined(raw, [{ index: 1, keys: ["pendingBackendSyncSun"] }]),
    "0"
  );

  const requestedForProcessingSun = safeSunString(
    pickFirstDefined(raw, [{ index: 2, keys: ["requestedForProcessingSun"] }]),
    "0"
  );

  const availableOnChainCount = safeNumber(
    pickFirstDefined(raw, [{ index: 3, keys: ["availableOnChainCount"] }]),
    0
  );

  const pendingBackendSyncCount = safeNumber(
    pickFirstDefined(raw, [{ index: 4, keys: ["pendingBackendSyncCount"] }]),
    0
  );

  const requestedForProcessingCount = safeNumber(
    pickFirstDefined(raw, [{ index: 5, keys: ["requestedForProcessingCount"] }]),
    0
  );

  const hasProcessingWithdrawal = safeBoolean(
    pickFirstDefined(raw, [{ index: 6, keys: ["hasProcessingWithdrawal"] }])
  );

  const allocatedInDbSun = safeSunString(
    pickFirstDefined(raw, [{ index: 7, keys: ["allocatedInDbSun"] }]),
    "0"
  );

  const allocatedInDbCount = safeNumber(
    pickFirstDefined(raw, [{ index: 8, keys: ["allocatedInDbCount"] }]),
    0
  );

  const withdrawSessionId = normalizeOptionalString(
    pickFirstDefined(raw, [{ index: 9, keys: ["withdrawSessionId"] }])
  );

  return {
    availableOnChainSun,
    availableOnChainTrx: sunToTrxString(availableOnChainSun),
    availableOnChainCount,

    allocatedInDbSun,
    allocatedInDbTrx: sunToTrxString(allocatedInDbSun),
    allocatedInDbCount,

    pendingBackendSyncSun,
    pendingBackendSyncTrx: sunToTrxString(pendingBackendSyncSun),
    pendingBackendSyncCount,

    requestedForProcessingSun,
    requestedForProcessingTrx: sunToTrxString(requestedForProcessingSun),
    requestedForProcessingCount,

    hasProcessingWithdrawal,
    withdrawSessionId
  };
}

function buildFallbackWithdrawalQueue(stats: AmbassadorStats): AmbassadorWithdrawalQueue {
  return {
    availableOnChainSun: stats.claimableRewardsSun,
    availableOnChainTrx: stats.claimableRewardsTrx,
    availableOnChainCount: stats.claimableRewardsSun !== "0" ? 1 : 0,

    allocatedInDbSun: "0",
    allocatedInDbTrx: "0",
    allocatedInDbCount: 0,

    pendingBackendSyncSun: "0",
    pendingBackendSyncTrx: "0",
    pendingBackendSyncCount: 0,

    requestedForProcessingSun: "0",
    requestedForProcessingTrx: "0",
    requestedForProcessingCount: 0,

    hasProcessingWithdrawal: false,
    withdrawSessionId: null
  };
}

export async function confirmWithdrawal(
  input: ConfirmWithdrawalInput
): Promise<unknown> {
  const baseUrl = getBackendBaseUrl();

  if (!baseUrl) {
    return null;
  }

  const wallet = assertNonEmpty(input.wallet, "wallet");
  const txid = assertNonEmpty(input.txid, "txid");

  const body: Record<string, unknown> = {
    wallet,
    txid
  };

  if (input.withdrawSessionId) {
    body.withdrawSessionId = input.withdrawSessionId;
  }

  const response = await fetch(`${baseUrl}${DEFAULT_CONFIRM_WITHDRAWAL_ENDPOINT}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const payload = await readJson(response);

  if (!response.ok) {
    throw new Error(
      payload?.error || payload?.message || "Failed to confirm withdrawal"
    );
  }

  return payload?.result ?? payload ?? null;
}

export async function readAmbassadorIdentity(wallet?: string): Promise<AmbassadorIdentity> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();

  const [coreRaw, profileRaw] = await Promise.all([
    contract.getDashboardCore(resolvedWallet).call(),
    contract.getDashboardProfile(resolvedWallet).call()
  ]);

  return mapIdentity(resolvedWallet, coreRaw, profileRaw);
}

export async function readAmbassadorStats(wallet?: string): Promise<AmbassadorStats> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const raw = await contract.getDashboardStats(resolvedWallet).call();

  return mapStats(raw);
}

export async function readAmbassadorLevelProgress(
  wallet?: string
): Promise<AmbassadorLevelProgress> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const identity = await readAmbassadorIdentity(resolvedWallet);
  const raw = await contract.getAmbassadorLevelProgress(resolvedWallet).call();

  return mapProgress(raw, identity);
}

export async function readAmbassadorWithdrawalQueue(
  wallet?: string,
  statsOverride?: AmbassadorStats
): Promise<AmbassadorWithdrawalQueue> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();
  const stats = statsOverride ?? (await readAmbassadorStats(resolvedWallet));

  if (typeof contract.getAmbassadorWithdrawalQueue === "function") {
    const raw = await contract.getAmbassadorWithdrawalQueue(resolvedWallet).call();
    return mapWithdrawalQueue(raw, stats);
  }

  if (typeof contract.getDashboardWithdrawalQueue === "function") {
    const raw = await contract.getDashboardWithdrawalQueue(resolvedWallet).call();
    return mapWithdrawalQueue(raw, stats);
  }

  return buildFallbackWithdrawalQueue(stats);
}

export async function withdrawRewards(
  input?: { wallet?: string; withdrawSessionId?: string | null }
): Promise<WithdrawResult> {
  const contract = await getControllerContractInstance();
  const txid = await contract.withdrawRewards().send();

  const result: WithdrawResult = {
    txid: assertNonEmpty(
      typeof txid === "string"
        ? txid
        : txid?.txid || txid?.transaction?.txID || txid?.txID || "",
      "txid"
    )
  };

  const wallet =
    input?.wallet && input.wallet.trim()
      ? input.wallet.trim()
      : await getConnectedWalletAddress();

  try {
    await confirmWithdrawal({
      wallet,
      txid: result.txid,
      withdrawSessionId: input?.withdrawSessionId ?? null
    });
  } catch (error) {
    console.error("confirmWithdrawal failed:", error);
  }

  return result;
}

export async function readAmbassadorDashboard(wallet?: string): Promise<AmbassadorDashboard> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const contract = await getControllerContractInstance();

  const [coreRaw, profileRaw, statsRaw, progressRaw] = await Promise.all([
    contract.getDashboardCore(resolvedWallet).call(),
    contract.getDashboardProfile(resolvedWallet).call(),
    contract.getDashboardStats(resolvedWallet).call(),
    contract.getAmbassadorLevelProgress(resolvedWallet).call()
  ]);

  const identity = mapIdentity(resolvedWallet, coreRaw, profileRaw);
  const stats = mapStats(statsRaw);
  const progress = mapProgress(progressRaw, identity);

  let withdrawalQueue: AmbassadorWithdrawalQueue;

  if (typeof contract.getAmbassadorWithdrawalQueue === "function") {
    const raw = await contract.getAmbassadorWithdrawalQueue(resolvedWallet).call();
    withdrawalQueue = mapWithdrawalQueue(raw, stats);
  } else if (typeof contract.getDashboardWithdrawalQueue === "function") {
    const raw = await contract.getDashboardWithdrawalQueue(resolvedWallet).call();
    withdrawalQueue = mapWithdrawalQueue(raw, stats);
  } else {
    withdrawalQueue = buildFallbackWithdrawalQueue(stats);
  }

  return {
    identity,
    stats,
    progress,
    withdrawalQueue
  };
}
```

---

## FILE PATH

`apps/cabinet/src/lib/referral/storage.ts`

## FILE CONTENT

```ts
import { REFERRAL_STORAGE_KEY } from "../../../../../shared/config/referral";
import { assertValidSlug, normalizeSlug } from "../../../../../shared/utils/slug";

export interface CabinetStoredReferralRecord {
  slug: string;
  capturedAt: number;
  expiresAt: number;
  source: "query";
}

function isBrowser(): boolean {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

function safeParseReferral(raw: string | null): CabinetStoredReferralRecord | null {
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as Partial<CabinetStoredReferralRecord>;

    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    if (typeof parsed.slug !== "string") {
      return null;
    }

    if (typeof parsed.capturedAt !== "number") {
      return null;
    }

    if (typeof parsed.expiresAt !== "number") {
      return null;
    }

    if (parsed.source !== "query") {
      return null;
    }

    const slug = assertValidSlug(normalizeSlug(parsed.slug));

    return {
      slug,
      capturedAt: parsed.capturedAt,
      expiresAt: parsed.expiresAt,
      source: "query"
    };
  } catch {
    return null;
  }
}

export function getStoredReferralRaw(): CabinetStoredReferralRecord | null {
  if (!isBrowser()) {
    return null;
  }

  return safeParseReferral(window.localStorage.getItem(REFERRAL_STORAGE_KEY));
}

export function isStoredReferralExpired(
  record: CabinetStoredReferralRecord,
  now = Date.now()
): boolean {
  return record.expiresAt <= now;
}

export function getStoredReferral(now = Date.now()): CabinetStoredReferralRecord | null {
  const record = getStoredReferralRaw();

  if (!record) {
    return null;
  }

  if (isStoredReferralExpired(record, now)) {
    clearStoredReferral();
    return null;
  }

  return record;
}

export function getStoredReferralSlug(now = Date.now()): string | null {
  const record = getStoredReferral(now);
  return record?.slug ?? null;
}

export function clearStoredReferral(): void {
  if (!isBrowser()) {
    return;
  }

  window.localStorage.removeItem(REFERRAL_STORAGE_KEY);
}
```

---

## FILE PATH

`apps/cabinet/src/lib/telegram/link.ts`

## FILE CONTENT

```ts
import { buildTronscanAddressUrl } from "../../../../../shared/config/contracts";

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

export function buildWalletExplorerUrl(wallet: string): string {
  return buildTronscanAddressUrl(assertNonEmpty(wallet, "wallet"));
}
```
