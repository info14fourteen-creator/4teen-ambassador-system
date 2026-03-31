# 4teen-ambassador-system — CABINET

Generated: 2026-03-31T11:37:07.563Z
Repository: info14fourteen-creator/4teen-ambassador-system
Branch: main

## Included files

- 4teen-ambassador-system :: apps/cabinet/src/app/ambassador/page.tsx
- 4teen-ambassador-system :: apps/cabinet/src/hooks/useAmbassadorDashboard.ts
- 4teen-ambassador-system :: apps/cabinet/src/lib/blockchain/controller.ts
- 4teen-ambassador-system :: apps/cabinet/src/lib/referral/storage.ts
- 4teen-ambassador-system :: apps/cabinet/src/lib/telegram/link.ts

---

## FILE: 4teen-ambassador-system :: apps/cabinet/src/app/ambassador/page.tsx

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
    return new Date(normalized).toLocaleString();
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

  const walletExplorerUrl = useMemo(() => {
    if (!wallet) return "";
    return buildWalletExplorerUrl(wallet);
  }, [wallet]);

  const withdrawExplorerUrl = useMemo(() => {
    if (!lastWithdrawTxid) return "";
    return buildTronscanTransactionUrl(lastWithdrawTxid);
  }, [lastWithdrawTxid]);

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

  const identity = dashboard?.identity ?? null;
  const stats = dashboard?.stats ?? null;
  const progress = dashboard?.progress ?? null;
  const withdrawalQueue = dashboard?.withdrawalQueue ?? null;

  const effectiveLevel = identity?.effectiveLevel ?? identity?.level ?? 0;
  const currentLevel = progress?.currentLevel ?? effectiveLevel;

  const trackedVolumeSun = stats?.trackedVolumeSun ?? "0";
  const trackedVolumeTrx = stats?.trackedVolumeTrx ?? sunToTrxString(trackedVolumeSun);

  const claimableRewardsSun =
    stats?.claimableRewardsSun ?? withdrawalQueue?.availableOnChainSun ?? "0";
  const claimableRewardsTrx =
    stats?.claimableRewardsTrx ?? sunToTrxString(claimableRewardsSun);

  const lifetimeRewardsSun = stats?.lifetimeRewardsSun ?? "0";
  const lifetimeRewardsTrx =
    stats?.lifetimeRewardsTrx ?? sunToTrxString(lifetimeRewardsSun);

  const withdrawnRewardsSun = stats?.withdrawnRewardsSun ?? "0";
  const withdrawnRewardsTrx =
    stats?.withdrawnRewardsTrx ?? sunToTrxString(withdrawnRewardsSun);

  const allocatedInDbSun = withdrawalQueue?.allocatedInDbSun ?? "0";
  const allocatedInDbTrx =
    withdrawalQueue?.allocatedInDbTrx ?? sunToTrxString(allocatedInDbSun);

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
            trxValue={sunToTrxString(statusCards.availableOnChainSun)}
            sunValue={statusCards.availableOnChainSun}
            count={statusCards.availableOnChainCount}
            accentClass="border-emerald-500/20 bg-emerald-500/10"
            hint="Real withdrawable amount from contract state."
          />

          <StatusCard
            label="Allocated in DB"
            trxValue={sunToTrxString(statusCards.allocatedInDbSun)}
            sunValue={statusCards.allocatedInDbSun}
            count={statusCards.allocatedInDbCount}
            accentClass="border-violet-500/20 bg-violet-500/10"
            hint="Backend accounting only. Not guaranteed withdrawable now."
          />

          <StatusCard
            label="Pending backend sync"
            trxValue={sunToTrxString(statusCards.pendingBackendSyncSun)}
            sunValue={statusCards.pendingBackendSyncSun}
            count={statusCards.pendingBackendSyncCount}
            accentClass="border-amber-500/20 bg-amber-500/10"
            hint="Verified rewards that still need backend and on-chain sync."
          />

          <StatusCard
            label="Requested for processing"
            trxValue={sunToTrxString(statusCards.requestedForProcessingSun)}
            sunValue={statusCards.requestedForProcessingSun}
            count={statusCards.requestedForProcessingCount}
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
            hint={`Current buyers: ${progress?.buyersCount ?? 0}`}
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

## FILE: 4teen-ambassador-system :: apps/cabinet/src/hooks/useAmbassadorDashboard.ts

```ts
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
```

---

## FILE: 4teen-ambassador-system :: apps/cabinet/src/lib/blockchain/controller.ts

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

function safeString(value: any, fallback = "0"): string {
  if (value == null) return fallback;
  return String(value);
}

function safeSunString(value: any, fallback = "0"): string {
  const raw = safeString(value, fallback).trim();
  return /^\d+$/.test(raw) ? raw : fallback;
}

function safeNumber(value: any): number {
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

  return 0;
}

function safeBoolean(value: any): boolean {
  return Boolean(value);
}

function pickTupleValue(source: any, index: number, key?: string): any {
  if (Array.isArray(source)) {
    return source[index];
  }

  if (source && typeof source === "object") {
    if (key && key in source) {
      return source[key];
    }

    const numericKey = String(index);
    if (numericKey in source) {
      return source[numericKey];
    }

    const values = Object.values(source);
    return values[index];
  }

  return undefined;
}

export function sunToTrxString(value: any): string {
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

function normalizeHex32(value: any): string {
  const raw = safeString(value, ZERO_BYTES32).trim().toLowerCase();
  return raw || ZERO_BYTES32;
}

function normalizeMetaHash(value: any): string {
  const raw = normalizeHex32(value);
  return raw === ZERO_BYTES32 ? "—" : raw;
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
  const exists = safeBoolean(pickTupleValue(coreRaw, 0, "exists"));
  const active = safeBoolean(pickTupleValue(coreRaw, 1, "active"));
  const effectiveLevel = safeNumber(pickTupleValue(coreRaw, 2, "effectiveLevel"));
  const rewardPercent = safeNumber(pickTupleValue(coreRaw, 3, "rewardPercent"));
  const createdAt = safeNumber(pickTupleValue(coreRaw, 4, "createdAt"));

  const selfRegistered = safeBoolean(pickTupleValue(profileRaw, 0, "selfRegistered"));
  const manualAssigned = safeBoolean(pickTupleValue(profileRaw, 1, "manualAssigned"));
  const overrideEnabled = safeBoolean(pickTupleValue(profileRaw, 2, "overrideEnabled"));
  const currentLevel = safeNumber(pickTupleValue(profileRaw, 3, "currentLevel"));
  const overrideLevel = safeNumber(pickTupleValue(profileRaw, 4, "overrideLevel"));
  const slugHash = normalizeHex32(pickTupleValue(profileRaw, 5, "slugHash"));
  const metaHash = normalizeMetaHash(pickTupleValue(profileRaw, 6, "metaHash"));

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
  const totalBuyers = safeNumber(pickTupleValue(statsRaw, 0, "totalBuyers"));
  const trackedVolumeSun = safeSunString(
    pickTupleValue(statsRaw, 1, "totalVolumeSun") ??
      pickTupleValue(statsRaw, 1, "trackedVolumeSun"),
    "0"
  );
  const lifetimeRewardsSun = safeSunString(
    pickTupleValue(statsRaw, 2, "totalRewardsAccruedSun") ??
      pickTupleValue(statsRaw, 2, "lifetimeRewardsSun"),
    "0"
  );
  const withdrawnRewardsSun = safeSunString(
    pickTupleValue(statsRaw, 3, "totalRewardsClaimedSun") ??
      pickTupleValue(statsRaw, 3, "withdrawnRewardsSun"),
    "0"
  );
  const claimableRewardsSun = safeSunString(
    pickTupleValue(statsRaw, 4, "claimableRewardsSun"),
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

function mapProgress(progressRaw: any): AmbassadorLevelProgress {
  return {
    currentLevel: safeNumber(pickTupleValue(progressRaw, 0, "currentLevel")),
    buyersCount: safeNumber(pickTupleValue(progressRaw, 1, "buyersCount")),
    nextThreshold: safeNumber(pickTupleValue(progressRaw, 2, "nextThreshold")),
    remainingToNextLevel: safeNumber(pickTupleValue(progressRaw, 3, "remainingToNextLevel"))
  };
}

function mapWithdrawalQueue(raw: any, stats: AmbassadorStats): AmbassadorWithdrawalQueue {
  const availableOnChainSun = safeSunString(
    pickTupleValue(raw, 0, "availableOnChainSun") ?? stats.claimableRewardsSun,
    "0"
  );
  const pendingBackendSyncSun = safeSunString(
    pickTupleValue(raw, 1, "pendingBackendSyncSun"),
    "0"
  );
  const requestedForProcessingSun = safeSunString(
    pickTupleValue(raw, 2, "requestedForProcessingSun"),
    "0"
  );

  const availableOnChainCount = safeNumber(
    pickTupleValue(raw, 3, "availableOnChainCount")
  );
  const pendingBackendSyncCount = safeNumber(
    pickTupleValue(raw, 4, "pendingBackendSyncCount")
  );
  const requestedForProcessingCount = safeNumber(
    pickTupleValue(raw, 5, "requestedForProcessingCount")
  );
  const hasProcessingWithdrawal = safeBoolean(
    pickTupleValue(raw, 6, "hasProcessingWithdrawal")
  );

  const allocatedInDbSun = safeSunString(
    pickTupleValue(raw, 7, "allocatedInDbSun"),
    "0"
  );
  const allocatedInDbCount = safeNumber(
    pickTupleValue(raw, 8, "allocatedInDbCount")
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

    hasProcessingWithdrawal
  };
}

function buildFallbackWithdrawalQueue(stats: AmbassadorStats): AmbassadorWithdrawalQueue {
  return {
    availableOnChainSun: stats.claimableRewardsSun,
    availableOnChainTrx: stats.claimableRewardsTrx,
    availableOnChainCount: 0,

    allocatedInDbSun: "0",
    allocatedInDbTrx: "0",
    allocatedInDbCount: 0,

    pendingBackendSyncSun: "0",
    pendingBackendSyncTrx: "0",
    pendingBackendSyncCount: 0,

    requestedForProcessingSun: "0",
    requestedForProcessingTrx: "0",
    requestedForProcessingCount: 0,

    hasProcessingWithdrawal: false
  };
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
  const raw = await contract.getAmbassadorLevelProgress(resolvedWallet).call();

  return mapProgress(raw);
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

export async function withdrawRewards(): Promise<WithdrawResult> {
  const contract = await getControllerContractInstance();
  const txid = await contract.withdrawRewards().send();

  return {
    txid: assertNonEmpty(
      typeof txid === "string"
        ? txid
        : txid?.txid || txid?.transaction?.txID || txid?.txID || "",
      "txid"
    )
  };
}

export async function readAmbassadorDashboard(wallet?: string): Promise<AmbassadorDashboard> {
  const resolvedWallet = wallet
    ? assertNonEmpty(wallet, "wallet")
    : await getConnectedWalletAddress();

  const [identity, stats, progress] = await Promise.all([
    readAmbassadorIdentity(resolvedWallet),
    readAmbassadorStats(resolvedWallet),
    readAmbassadorLevelProgress(resolvedWallet)
  ]);

  const withdrawalQueue = await readAmbassadorWithdrawalQueue(resolvedWallet, stats);

  return {
    identity,
    stats,
    progress,
    withdrawalQueue
  };
}
```

---

## FILE: 4teen-ambassador-system :: apps/cabinet/src/lib/referral/storage.ts

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

## FILE: 4teen-ambassador-system :: apps/cabinet/src/lib/telegram/link.ts

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
