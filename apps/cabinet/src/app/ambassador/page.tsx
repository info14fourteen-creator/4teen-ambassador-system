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
  accentClass
}: {
  label: string;
  trxValue: string;
  sunValue: string;
  count: number;
  accentClass: string;
}) {
  return (
    <div className={`rounded-2xl border p-4 ${accentClass}`}>
      <div className="text-xs uppercase tracking-[0.18em] text-white/55">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-white">{trxValue} TRX</div>
      <div className="mt-1 text-sm text-white/55">{sunValue} SUN</div>
      <div className="mt-3 text-sm text-white/65">
        {count} {count === 1 ? "purchase" : "purchases"}
      </div>
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

function buildWithdrawButtonLabel(params: {
  isRegistered: boolean;
  isWithdrawing: boolean;
  hasProcessingWithdrawal: boolean;
  hasAvailableOnChain: boolean;
  hasPendingBackendSync: boolean;
  hasRequestedForProcessing: boolean;
}): string {
  const {
    isRegistered,
    isWithdrawing,
    hasProcessingWithdrawal,
    hasAvailableOnChain,
    hasPendingBackendSync,
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

  if (hasPendingBackendSync && !hasAvailableOnChain) {
    return "Prepare withdrawal request";
  }

  if (hasAvailableOnChain) {
    return "Withdraw rewards";
  }

  return "No rewards available";
}

function buildWithdrawHint(params: {
  hasAvailableOnChain: boolean;
  hasPendingBackendSync: boolean;
  hasRequestedForProcessing: boolean;
}): string {
  const { hasAvailableOnChain, hasPendingBackendSync, hasRequestedForProcessing } = params;

  if (hasRequestedForProcessing) {
    return "Your withdrawal request was created and is waiting for backend processing.";
  }

  if (hasPendingBackendSync && hasAvailableOnChain) {
    return "Part of rewards is already on-chain, and part is still waiting for backend sync.";
  }

  if (hasPendingBackendSync) {
    return "Rewards exist, but they are not yet written on-chain.";
  }

  if (hasAvailableOnChain) {
    return "These rewards are already written on-chain and available now.";
  }

  return "No rewards available for withdrawal yet.";
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

  const withdrawButtonLabel = useMemo(
    () =>
      buildWithdrawButtonLabel({
        isRegistered,
        isWithdrawing,
        hasProcessingWithdrawal,
        hasAvailableOnChain: statusCards.hasAvailableOnChain,
        hasPendingBackendSync: statusCards.hasPendingBackendSync,
        hasRequestedForProcessing: statusCards.hasRequestedForProcessing
      }),
    [
      isRegistered,
      isWithdrawing,
      hasProcessingWithdrawal,
      statusCards.hasAvailableOnChain,
      statusCards.hasPendingBackendSync,
      statusCards.hasRequestedForProcessing
    ]
  );

  const withdrawHint = useMemo(
    () =>
      buildWithdrawHint({
        hasAvailableOnChain: statusCards.hasAvailableOnChain,
        hasPendingBackendSync: statusCards.hasPendingBackendSync,
        hasRequestedForProcessing: statusCards.hasRequestedForProcessing
      }),
    [
      statusCards.hasAvailableOnChain,
      statusCards.hasPendingBackendSync,
      statusCards.hasRequestedForProcessing
    ]
  );

  const withdrawDisabled = useMemo(() => {
    if (!isRegistered) return true;
    if (isWithdrawing) return true;
    if (hasProcessingWithdrawal) return true;
    if (statusCards.hasRequestedForProcessing) return true;

    return !statusCards.hasAvailableOnChain && !statusCards.hasPendingBackendSync;
  }, [
    isRegistered,
    isWithdrawing,
    hasProcessingWithdrawal,
    statusCards.hasRequestedForProcessing,
    statusCards.hasAvailableOnChain,
    statusCards.hasPendingBackendSync
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
  const rewards = dashboard?.rewards ?? null;
  const progress = dashboard?.progress ?? null;

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
                Track your ambassador profile, level, buyers, reward queues, on-chain
                availability and withdrawal processing in one place.
              </p>
            </div>

            <div className="flex flex-wrap gap-3">
              <ActionButton onClick={refresh} disabled={isRefreshing || isWithdrawing} variant="secondary">
                {isRefreshing ? "Refreshing..." : "Refresh"}
              </ActionButton>

              <ActionButton onClick={handleWithdrawRewards} disabled={withdrawDisabled}>
                {withdrawButtonLabel}
              </ActionButton>
            </div>
          </div>

          <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4 text-sm text-white/65">
            {withdrawHint}
          </div>

          {error ? (
            <div className="mt-4 rounded-2xl border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-200">
              {error}
            </div>
          ) : null}
        </section>

        <section className="grid gap-4 md:grid-cols-3">
          <StatusCard
            label="Available on-chain"
            trxValue={sunToTrxString(statusCards.availableOnChainSun)}
            sunValue={statusCards.availableOnChainSun}
            count={statusCards.availableOnChainCount}
            accentClass="border-emerald-500/20 bg-emerald-500/10"
          />

          <StatusCard
            label="Pending backend sync"
            trxValue={sunToTrxString(statusCards.pendingBackendSyncSun)}
            sunValue={statusCards.pendingBackendSyncSun}
            count={statusCards.pendingBackendSyncCount}
            accentClass="border-amber-500/20 bg-amber-500/10"
          />

          <StatusCard
            label="Requested for processing"
            trxValue={sunToTrxString(statusCards.requestedForProcessingSun)}
            sunValue={statusCards.requestedForProcessingSun}
            count={statusCards.requestedForProcessingCount}
            accentClass="border-sky-500/20 bg-sky-500/10"
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
                ? `${identity.active ? "Active" : "Inactive"} • ${levelToLabel(identity.effectiveLevel)}`
                : "No ambassador profile found"
            }
          />
          <ValueCard
            label="Reward percent"
            value={`${identity?.rewardPercent ?? 0}%`}
            hint={`Effective level: ${levelToLabel(identity?.effectiveLevel ?? 0)}`}
          />
        </section>

        <section className="grid gap-4 md:grid-cols-3">
          <ValueCard label="Total buyers" value={String(stats?.totalBuyers ?? 0)} />
          <ValueCard
            label="Tracked volume"
            value={`${stats?.totalVolumeTrx ?? "0"} TRX`}
            hint={`${stats?.totalVolumeSun ?? "0"} SUN`}
          />
          <ValueCard
            label="Claimable rewards"
            value={`${rewards?.availableTrx ?? "0"} TRX`}
            hint={`${rewards?.availableSun ?? "0"} SUN`}
          />
        </section>

        <section className="grid gap-4 md:grid-cols-3">
          <ValueCard
            label="Lifetime rewards"
            value={`${rewards?.lifetimeTrx ?? "0"} TRX`}
            hint={`${rewards?.lifetimeSun ?? "0"} SUN`}
          />
          <ValueCard
            label="Withdrawn rewards"
            value={`${rewards?.withdrawnTrx ?? "0"} TRX`}
            hint={`${rewards?.withdrawnSun ?? "0"} SUN`}
          />
          <ValueCard
            label="Accrued total"
            value={`${stats?.totalRewardsAccruedTrx ?? "0"} TRX`}
            hint={`${stats?.totalRewardsAccruedSun ?? "0"} SUN`}
          />
        </section>

        <section className="grid gap-4 md:grid-cols-4">
          <ValueCard
            label="Current level"
            value={levelToLabel(progress?.currentLevel ?? 0)}
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
              label="Registration mode"
              value={
                identity?.selfRegistered
                  ? "Self-registered"
                  : identity?.manualAssigned
                    ? "Manually assigned"
                    : "—"
              }
            />
            <ValueCard
              label="Override"
              value={identity?.overrideEnabled ? "Enabled" : "Disabled"}
              hint={
                identity
                  ? `Current: ${levelToLabel(identity.currentLevel)} • Override: ${levelToLabel(identity.overrideLevel)}`
                  : undefined
              }
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
