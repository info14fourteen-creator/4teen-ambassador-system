"use client";

import { useMemo } from "react";
import { buildTronscanTransactionUrl } from "../../../../../shared/config/contracts";
import { useAmbassadorDashboard } from "../../hooks/useAmbassadorDashboard";
import { buildWalletExplorerUrl } from "../../lib/telegram/link";
import { levelToLabel } from "../../lib/blockchain/controller";

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
      <div className="mt-2 text-2xl font-semibold text-white break-words">{value}</div>
      {hint ? <div className="mt-1 text-sm text-white/45">{hint}</div> : null}
    </div>
  );
}

function ActionButton({
  children,
  onClick,
  disabled
}: {
  children: React.ReactNode;
  onClick?: () => void | Promise<void>;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={() => {
        void onClick?.();
      }}
      disabled={disabled}
      className="rounded-2xl bg-[#ff6900] px-4 py-3 text-sm font-semibold text-white transition disabled:cursor-not-allowed disabled:opacity-50"
    >
      {children}
    </button>
  );
}

export default function AmbassadorPage() {
  const {
    wallet,
    dashboard,
    isConnected,
    isRegistered,
    isLoading,
    isRefreshing,
    isWithdrawing,
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

  if (isLoading) {
    return (
      <main className="min-h-screen bg-[#111] px-6 py-10 text-white">
        <div className="mx-auto max-w-5xl">
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
      <div className="mx-auto max-w-5xl space-y-6">
        <section className="rounded-3xl border border-white/10 bg-white/5 p-6">
          <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
            <div>
              <div className="text-xs uppercase tracking-[0.18em] text-white/45">
                4TEEN Ambassador Cabinet
              </div>
              <h1 className="mt-2 text-3xl font-semibold">Your ambassador dashboard</h1>
              <p className="mt-2 max-w-2xl text-sm text-white/60">
                Live on-chain cabinet for ambassador status, reward level, buyer count,
                volume, accrued rewards, claimable rewards, and withdrawals.
              </p>
            </div>

            <div className="flex gap-3">
              <ActionButton onClick={refresh} disabled={isRefreshing || isWithdrawing}>
                {isRefreshing ? "Refreshing..." : "Refresh"}
              </ActionButton>

              <ActionButton
                onClick={handleWithdrawRewards}
                disabled={!isRegistered || !rewards || rewards.availableSun === "0" || isWithdrawing}
              >
                {isWithdrawing ? "Withdrawing..." : "Withdraw rewards"}
              </ActionButton>
            </div>
          </div>

          {error ? (
            <div className="mt-4 rounded-2xl border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-200">
              {error}
            </div>
          ) : null}
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
          <ValueCard
            label="Total buyers"
            value={String(stats?.totalBuyers ?? 0)}
          />
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

        <section className="grid gap-4 md:grid-cols-3">
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
        </section>

        <section className="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 className="text-xl font-semibold">On-chain profile</h2>

          <div className="mt-4 grid gap-4 md:grid-cols-2">
            <ValueCard
              label="Slug hash"
              value={identity?.slugHash || "—"}
            />
            <ValueCard
              label="Meta hash"
              value={identity?.metaHash || "—"}
            />
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
                className="rounded-2xl border border-white/10 px-4 py-3 text-sm font-semibold text-white/80"
              >
                Wallet on Tronscan
              </a>
            ) : null}

            {withdrawExplorerUrl ? (
              <a
                href={withdrawExplorerUrl}
                target="_blank"
                rel="noreferrer"
                className="rounded-2xl border border-white/10 px-4 py-3 text-sm font-semibold text-white/80"
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
