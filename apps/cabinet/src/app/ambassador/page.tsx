"use client";

import { useMemo, useState } from "react";
import { buildTronscanTransactionUrl } from "../../../../../shared/config/contracts";
import { useAmbassadorDashboard } from "../../hooks/useAmbassadorDashboard";
import {
  buildReferralShareUrl,
  buildTelegramBindLink,
  buildWalletExplorerUrl
} from "../../lib/telegram/link";

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
      <div className="mt-2 text-2xl font-semibold text-white">{value}</div>
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
    identity,
    stats,
    rewards,
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

  const [copyState, setCopyState] = useState<string>("");

  const referralUrl = useMemo(() => {
    if (!identity?.slug) return "";
    return buildReferralShareUrl(identity.slug);
  }, [identity?.slug]);

  const telegramBindLink = useMemo(() => {
    if (!wallet) return "";
    return buildTelegramBindLink(wallet);
  }, [wallet]);

  const walletExplorerUrl = useMemo(() => {
    if (!wallet) return "";
    return buildWalletExplorerUrl(wallet);
  }, [wallet]);

  const withdrawExplorerUrl = useMemo(() => {
    if (!lastWithdrawTxid) return "";
    return buildTronscanTransactionUrl(lastWithdrawTxid);
  }, [lastWithdrawTxid]);

  async function handleCopyReferral() {
    if (!referralUrl || typeof navigator === "undefined" || !navigator.clipboard) {
      return;
    }

    await navigator.clipboard.writeText(referralUrl);
    setCopyState("Copied");

    window.setTimeout(() => {
      setCopyState("");
    }, 1800);
  }

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
                Track your referral slug, qualified purchases, available rewards,
                Telegram binding status, and withdrawals from the controller.
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
            hint={identity?.slug ? `Slug: ${identity.slug}` : "No ambassador profile found"}
          />
          <ValueCard
            label="Telegram"
            value={identity?.telegramBound ? "Bound" : "Not bound"}
            hint={identity?.telegramUsername || "Telegram username not linked yet"}
          />
        </section>

        <section className="grid gap-4 md:grid-cols-3">
          <ValueCard
            label="Total referrals"
            value={String(stats?.totalReferrals ?? 0)}
          />
          <ValueCard
            label="Qualified purchases"
            value={String(stats?.totalQualifiedPurchases ?? 0)}
          />
          <ValueCard
            label="Available rewards"
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
            label="Withdrawn"
            value={`${rewards?.withdrawnTrx ?? "0"} TRX`}
            hint={`${rewards?.withdrawnSun ?? "0"} SUN`}
          />
          <ValueCard
            label="Total reward volume"
            value={`${stats?.totalRewardTrx ?? "0"} TRX`}
            hint={`${stats?.totalRewardSun ?? "0"} SUN`}
          />
        </section>

        <section className="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 className="text-xl font-semibold">Referral link</h2>

          {identity?.slug ? (
            <>
              <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4 text-sm text-white/80 break-all">
                {referralUrl}
              </div>

              <div className="mt-4 flex flex-wrap gap-3">
                <ActionButton onClick={handleCopyReferral}>
                  {copyState || "Copy referral link"}
                </ActionButton>

                {referralUrl ? (
                  <a
                    href={referralUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="rounded-2xl border border-white/10 px-4 py-3 text-sm font-semibold text-white/80"
                  >
                    Open link
                  </a>
                ) : null}
              </div>
            </>
          ) : (
            <div className="mt-4 text-sm text-white/60">
              Referral slug is not available yet.
            </div>
          )}
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

            {telegramBindLink ? (
              <a
                href={telegramBindLink}
                target="_blank"
                rel="noreferrer"
                className="rounded-2xl border border-white/10 px-4 py-3 text-sm font-semibold text-white/80"
              >
                Bind Telegram
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
