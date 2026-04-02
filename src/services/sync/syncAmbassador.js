const { upsertAmbassador } = require('../../db/queries/ambassadors');
const { readAmbassadorDashboard } = require('../tron/controller');

function toBool(value) {
  return Boolean(value);
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toText(value, fallback = '0') {
  if (value == null) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

async function syncAmbassador(ambassadorWallet) {
  const { core, stats, profile, payout } = await readAmbassadorDashboard(ambassadorWallet);

  await upsertAmbassador({
    ambassador_wallet: ambassadorWallet,
    slug: null,
    slug_hash: profile?.slugHash != null ? String(profile.slugHash) : null,
    meta_hash: profile?.metaHash != null ? String(profile.metaHash) : null,
    exists_on_chain: toBool(core?.exists),
    active: toBool(core?.active),
    self_registered: toBool(profile?.selfRegistered),
    manual_assigned: toBool(profile?.manualAssigned),
    override_enabled: toBool(profile?.overrideEnabled),
    current_level: toNumber(profile?.currentLevel, 0),
    override_level: toNumber(profile?.overrideLevel, 0),
    effective_level: toNumber(core?.effectiveLevel, 0),
    reward_percent: toText(core?.rewardPercent, '0'),
    created_at_chain: toNumber(core?.createdAt, 0),
    total_buyers: toText(stats?.totalBuyers, '0'),
    total_volume_sun: toText(stats?.totalVolumeSun, '0'),
    total_rewards_accrued_sun: toText(
      payout?.totalRewardsAccruedSun ?? stats?.totalRewardsAccruedSun,
      '0'
    ),
    total_rewards_claimed_sun: toText(
      payout?.totalRewardsClaimedSun ?? stats?.totalRewardsClaimedSun,
      '0'
    ),
    claimable_rewards_sun: toText(
      payout?.claimableRewardsSun ?? stats?.claimableRewardsSun,
      '0'
    ),
    last_chain_sync_at: new Date().toISOString()
  });

  return {
    ok: true,
    ambassadorWallet
  };
}

module.exports = {
  syncAmbassador
};
