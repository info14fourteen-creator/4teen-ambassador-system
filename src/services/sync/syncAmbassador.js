const { upsertAmbassador } = require('../../db/queries/ambassadors');
const { readAmbassadorDashboard } = require('../tron/controller');

async function syncAmbassador(ambassadorWallet) {
  const { core, stats, profile, payout } = await readAmbassadorDashboard(ambassadorWallet);

  await upsertAmbassador({
    ambassadorWallet,
    slugHash: String(profile.slugHash || ''),
    metaHash: String(profile.metaHash || ''),
    existsOnChain: Boolean(core.exists),
    active: Boolean(core.active),
    selfRegistered: Boolean(profile.selfRegistered),
    manualAssigned: Boolean(profile.manualAssigned),
    overrideEnabled: Boolean(profile.overrideEnabled),
    currentLevel: Number(profile.currentLevel || 0),
    overrideLevel: Number(profile.overrideLevel || 0),
    effectiveLevel: Number(core.effectiveLevel || 0),
    rewardPercent: String(core.rewardPercent || 0),
    createdAtChain: Number(core.createdAt || 0),
    totalBuyers: String(stats.totalBuyers || 0),
    totalVolumeSun: String(stats.totalVolumeSun || 0),
    totalRewardsAccruedSun: String(stats.totalRewardsAccruedSun || 0),
    totalRewardsClaimedSun: String(payout.totalRewardsClaimedSun || 0),
    claimableRewardsSun: String(payout.claimableRewardsSun || 0)
  });

  return {
    ok: true,
    ambassadorWallet
  };
}

module.exports = {
  syncAmbassador
};
