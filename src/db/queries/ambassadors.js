const { pool } = require('../pool');

async function upsertAmbassador(input) {
  const result = await pool.query(
    `
      INSERT INTO ambassadors (
        ambassador_wallet,
        exists_on_chain,
        active,
        self_registered,
        manual_assigned,
        override_enabled,
        current_level,
        override_level,
        effective_level,
        reward_percent,
        slug_hash,
        meta_hash,
        created_at_chain,
        total_buyers,
        total_volume_sun,
        total_rewards_accrued_sun,
        total_rewards_claimed_sun,
        claimable_rewards_sun,
        last_chain_sync_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,NOW()
      )
      ON CONFLICT (ambassador_wallet)
      DO UPDATE SET
        exists_on_chain = EXCLUDED.exists_on_chain,
        active = EXCLUDED.active,
        self_registered = EXCLUDED.self_registered,
        manual_assigned = EXCLUDED.manual_assigned,
        override_enabled = EXCLUDED.override_enabled,
        current_level = EXCLUDED.current_level,
        override_level = EXCLUDED.override_level,
        effective_level = EXCLUDED.effective_level,
        reward_percent = EXCLUDED.reward_percent,
        slug_hash = EXCLUDED.slug_hash,
        meta_hash = EXCLUDED.meta_hash,
        created_at_chain = EXCLUDED.created_at_chain,
        total_buyers = EXCLUDED.total_buyers,
        total_volume_sun = EXCLUDED.total_volume_sun,
        total_rewards_accrued_sun = EXCLUDED.total_rewards_accrued_sun,
        total_rewards_claimed_sun = EXCLUDED.total_rewards_claimed_sun,
        claimable_rewards_sun = EXCLUDED.claimable_rewards_sun,
        last_chain_sync_at = EXCLUDED.last_chain_sync_at,
        updated_at = NOW()
      RETURNING
        ambassador_wallet,
        slug,
        exists_on_chain,
        active,
        self_registered,
        manual_assigned,
        override_enabled,
        current_level,
        override_level,
        effective_level,
        reward_percent,
        slug_hash,
        meta_hash,
        created_at_chain,
        total_buyers,
        total_volume_sun,
        total_rewards_accrued_sun,
        total_rewards_claimed_sun,
        claimable_rewards_sun,
        last_chain_sync_at,
        updated_at
    `,
    [
      input.ambassador_wallet,
      input.exists_on_chain,
      input.active,
      input.self_registered,
      input.manual_assigned,
      input.override_enabled,
      input.current_level,
      input.override_level,
      input.effective_level,
      input.reward_percent,
      input.slug_hash,
      input.meta_hash,
      input.created_at_chain,
      input.total_buyers,
      input.total_volume_sun,
      input.total_rewards_accrued_sun,
      input.total_rewards_claimed_sun,
      input.claimable_rewards_sun,
      input.last_chain_sync_at
    ]
  );

  return result.rows[0];
}

async function getAmbassadorByWallet(ambassadorWallet) {
  const result = await pool.query(
    `
      SELECT
        ambassador_wallet,
        slug,
        exists_on_chain,
        active,
        self_registered,
        manual_assigned,
        override_enabled,
        current_level,
        override_level,
        effective_level,
        reward_percent,
        slug_hash,
        meta_hash,
        created_at_chain,
        total_buyers,
        total_volume_sun,
        total_rewards_accrued_sun,
        total_rewards_claimed_sun,
        claimable_rewards_sun,
        last_chain_sync_at,
        updated_at
      FROM ambassadors
      WHERE ambassador_wallet = $1
      LIMIT 1
    `,
    [ambassadorWallet]
  );

  return result.rows[0] || null;
}

async function setAmbassadorSlug(ambassadorWallet, slug) {
  const result = await pool.query(
    `
      UPDATE ambassadors
      SET slug = $2,
          updated_at = NOW()
      WHERE ambassador_wallet = $1
      RETURNING
        ambassador_wallet,
        slug,
        slug_hash,
        updated_at
    `,
    [ambassadorWallet, slug]
  );

  return result.rows[0] || null;
}

module.exports = {
  upsertAmbassador,
  getAmbassadorByWallet,
  setAmbassadorSlug
};
