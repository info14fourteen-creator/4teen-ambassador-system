const { pool } = require('../pool');

async function getSyncState(key, fallback = '0', client = pool) {
  const result = await client.query(
    `
      SELECT state_value
      FROM sync_state
      WHERE state_key = $1
      LIMIT 1
    `,
    [key]
  );

  return result.rows[0]?.state_value || fallback;
}

async function setSyncState(key, value, client = pool) {
  await client.query(
    `
      INSERT INTO sync_state (state_key, state_value, updated_at)
      VALUES ($1,$2,NOW())
      ON CONFLICT (state_key)
      DO UPDATE SET
        state_value = EXCLUDED.state_value,
        updated_at = NOW()
    `,
    [key, String(value)]
  );
}

module.exports = {
  getSyncState,
  setSyncState
};
