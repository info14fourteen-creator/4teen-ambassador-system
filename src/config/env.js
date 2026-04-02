function requireEnv(name) {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing env: ${name}`);
  }

  return value;
}

module.exports = {
  PORT: Number(process.env.PORT || 3000),
  DATABASE_URL: requireEnv('DATABASE_URL'),
  TRON_FULL_HOST: requireEnv('TRON_FULL_HOST'),
  TRON_PRIVATE_KEY: requireEnv('TRON_PRIVATE_KEY'),
  TRONGRID_API_KEY: requireEnv('TRONGRID_API_KEY'),
  FOURTEEN_TOKEN_CONTRACT: requireEnv('FOURTEEN_TOKEN_CONTRACT'),
  FOURTEEN_CONTROLLER_CONTRACT: requireEnv('FOURTEEN_CONTROLLER_CONTRACT'),
  ADMIN_SYNC_TOKEN: requireEnv('ADMIN_SYNC_TOKEN')
};
