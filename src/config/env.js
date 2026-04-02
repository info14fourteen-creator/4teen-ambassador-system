require('dotenv').config();

function requireEnv(name) {
  const value = process.env[name];

  if (value == null || String(value).trim() === '') {
    throw new Error(`Missing env: ${name}`);
  }

  return String(value).trim();
}

function optionalEnv(name, fallback = '') {
  const value = process.env[name];

  if (value == null) {
    return fallback;
  }

  const normalized = String(value).trim();
  return normalized || fallback;
}

module.exports = {
  PORT: Number(optionalEnv('PORT', '3000')),
  DATABASE_URL: requireEnv('DATABASE_URL'),
  TRON_FULL_HOST: requireEnv('TRON_FULL_HOST'),
  TRON_PRIVATE_KEY: requireEnv('TRON_PRIVATE_KEY'),
  FOURTEEN_TOKEN_CONTRACT: requireEnv('FOURTEEN_TOKEN_CONTRACT'),
  FOURTEEN_CONTROLLER_CONTRACT: requireEnv('FOURTEEN_CONTROLLER_CONTRACT'),
  ADMIN_SYNC_TOKEN: requireEnv('ADMIN_SYNC_TOKEN'),
  ALLOWED_ORIGINS: optionalEnv('ALLOWED_ORIGINS'),
  TRONGRID_API_KEY: optionalEnv('TRONGRID_API_KEY')
};
