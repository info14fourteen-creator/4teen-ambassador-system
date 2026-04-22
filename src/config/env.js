function requireEnv(name) {
  const value = process.env[name];

  if (value === undefined || value === null || String(value).trim() === '') {
    throw new Error(`Missing env: ${name}`);
  }

  return String(value).trim();
}

function optionalEnv(name, fallback = '') {
  const value = process.env[name];

  if (value === undefined || value === null) {
    return fallback;
  }

  const normalized = String(value).trim();
  return normalized === '' ? fallback : normalized;
}

function numberEnv(name, fallback) {
  const raw = process.env[name];

  if (raw === undefined || raw === null || String(raw).trim() === '') {
    return fallback;
  }

  const parsed = Number(raw);

  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid number env: ${name}`);
  }

  return parsed;
}

module.exports = {
  PORT: numberEnv('PORT', 3000),

  DATABASE_URL: requireEnv('DATABASE_URL'),
  TRON_FULL_HOST: requireEnv('TRON_FULL_HOST'),
  TRON_PRIVATE_KEY: requireEnv('TRON_PRIVATE_KEY'),
  TRONGRID_API_KEY: optionalEnv('TRONGRID_API_KEY', ''),
  TRONGRID_API_KEY_1: optionalEnv('TRONGRID_API_KEY_1', ''),
  TRONGRID_API_KEY_2: optionalEnv('TRONGRID_API_KEY_2', ''),
  TRONGRID_API_KEY_3: optionalEnv('TRONGRID_API_KEY_3', ''),
  TRONSCAN_API_KEY: optionalEnv('TRONSCAN_API_KEY', ''),
  TRONSCAN_API_KEY_1: optionalEnv('TRONSCAN_API_KEY_1', ''),
  TRONSCAN_API_KEY_2: optionalEnv('TRONSCAN_API_KEY_2', ''),
  TRONSCAN_API_KEY_3: optionalEnv('TRONSCAN_API_KEY_3', ''),
  CMC_API_KEY: optionalEnv('CMC_API_KEY', ''),
  CMC_API_KEY_1: optionalEnv('CMC_API_KEY_1', ''),
  CMC_API_KEY_2: optionalEnv('CMC_API_KEY_2', ''),
  CMC_API_KEY_3: optionalEnv('CMC_API_KEY_3', ''),

  FOURTEEN_TOKEN_CONTRACT: requireEnv('FOURTEEN_TOKEN_CONTRACT'),
  FOURTEEN_CONTROLLER_CONTRACT: requireEnv('FOURTEEN_CONTROLLER_CONTRACT'),

  ADMIN_SYNC_TOKEN: optionalEnv('ADMIN_SYNC_TOKEN', ''),
  CRON_SECRET: optionalEnv('CRON_SECRET', ''),

  ALLOWED_ORIGINS: optionalEnv(
    'ALLOWED_ORIGINS',
    'https://4teen.me,https://www.4teen.me,http://localhost:3000,http://127.0.0.1:3000'
  ),

  CONTROLLER_FEE_LIMIT_SUN: numberEnv('CONTROLLER_FEE_LIMIT_SUN', 300000000),

  GASSTATION_ENABLED: optionalEnv('GASSTATION_ENABLED', 'false'),
  GASSTATION_API_BASE_URL: optionalEnv('GASSTATION_API_BASE_URL', 'https://openapi.gasstation.ai'),
  GASSTATION_API_KEY: optionalEnv('GASSTATION_API_KEY', ''),
  GASSTATION_API_SECRET: optionalEnv('GASSTATION_API_SECRET', ''),
  GASSTATION_SERVICE_CHARGE_TYPE: optionalEnv('GASSTATION_SERVICE_CHARGE_TYPE', '10010'),
  GASSTATION_MIN_BANDWIDTH: numberEnv('GASSTATION_MIN_BANDWIDTH', 5000),
  GASSTATION_MIN_ENERGY: numberEnv('GASSTATION_MIN_ENERGY', 64400),
  GASSTATION_REGISTRATION_ENERGY: numberEnv('GASSTATION_REGISTRATION_ENERGY', 100000),
  QUOTAGUARDSTATIC_URL: optionalEnv('QUOTAGUARDSTATIC_URL', ''),
  OPERATOR_WALLET: optionalEnv('OPERATOR_WALLET', 'TN95o1fsA7mNwJGYGedvf3y7DJZKLH6TCT'),

  ALLOCATION_MIN_BANDWIDTH: numberEnv('ALLOCATION_MIN_BANDWIDTH', 500),
  ALLOCATION_MIN_ENERGY: numberEnv('ALLOCATION_MIN_ENERGY', 70000),

  OWNER_AUTO_WITHDRAW_ENABLED: optionalEnv('OWNER_AUTO_WITHDRAW_ENABLED', 'false'),
  OWNER_WITHDRAW_FEE_LIMIT_SUN: numberEnv('OWNER_WITHDRAW_FEE_LIMIT_SUN', 300000000),
  OWNER_WITHDRAW_MIN_SUN: numberEnv('OWNER_WITHDRAW_MIN_SUN', 1000000),

  SCAN_PAGE_SIZE: numberEnv('SCAN_PAGE_SIZE', 50)
};
