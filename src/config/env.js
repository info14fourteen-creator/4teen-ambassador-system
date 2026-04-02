function requireEnv(name) {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing env: ${name}`);
  }

  return value;
}

function optionalNumber(name, fallback) {
  const raw = process.env[name];

  if (raw === undefined || raw === null || raw === '') {
    return fallback;
  }

  const value = Number(raw);

  if (!Number.isFinite(value)) {
    throw new Error(`Invalid numeric env: ${name}`);
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
  ADMIN_SYNC_TOKEN: requireEnv('ADMIN_SYNC_TOKEN'),

  OPERATOR_WALLET: process.env.OPERATOR_WALLET || 'TN95o1fsA7mNwJGYGedvf3y7DJZKLH6TCT',

  GASSTATION_ENABLED: String(process.env.GASSTATION_ENABLED || 'true').toLowerCase() === 'true',
  GASSTATION_API_BASE_URL: requireEnv('GASSTATION_API_BASE_URL'),
  GASSTATION_API_KEY: requireEnv('GASSTATION_API_KEY'),
  GASSTATION_API_SECRET: requireEnv('GASSTATION_API_SECRET'),
  GASSTATION_MIN_ENERGY: optionalNumber('GASSTATION_MIN_ENERGY', 64400),
  GASSTATION_MIN_BANDWIDTH: optionalNumber('GASSTATION_MIN_BANDWIDTH', 5000),
  GASSTATION_SERVICE_CHARGE_TYPE: String(process.env.GASSTATION_SERVICE_CHARGE_TYPE || '10010'),
  GASSTATION_RENT_MINUTES: optionalNumber('GASSTATION_RENT_MINUTES', 10),
  CONTROLLER_FEE_LIMIT_SUN: optionalNumber('CONTROLLER_FEE_LIMIT_SUN', 300000000)
};
