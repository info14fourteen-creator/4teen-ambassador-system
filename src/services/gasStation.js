const crypto = require('crypto');
const { ProxyAgent, fetch: undiciFetch } = require('undici');
const env = require('../config/env');
const { tronWeb } = require('./tron/client');

const SUN = 1_000_000;
const MIN_OPERATOR_RESERVE_SUN = 2 * SUN;
const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_BASE_URL = 'https://openapi.gasstation.ai';
const DEFAULT_SERVICE_CHARGE_TYPE = '10010';
const MIN_ENERGY_ORDER = 64400;
const MIN_BANDWIDTH_ORDER = 5000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function assertNonEmpty(value, fieldName) {
  const normalized = String(value ?? '').trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function normalizeBaseUrl(value) {
  return String(value || DEFAULT_BASE_URL).trim().replace(/\/+$/, '');
}

function normalizeTimeoutMs(value) {
  const parsed = Number(value ?? DEFAULT_TIMEOUT_MS);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_TIMEOUT_MS;
  }

  return Math.max(Math.floor(parsed), 1000);
}

function normalizePositiveInteger(value, fieldName) {
  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${fieldName} must be a positive number`);
  }

  return Math.ceil(parsed);
}

function safeFiniteNumber(value, fallback = 0) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'bigint') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value.trim());
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  return fallback;
}

function toSun(value) {
  const num = Number(value || 0);

  if (!Number.isFinite(num) || num <= 0) {
    return 0;
  }

  return Math.floor(num * SUN);
}

function fromSun(value) {
  const num = Number(value || 0);

  if (!Number.isFinite(num)) {
    return 0;
  }

  return num / SUN;
}

function pkcs7Pad(buffer) {
  const blockSize = 16;
  const remainder = buffer.length % blockSize;
  const padLength = remainder === 0 ? blockSize : blockSize - remainder;
  const padding = Buffer.alloc(padLength, padLength);
  return Buffer.concat([buffer, padding]);
}

function encryptAesEcbPkcs7Base64(plainText, secretKey) {
  const key = Buffer.from(assertNonEmpty(secretKey, 'GASSTATION_API_SECRET'), 'utf8');

  if (![16, 24, 32].includes(key.length)) {
    throw new Error('GASSTATION_API_SECRET must be 16, 24, or 32 bytes long');
  }

  const plainBuffer = Buffer.from(plainText, 'utf8');
  const padded = pkcs7Pad(plainBuffer);

  const cipher = crypto.createCipheriv(`aes-${key.length * 8}-ecb`, key, null);
  cipher.setAutoPadding(false);

  const encrypted = Buffer.concat([cipher.update(padded), cipher.final()]);
  return encrypted.toString('base64');
}

async function requestJson({
  url,
  method = 'GET',
  proxyUrl,
  timeoutMs = DEFAULT_TIMEOUT_MS
}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const dispatcher = proxyUrl ? new ProxyAgent(proxyUrl) : undefined;

  try {
    const response = await undiciFetch(url, {
      method,
      headers: {
        Accept: 'application/json'
      },
      signal: controller.signal,
      dispatcher
    });

    const rawBody = await response.text();
    let parsed = null;

    try {
      parsed = rawBody ? JSON.parse(rawBody) : null;
    } catch (_) {
      throw new Error(
        `GasStation returned non-JSON response: status=${response.status}; body=${rawBody.slice(0, 300)}`
      );
    }

    if (!response.ok) {
      const apiMessage =
        parsed && typeof parsed.msg === 'string' && parsed.msg.trim()
          ? parsed.msg.trim()
          : null;

      throw new Error(
        apiMessage
          ? `GasStation HTTP ${response.status}: ${apiMessage}`
          : `GasStation HTTP ${response.status}; body=${rawBody.slice(0, 300)}`
      );
    }

    if (!parsed || typeof parsed !== 'object') {
      throw new Error('GasStation returned invalid response object');
    }

    const apiCode = safeFiniteNumber(parsed.code, NaN);

    if (!Number.isFinite(apiCode)) {
      throw new Error('GasStation response code is missing or invalid');
    }

    if (apiCode !== 0) {
      const apiMessage =
        typeof parsed.msg === 'string' && parsed.msg.trim()
          ? parsed.msg.trim()
          : `GasStation error ${apiCode}`;

      throw new Error(`GasStation error ${apiCode}: ${apiMessage}`);
    }

    return parsed.data ?? null;
  } catch (error) {
    if (error?.name === 'AbortError') {
      throw new Error('GasStation request timed out');
    }

    const message =
      error && typeof error.message === 'string' && error.message.trim()
        ? error.message.trim()
        : 'fetch failed';

    throw new Error(`GasStation fetch failed: ${message}`);
  } finally {
    clearTimeout(timer);

    if (dispatcher) {
      try {
        await dispatcher.close();
      } catch (_) {}
    }
  }
}

class GasStationClient {
  constructor(config) {
    this.appId = assertNonEmpty(config.appId, 'GASSTATION_API_KEY');
    this.secretKey = assertNonEmpty(config.secretKey, 'GASSTATION_API_SECRET');
    this.baseUrl = normalizeBaseUrl(config.baseUrl);
    this.proxyUrl = config.proxyUrl ? String(config.proxyUrl).trim() : undefined;
    this.timeoutMs = normalizeTimeoutMs(config.timeoutMs);
  }

  buildEncryptedUrl(path, payload) {
    const plainText = JSON.stringify(payload);
    const encrypted = encryptAesEcbPkcs7Base64(plainText, this.secretKey);

    const url = new URL(`${this.baseUrl}${path}`);
    url.searchParams.set('app_id', this.appId);
    url.searchParams.set('data', encrypted);

    return url.toString();
  }

  async getJson(path, payload, method = 'GET') {
    const url = this.buildEncryptedUrl(path, payload);

    return requestJson({
      url,
      method,
      proxyUrl: this.proxyUrl,
      timeoutMs: this.timeoutMs
    });
  }

  async getBalance() {
    return this.getJson(
      '/api/mpc/tron/gas/balance',
      {
        time: String(Math.floor(Date.now() / 1000))
      },
      'GET'
    );
  }

  async getPrice(resourceValue) {
    const payload = {
      service_charge_type: String(env.GASSTATION_SERVICE_CHARGE_TYPE || DEFAULT_SERVICE_CHARGE_TYPE)
    };

    if (resourceValue != null) {
      payload.value = normalizePositiveInteger(resourceValue, 'resourceValue');
    }

    return this.getJson('/api/tron/gas/order/price', payload, 'GET');
  }

  async createEnergyOrder({ requestId, receiveAddress, energyNum }) {
    const normalizedEnergyNum = normalizePositiveInteger(energyNum, 'energyNum');

    if (normalizedEnergyNum < MIN_ENERGY_ORDER) {
      throw new Error(`energyNum must be at least ${MIN_ENERGY_ORDER}`);
    }

    return this.getJson(
      '/api/tron/gas/create_order',
      {
        request_id: assertNonEmpty(requestId, 'requestId'),
        receive_address: assertNonEmpty(receiveAddress, 'receiveAddress'),
        buy_type: 0,
        service_charge_type: String(env.GASSTATION_SERVICE_CHARGE_TYPE || DEFAULT_SERVICE_CHARGE_TYPE),
        energy_num: normalizedEnergyNum
      },
      'POST'
    );
  }

  async createBandwidthOrder({ requestId, receiveAddress, netNum }) {
    const normalizedNetNum = normalizePositiveInteger(netNum, 'netNum');

    if (normalizedNetNum < MIN_BANDWIDTH_ORDER) {
      throw new Error(`netNum must be at least ${MIN_BANDWIDTH_ORDER}`);
    }

    return this.getJson(
      '/api/tron/gas/create_order',
      {
        request_id: assertNonEmpty(requestId, 'requestId'),
        receive_address: assertNonEmpty(receiveAddress, 'receiveAddress'),
        buy_type: 0,
        service_charge_type: String(env.GASSTATION_SERVICE_CHARGE_TYPE || DEFAULT_SERVICE_CHARGE_TYPE),
        net_num: normalizedNetNum
      },
      'POST'
    );
  }

  async getOrderList(requestIds) {
    return this.getJson(
      '/api/tron/gas/record/list',
      {
        request_ids: requestIds.join(',')
      },
      'GET'
    );
  }
}

function createGasStationClientFromEnv() {
  return new GasStationClient({
    appId: env.GASSTATION_API_KEY,
    secretKey: env.GASSTATION_API_SECRET,
    baseUrl: env.GASSTATION_API_BASE_URL,
    proxyUrl: env.QUOTAGUARDSTATIC_URL,
    timeoutMs: Number(process.env.GASSTATION_TIMEOUT_MS || DEFAULT_TIMEOUT_MS)
  });
}

function buildRequestId(prefix) {
  return `${prefix}-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
}

async function waitForOrderSuccess(client, requestId, { attempts = 20, delayMs = 3000 } = {}) {
  for (let i = 0; i < attempts; i += 1) {
    const rows = await client.getOrderList([requestId]);
    const row = Array.isArray(rows) ? rows[0] : null;

    if (row) {
      const status = Number(row.status);

      if (status === 1 || status === 10) {
        return row;
      }

      if (status === 2) {
        throw new Error(`Gas Station order failed for request_id=${requestId}`);
      }
    }

    await sleep(delayMs);
  }

  throw new Error(`Gas Station order timeout for request_id=${requestId}`);
}

async function getOperatorState() {
  const account = await tronWeb.trx.getAccount(env.OPERATOR_WALLET);
  const resources = await tronWeb.trx.getAccountResources(env.OPERATOR_WALLET);
  const balanceSun = Number(await tronWeb.trx.getBalance(env.OPERATOR_WALLET) || 0);

  const freeNetLimit = Number(account?.freeNetLimit || 0);
  const freeNetUsed = Number(account?.freeNetUsed || 0);
  const netLimit = Number(account?.NetLimit || 0);
  const netUsed = Number(account?.NetUsed || 0);

  const energyLimit = Number(resources?.EnergyLimit || 0);
  const energyUsed = Number(resources?.EnergyUsed || 0);

  const availableBandwidth = Math.max(0, freeNetLimit - freeNetUsed) + Math.max(0, netLimit - netUsed);
  const availableEnergy = Math.max(0, energyLimit - energyUsed);

  return {
    wallet: env.OPERATOR_WALLET,
    balanceSun,
    balanceTrx: fromSun(balanceSun),
    availableEnergy,
    availableBandwidth
  };
}

async function sendTrx(toAddress, amountSun) {
  const unsignedTx = await tronWeb.transactionBuilder.sendTrx(
    toAddress,
    amountSun,
    env.OPERATOR_WALLET
  );

  const signedTx = await tronWeb.trx.sign(unsignedTx, env.TRON_PRIVATE_KEY);
  const broadcast = await tronWeb.trx.sendRawTransaction(signedTx);

  if (!broadcast?.result) {
    throw new Error('Failed to top up Gas Station deposit address');
  }

  return String(broadcast.txid || signedTx.txID || '');
}

async function topUpGasStationIfNeeded(client, requiredAmountSun) {
  const gasBalance = await client.getBalance();
  const currentGasBalanceSun = toSun(gasBalance?.balance || 0);

  if (currentGasBalanceSun >= requiredAmountSun) {
    return {
      toppedUp: false,
      gasStationBalanceSun: currentGasBalanceSun,
      depositAddress: gasBalance?.deposit_address || null,
      topUpTxHash: null
    };
  }

  const operator = await getOperatorState();
  const availableForTopUpSun = Math.max(0, operator.balanceSun - MIN_OPERATOR_RESERVE_SUN);

  if (availableForTopUpSun <= 0) {
    throw new Error('Operator wallet does not have enough TRX to top up Gas Station balance');
  }

  const depositAddress = String(gasBalance?.deposit_address || '');

  if (!depositAddress) {
    throw new Error('Gas Station did not return deposit_address');
  }

  const topUpTxHash = await sendTrx(depositAddress, availableForTopUpSun);

  for (let i = 0; i < 15; i += 1) {
    await sleep(3000);

    const reloaded = await client.getBalance();
    const reloadedSun = toSun(reloaded?.balance || 0);

    if (reloadedSun >= requiredAmountSun) {
      return {
        toppedUp: true,
        gasStationBalanceSun: reloadedSun,
        depositAddress,
        topUpTxHash
      };
    }
  }

  throw new Error('Gas Station balance did not update after top up');
}

function computeRequiredOrders(state) {
  const minEnergy = Number(env.GASSTATION_MIN_ENERGY || 64400);
  const minBandwidth = Number(env.GASSTATION_MIN_BANDWIDTH || 5000);

  const energyDeficit = Math.max(0, minEnergy - Number(state.availableEnergy || 0));
  const bandwidthDeficit = Math.max(0, minBandwidth - Number(state.availableBandwidth || 0));

  return {
    needEnergy: energyDeficit > 0,
    needBandwidth: bandwidthDeficit > 0,
    energyQuantity: energyDeficit > 0 ? Math.max(minEnergy, energyDeficit) : 0,
    bandwidthQuantity: bandwidthDeficit > 0 ? Math.max(minBandwidth, bandwidthDeficit) : 0
  };
}

async function estimateRentalCostSun(client, orders) {
  let total = 0;
  const details = [];

  if (orders.energyQuantity > 0) {
    const priceData = await client.getPrice(orders.energyQuantity);
    const list = Array.isArray(priceData?.price_builder_list)
      ? priceData.price_builder_list
      : Array.isArray(priceData?.list)
        ? priceData.list
        : [];

    const selected = list.find(
      (item) => String(item?.service_charge_type || '') === String(env.GASSTATION_SERVICE_CHARGE_TYPE || DEFAULT_SERVICE_CHARGE_TYPE)
    ) || list[0];

    if (!selected) {
      throw new Error('Gas Station did not return energy price');
    }

    const unitPriceSun = Math.ceil(Number(selected?.price || 0));
    const remainingNumber = Number(selected?.remaining_number || 0);

    if (remainingNumber > 0 && orders.energyQuantity > remainingNumber) {
      throw new Error('Gas Station energy inventory is insufficient for requested amount');
    }

    const amountSun = Math.ceil(orders.energyQuantity * unitPriceSun);

    total += amountSun;
    details.push({
      resourceType: 'energy',
      quantity: orders.energyQuantity,
      unitPriceSun,
      amountSun
    });
  }

  if (orders.bandwidthQuantity > 0) {
    const priceData = await client.getPrice(orders.bandwidthQuantity);
    const list = Array.isArray(priceData?.price_builder_list)
      ? priceData.price_builder_list
      : Array.isArray(priceData?.list)
        ? priceData.list
        : [];

    const selected = list.find(
      (item) => String(item?.service_charge_type || '') === String(env.GASSTATION_SERVICE_CHARGE_TYPE || DEFAULT_SERVICE_CHARGE_TYPE)
    ) || list[0];

    if (!selected) {
      throw new Error('Gas Station did not return bandwidth price');
    }

    const unitPriceSun = Math.ceil(Number(selected?.price || 0));
    const remainingNumber = Number(selected?.remaining_number || 0);

    if (remainingNumber > 0 && orders.bandwidthQuantity > remainingNumber) {
      throw new Error('Gas Station bandwidth inventory is insufficient for requested amount');
    }

    const amountSun = Math.ceil(orders.bandwidthQuantity * unitPriceSun);

    total += amountSun;
    details.push({
      resourceType: 'net',
      quantity: orders.bandwidthQuantity,
      unitPriceSun,
      amountSun
    });
  }

  return {
    totalAmountSun: total,
    details
  };
}

async function ensureOperatorResources() {
  const before = await getOperatorState();
  const orders = computeRequiredOrders(before);

  if (!orders.needEnergy && !orders.needBandwidth) {
    return {
      rented: false,
      before,
      after: before,
      orders: [],
      topUp: null
    };
  }

  if (!String(env.GASSTATION_ENABLED).toLowerCase().includes('true')) {
    throw new Error('Gas Station is disabled but operator resources are insufficient');
  }

  const client = createGasStationClientFromEnv();
  const estimate = await estimateRentalCostSun(client, orders);
  const topUp = await topUpGasStationIfNeeded(client, estimate.totalAmountSun);

  const createdOrders = [];

  if (orders.energyQuantity > 0) {
    const requestId = buildRequestId('energy');
    const created = await client.createEnergyOrder({
      requestId,
      receiveAddress: env.OPERATOR_WALLET,
      energyNum: orders.energyQuantity
    });

    const finalRow = await waitForOrderSuccess(client, requestId);

    createdOrders.push({
      resourceType: 'energy',
      requestId,
      tradeNo: String(created?.trade_no || ''),
      quantity: orders.energyQuantity,
      row: finalRow || null
    });
  }

  if (orders.bandwidthQuantity > 0) {
    const requestId = buildRequestId('net');
    const created = await client.createBandwidthOrder({
      requestId,
      receiveAddress: env.OPERATOR_WALLET,
      netNum: orders.bandwidthQuantity
    });

    const finalRow = await waitForOrderSuccess(client, requestId);

    createdOrders.push({
      resourceType: 'net',
      requestId,
      tradeNo: String(created?.trade_no || ''),
      quantity: orders.bandwidthQuantity,
      row: finalRow || null
    });
  }

  await sleep(2000);

  const after = await getOperatorState();

  return {
    rented: true,
    before,
    after,
    orders: createdOrders,
    topUp
  };
}

async function quoteEnergyRental({ energyNum }) {
  const normalizedEnergyNum = normalizePositiveInteger(energyNum, 'energyNum');
  const client = createGasStationClientFromEnv();
  const estimate = await estimateRentalCostSun(client, {
    energyQuantity: normalizedEnergyNum,
    bandwidthQuantity: 0
  });

  return {
    energyQuantity: normalizedEnergyNum,
    amountSun: estimate.totalAmountSun,
    amountTrx: fromSun(estimate.totalAmountSun),
    estimate
  };
}

async function rentEnergyForWallet({ receiveAddress, energyNum, requestPrefix = 'energy' }) {
  if (!String(env.GASSTATION_ENABLED).toLowerCase().includes('true')) {
    throw new Error('Gas Station is disabled');
  }

  const normalizedEnergyNum = normalizePositiveInteger(energyNum, 'energyNum');
  const client = createGasStationClientFromEnv();
  const estimate = await estimateRentalCostSun(client, {
    energyQuantity: normalizedEnergyNum,
    bandwidthQuantity: 0
  });
  const topUp = await topUpGasStationIfNeeded(client, estimate.totalAmountSun);
  const requestId = buildRequestId(requestPrefix);
  const created = await client.createEnergyOrder({
    requestId,
    receiveAddress: assertNonEmpty(receiveAddress, 'receiveAddress'),
    energyNum: normalizedEnergyNum
  });
  const finalRow = await waitForOrderSuccess(client, requestId);

  return {
    rented: true,
    resourceType: 'energy',
    requestId,
    tradeNo: String(created?.trade_no || ''),
    quantity: normalizedEnergyNum,
    row: finalRow || null,
    estimate,
    topUp
  };
}

module.exports = {
  ensureOperatorResources,
  quoteEnergyRental,
  rentEnergyForWallet
};
