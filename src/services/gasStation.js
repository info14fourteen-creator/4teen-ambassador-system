const crypto = require('crypto');
const env = require('../config/env');
const { tronWeb } = require('./tron/client');

const SUN = 1_000_000;
const MIN_OPERATOR_RESERVE_SUN = 2 * SUN;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toBase58Address(value) {
  if (!value) return null;

  try {
    if (typeof value === 'string' && value.startsWith('T')) {
      return value;
    }

    let hex = String(value).toLowerCase();

    if (hex.startsWith('0x')) {
      hex = hex.slice(2);
    }

    if (!hex.startsWith('41')) {
      hex = `41${hex}`;
    }

    return tronWeb.address.fromHex(hex);
  } catch (_) {
    return null;
  }
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

function encryptPayload(payload) {
  const key = Buffer.from(env.GASSTATION_API_SECRET, 'utf8');

  if (key.length !== 16 && key.length !== 24 && key.length !== 32) {
    throw new Error('GASSTATION_API_SECRET must be 16, 24 or 32 bytes for AES');
  }

  const cipher = crypto.createCipheriv(`aes-${key.length * 8}-ecb`, key, null);
  cipher.setAutoPadding(true);

  const plaintext = JSON.stringify({
    ...payload,
    time: String(Math.floor(Date.now() / 1000))
  });

  return Buffer.concat([
    cipher.update(plaintext, 'utf8'),
    cipher.final()
  ]).toString('base64');
}

async function callGasStation(method, path, payload) {
  const encrypted = encryptPayload(payload);
  const url = `${env.GASSTATION_API_BASE_URL}${path}?app_id=${encodeURIComponent(env.GASSTATION_API_KEY)}&data=${encodeURIComponent(encrypted)}`;

  const response = await fetch(url, {
    method,
    headers: {
      Accept: 'application/json'
    }
  });

  const json = await response.json();

  if (!response.ok) {
    throw new Error(`Gas Station HTTP ${response.status}`);
  }

  if (Number(json?.code) !== 0) {
    throw new Error(`Gas Station error ${json?.code}: ${json?.msg || 'Unknown error'}`);
  }

  return json.data;
}

async function getGasStationBalance() {
  return callGasStation('GET', '/api/tron/gas/balance', {});
}

async function getGasStationPrice(resourceType) {
  const data = await callGasStation('GET', '/api/tron/gas/order/price', {
    resource_type: resourceType,
    service_charge_type: env.GASSTATION_SERVICE_CHARGE_TYPE
  });

  const list = Array.isArray(data?.price_builder_list) ? data.price_builder_list : [];
  const selected = list.find(
    (item) => String(item?.service_charge_type || '') === String(env.GASSTATION_SERVICE_CHARGE_TYPE)
  ) || list[0];

  if (!selected) {
    throw new Error(`Gas Station did not return price for ${resourceType}`);
  }

  return {
    resourceType,
    minNumber: Number(data?.min_number || 0),
    maxNumber: Number(data?.max_number || 0),
    serviceChargeType: String(selected?.service_charge_type || env.GASSTATION_SERVICE_CHARGE_TYPE),
    unitPriceSun: Math.ceil(Number(selected?.price || 0)),
    remainingNumber: Number(selected?.remaining_number || 0)
  };
}

function buildRequestId(prefix) {
  return `${prefix}-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
}

async function createGasOrder({ resourceType, quantity, receiveAddress }) {
  const payload = {
    request_id: buildRequestId(resourceType),
    receive_address: receiveAddress,
    buy_type: 0,
    service_charge_type: env.GASSTATION_SERVICE_CHARGE_TYPE
  };

  if (resourceType === 'energy') {
    payload.energy_num = quantity;
  } else if (resourceType === 'net') {
    payload.net_num = quantity;
  } else {
    throw new Error(`Unsupported Gas Station resource type: ${resourceType}`);
  }

  const data = await callGasStation('POST', '/api/tron/gas/create_order', payload);

  return {
    requestId: payload.request_id,
    tradeNo: String(data?.trade_no || data?.tradeNo || '')
  };
}

async function getOrderList(requestIds) {
  return callGasStation('GET', '/api/tron/gas/record/list', {
    request_ids: requestIds.join(',')
  });
}

async function waitForOrderSuccess(requestId, { attempts = 20, delayMs = 3000 } = {}) {
  for (let i = 0; i < attempts; i += 1) {
    const rows = await getOrderList([requestId]);
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

async function topUpGasStationIfNeeded(requiredAmountSun) {
  const gasBalance = await getGasStationBalance();
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

    const reloaded = await getGasStationBalance();
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
  const energyDeficit = Math.max(0, env.GASSTATION_MIN_ENERGY - Number(state.availableEnergy || 0));
  const bandwidthDeficit = Math.max(0, env.GASSTATION_MIN_BANDWIDTH - Number(state.availableBandwidth || 0));

  return {
    needEnergy: energyDeficit > 0,
    needBandwidth: bandwidthDeficit > 0,
    energyQuantity: energyDeficit > 0 ? Math.max(env.GASSTATION_MIN_ENERGY, energyDeficit) : 0,
    bandwidthQuantity: bandwidthDeficit > 0 ? Math.max(env.GASSTATION_MIN_BANDWIDTH, bandwidthDeficit) : 0
  };
}

async function estimateRentalCostSun(orders) {
  let total = 0;
  const details = [];

  if (orders.energyQuantity > 0) {
    const price = await getGasStationPrice('energy');

    if (price.remainingNumber > 0 && orders.energyQuantity > price.remainingNumber) {
      throw new Error('Gas Station energy inventory is insufficient for requested amount');
    }

    const amountSun = Math.ceil(orders.energyQuantity * price.unitPriceSun);

    total += amountSun;
    details.push({
      resourceType: 'energy',
      quantity: orders.energyQuantity,
      unitPriceSun: price.unitPriceSun,
      amountSun
    });
  }

  if (orders.bandwidthQuantity > 0) {
    const price = await getGasStationPrice('net');

    if (price.remainingNumber > 0 && orders.bandwidthQuantity > price.remainingNumber) {
      throw new Error('Gas Station bandwidth inventory is insufficient for requested amount');
    }

    const amountSun = Math.ceil(orders.bandwidthQuantity * price.unitPriceSun);

    total += amountSun;
    details.push({
      resourceType: 'net',
      quantity: orders.bandwidthQuantity,
      unitPriceSun: price.unitPriceSun,
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

  if (!env.GASSTATION_ENABLED) {
    throw new Error('Gas Station is disabled but operator resources are insufficient');
  }

  const estimate = await estimateRentalCostSun(orders);
  const topUp = await topUpGasStationIfNeeded(estimate.totalAmountSun);

  const createdOrders = [];

  if (orders.energyQuantity > 0) {
    const created = await createGasOrder({
      resourceType: 'energy',
      quantity: orders.energyQuantity,
      receiveAddress: env.OPERATOR_WALLET
    });

    const finalRow = await waitForOrderSuccess(created.requestId);

    createdOrders.push({
      resourceType: 'energy',
      requestId: created.requestId,
      tradeNo: created.tradeNo,
      quantity: orders.energyQuantity,
      finalStatus: Number(finalRow?.status || 0),
      row: finalRow
    });
  }

  if (orders.bandwidthQuantity > 0) {
    const created = await createGasOrder({
      resourceType: 'net',
      quantity: orders.bandwidthQuantity,
      receiveAddress: env.OPERATOR_WALLET
    });

    const finalRow = await waitForOrderSuccess(created.requestId);

    createdOrders.push({
      resourceType: 'net',
      requestId: created.requestId,
      tradeNo: created.tradeNo,
      quantity: orders.bandwidthQuantity,
      finalStatus: Number(finalRow?.status || 0),
      row: finalRow
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

module.exports = {
  ensureOperatorResources
};
