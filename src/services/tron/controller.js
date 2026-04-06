const env = require('../../config/env');
const { tronWeb } = require('./client');
const { zeroAddressToNull } = require('./utils');

const controllerAbi = [
  {
    inputs: [{ internalType: 'address', name: 'buyer', type: 'address' }],
    name: 'getBuyerAmbassador',
    outputs: [{ internalType: 'address', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function'
  },
  {
    inputs: [{ internalType: 'bytes32', name: 'purchaseId', type: 'bytes32' }],
    name: 'isPurchaseProcessed',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function'
  },
  {
    inputs: [{ internalType: 'bytes32', name: 'slugHash', type: 'bytes32' }],
    name: 'getAmbassadorBySlugHash',
    outputs: [{ internalType: 'address', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function'
  },
  {
    inputs: [
      { internalType: 'bytes32', name: 'purchaseId', type: 'bytes32' },
      { internalType: 'address', name: 'buyer', type: 'address' },
      { internalType: 'address', name: 'ambassadorCandidate', type: 'address' },
      { internalType: 'uint256', name: 'purchaseAmountSun', type: 'uint256' },
      { internalType: 'uint256', name: 'ownerShareSun', type: 'uint256' }
    ],
    name: 'recordVerifiedPurchase',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function'
  },
  {
    inputs: [{ internalType: 'address', name: 'ambassadorAddress', type: 'address' }],
    name: 'getDashboardCore',
    outputs: [
      { internalType: 'bool', name: 'exists', type: 'bool' },
      { internalType: 'bool', name: 'active', type: 'bool' },
      { internalType: 'uint8', name: 'effectiveLevel', type: 'uint8' },
      { internalType: 'uint256', name: 'rewardPercent', type: 'uint256' },
      { internalType: 'uint256', name: 'createdAt', type: 'uint256' }
    ],
    stateMutability: 'view',
    type: 'function'
  },
  {
    inputs: [{ internalType: 'address', name: 'ambassadorAddress', type: 'address' }],
    name: 'getDashboardStats',
    outputs: [
      { internalType: 'uint256', name: 'totalBuyers', type: 'uint256' },
      { internalType: 'uint256', name: 'totalVolumeSun', type: 'uint256' },
      { internalType: 'uint256', name: 'totalRewardsAccruedSun', type: 'uint256' },
      { internalType: 'uint256', name: 'totalRewardsClaimedSun', type: 'uint256' },
      { internalType: 'uint256', name: 'claimableRewardsSun', type: 'uint256' }
    ],
    stateMutability: 'view',
    type: 'function'
  },
  {
    inputs: [{ internalType: 'address', name: 'ambassadorAddress', type: 'address' }],
    name: 'getDashboardProfile',
    outputs: [
      { internalType: 'bool', name: 'selfRegistered', type: 'bool' },
      { internalType: 'bool', name: 'manualAssigned', type: 'bool' },
      { internalType: 'bool', name: 'overrideEnabled', type: 'bool' },
      { internalType: 'uint8', name: 'currentLevel', type: 'uint8' },
      { internalType: 'uint8', name: 'overrideLevel', type: 'uint8' },
      { internalType: 'bytes32', name: 'slugHash', type: 'bytes32' },
      { internalType: 'bytes32', name: 'metaHash', type: 'bytes32' }
    ],
    stateMutability: 'view',
    type: 'function'
  },
  {
    inputs: [{ internalType: 'address', name: 'ambassadorAddress', type: 'address' }],
    name: 'getAmbassadorPayoutData',
    outputs: [
      { internalType: 'uint256', name: 'claimableRewardsSun', type: 'uint256' },
      { internalType: 'uint256', name: 'totalRewardsAccruedSun', type: 'uint256' },
      { internalType: 'uint256', name: 'totalRewardsClaimedSun', type: 'uint256' }
    ],
    stateMutability: 'view',
    type: 'function'
  }
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toBase58Address(value) {
  if (!value) return null;

  try {
    if (typeof value === 'string' && value.startsWith('T')) {
      return value;
    }

    let hex = String(value).trim().toLowerCase();

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

function normalizeEventList(response) {
  if (Array.isArray(response)) {
    return response;
  }

  if (Array.isArray(response?.data)) {
    return response.data;
  }

  return [];
}

function normalizeBytes32(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/^0x/, '');
}

function assertBytes32Hex(value, fieldName = 'bytes32 value') {
  const normalized = normalizeBytes32(value);

  if (!/^[0-9a-f]{64}$/.test(normalized)) {
    throw new Error(`${fieldName} must be a 32-byte hex string`);
  }

  return `0x${normalized}`;
}

async function getControllerContract() {
  return tronWeb.contract(controllerAbi, env.FOURTEEN_CONTROLLER_CONTRACT);
}

async function getBuyerAmbassador(buyerWallet) {
  const contract = await getControllerContract();
  const result = await contract.getBuyerAmbassador(buyerWallet).call();
  return zeroAddressToNull(result);
}

async function getAmbassadorBySlugHash(slugHash) {
  const contract = await getControllerContract();
  const result = await contract.getAmbassadorBySlugHash(slugHash).call();
  return zeroAddressToNull(result);
}

async function isPurchaseProcessed(purchaseId) {
  const contract = await getControllerContract();
  const normalizedPurchaseId = assertBytes32Hex(purchaseId, 'purchaseId');
  const result = await contract.isPurchaseProcessed(normalizedPurchaseId).call();
  return Boolean(result);
}

async function recordVerifiedPurchase({
  purchaseId,
  buyerWallet,
  ambassadorCandidate,
  purchaseAmountSun,
  ownerShareSun
}) {
  const contract = await getControllerContract();
  const normalizedPurchaseId = assertBytes32Hex(purchaseId, 'purchaseId');

  const txid = await contract.recordVerifiedPurchase(
    normalizedPurchaseId,
    buyerWallet,
    ambassadorCandidate,
    String(purchaseAmountSun),
    String(ownerShareSun)
  ).send({
    feeLimit: env.CONTROLLER_FEE_LIMIT_SUN,
    shouldPollResponse: false
  });

  if (!txid) {
    throw new Error('Controller transaction sent but txid was not returned');
  }

  return String(txid).toLowerCase();
}

async function readAmbassadorDashboard(ambassadorWallet) {
  const contract = await getControllerContract();

  const core = await contract.getDashboardCore(ambassadorWallet).call();
  const stats = await contract.getDashboardStats(ambassadorWallet).call();
  const profile = await contract.getDashboardProfile(ambassadorWallet).call();
  const payout = await contract.getAmbassadorPayoutData(ambassadorWallet).call();

  return { core, stats, profile, payout };
}

async function getControllerEvents(eventName, {
  minBlockTimestamp,
  maxBlockTimestamp,
  fingerprint,
  limit = 20
} = {}) {
  const options = {
    eventName,
    onlyConfirmed: true,
    orderBy: 'block_timestamp,asc',
    limit
  };

  if (typeof minBlockTimestamp === 'number') {
    options.minBlockTimestamp = minBlockTimestamp;
  }

  if (typeof maxBlockTimestamp === 'number') {
    options.maxBlockTimestamp = maxBlockTimestamp;
  }

  if (fingerprint) {
    options.fingerprint = fingerprint;
  }

  return tronWeb.getEventResult(env.FOURTEEN_CONTROLLER_CONTRACT, options);
}

async function waitForControllerEventByTxHash(
  txHash,
  eventName,
  { attempts = 1, delayMs = 0 } = {}
) {
  const normalizedTxHash = String(txHash || '').trim().toLowerCase();

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const response = await tronWeb.getEventByTransactionID(normalizedTxHash);
    const list = normalizeEventList(response);

    const match = list.find((item) => {
      const itemEventName = String(item?.event_name || '');
      const contractAddress = toBase58Address(item?.contract_address);

      return (
        itemEventName === eventName &&
        contractAddress === env.FOURTEEN_CONTROLLER_CONTRACT
      );
    });

    if (match) {
      return match;
    }

    if (attempt < attempts && delayMs > 0) {
      await sleep(delayMs);
    }
  }

  throw new Error(`${eventName} event not found for transaction`);
}

async function getAllocationEventByTxHash(
  txHash,
  { attempts = 4, delayMs = 750 } = {}
) {
  const event = await waitForControllerEventByTxHash(
    txHash,
    'PurchaseFundsAllocated',
    { attempts, delayMs }
  );

  const result = event?.result || {};
  const blockTimestamp = Number(event?.block_timestamp || 0);

  return {
    txHash: String(event?.transaction_id || txHash || '').trim().toLowerCase(),
    purchaseId: normalizeBytes32(result.purchaseId || result['0']),
    buyerWallet: toBase58Address(result.buyer || result['1']),
    ambassadorWallet: toBase58Address(result.ambassador || result['2']),
    purchaseAmountSun: String(result.purchaseAmountSun || result['3'] || 0),
    ownerShareSun: String(result.ownerShareSun || result['4'] || 0),
    rewardSun: String(result.rewardSun || result['5'] || 0),
    ownerPartSun: String(result.ownerPartSun || result['6'] || 0),
    level: Number(result.level || result['7'] || 0),
    blockTime: blockTimestamp
      ? new Date(blockTimestamp).toISOString()
      : new Date().toISOString()
  };
}

async function getWithdrawalEventByTxHash(
  txHash,
  { attempts = 2, delayMs = 500 } = {}
) {
  const event = await waitForControllerEventByTxHash(
    txHash,
    'RewardsWithdrawn',
    { attempts, delayMs }
  );

  const result = event?.result || {};
  const blockTimestamp = Number(event?.block_timestamp || 0);

  return {
    txHash: String(event?.transaction_id || txHash || '').trim().toLowerCase(),
    ambassadorWallet: toBase58Address(result.ambassador || result['0']),
    amountSun: String(result.amountSun || result['1'] || 0),
    blockTime: blockTimestamp
      ? new Date(blockTimestamp).toISOString()
      : new Date().toISOString()
  };
}

module.exports = {
  getControllerContract,
  getBuyerAmbassador,
  getAmbassadorBySlugHash,
  isPurchaseProcessed,
  recordVerifiedPurchase,
  readAmbassadorDashboard,
  getControllerEvents,
  getAllocationEventByTxHash,
  getWithdrawalEventByTxHash
};
