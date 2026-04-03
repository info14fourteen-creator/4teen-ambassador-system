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

function toBase58Address(value) {
  if (!value) return null;

  try {
    if (typeof value === 'string' && value.startsWith('T')) {
      return value;
    }

    let hex = String(value).trim();

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
  const result = await contract.isPurchaseProcessed(purchaseId).call();
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

  const txid = await contract.recordVerifiedPurchase(
    purchaseId,
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

  return String(txid);
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

async function getWithdrawalEventByTxHash(txHash) {
  const normalizedTxHash = String(txHash || '').trim().toLowerCase();

  if (!normalizedTxHash) {
    throw new Error('txid is required');
  }

  const response = await tronWeb.getEventByTransactionID(normalizedTxHash);
  const events = normalizeEventList(response);

  const match = events.find((item) => {
    const eventName = String(item?.event_name || '').trim();
    const contractAddress = toBase58Address(item?.contract_address);

    return (
      eventName === 'RewardsWithdrawn' &&
      contractAddress === env.FOURTEEN_CONTROLLER_CONTRACT
    );
  });

  if (!match) {
    throw new Error('RewardsWithdrawn event not found for transaction');
  }

  const ambassadorWallet = toBase58Address(
    match?.result?.ambassador ?? match?.result?.['0']
  );
  const amountSun = String(
    match?.result?.amountSun ?? match?.result?.['1'] ?? '0'
  ).trim();
  const blockTimestamp = Number(match?.block_timestamp || 0);

  if (!ambassadorWallet) {
    throw new Error('Ambassador address was not found in RewardsWithdrawn event');
  }

  return {
    txHash: normalizedTxHash,
    ambassadorWallet,
    amountSun: /^\d+$/.test(amountSun) ? amountSun : '0',
    blockTime: blockTimestamp ? new Date(blockTimestamp).toISOString() : null,
    blockTimestamp
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
  getWithdrawalEventByTxHash
};
