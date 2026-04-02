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
    inputs: [{ internalType: 'bytes32', name: 'slugHash', type: 'bytes32' }],
    name: 'getAmbassadorBySlugHash',
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

  return contract.recordVerifiedPurchase(
    purchaseId,
    buyerWallet,
    ambassadorCandidate,
    purchaseAmountSun,
    ownerShareSun
  ).send({
    feeLimit: 300000000
  });
}

async function readAmbassadorDashboard(ambassadorWallet) {
  const contract = await getControllerContract();

  const core = await contract.getDashboardCore(ambassadorWallet).call();
  const stats = await contract.getDashboardStats(ambassadorWallet).call();
  const profile = await contract.getDashboardProfile(ambassadorWallet).call();
  const payout = await contract.getAmbassadorPayoutData(ambassadorWallet).call();

  return { core, stats, profile, payout };
}

module.exports = {
  getControllerContract,
  getBuyerAmbassador,
  getAmbassadorBySlugHash,
  isPurchaseProcessed,
  recordVerifiedPurchase,
  readAmbassadorDashboard
};
