const express = require('express');
const { handleProxyRequest } = require('../services/public/apiProxy');

const router = express.Router();

function getWildcardPath(req) {
  return `/${String(req.params[0] || '').replace(/^\/+/, '')}`;
}

router.all('/trongrid/*', (req, res) => {
  return handleProxyRequest(req, res, 'trongrid', getWildcardPath(req));
});

router.all('/tronscan/*', (req, res) => {
  return handleProxyRequest(req, res, 'tronscan', getWildcardPath(req));
});

router.all('/cmc/pro/*', (req, res) => {
  return handleProxyRequest(req, res, 'cmc-pro', getWildcardPath(req));
});

router.all('/cmc/data/*', (req, res) => {
  return handleProxyRequest(req, res, 'cmc-data', getWildcardPath(req));
});

router.all('/cmc/dapi/*', (req, res) => {
  return handleProxyRequest(req, res, 'cmc-dapi', getWildcardPath(req));
});

module.exports = router;
