const env = require('../../config/env');

function requireAdminAuth(req, res, next) {
  const token = req.headers['x-admin-sync-token'];

  if (token !== env.ADMIN_SYNC_TOKEN) {
    return res.status(401).json({
      ok: false,
      error: 'Unauthorized'
    });
  }

  next();
}

module.exports = {
  requireAdminAuth
};
