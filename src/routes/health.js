const express = require('express');

const router = express.Router();

router.get('/health', (req, res) => {
  res.json({
    ok: true,
    service: 'fourteen-ambassador-system',
    timestamp: Date.now()
  });
});

module.exports = router;
