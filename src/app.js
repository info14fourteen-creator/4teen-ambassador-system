const express = require('express');
const healthRouter = require('./routes/health');
const adminRouter = require('./routes/admin');
const cabinetRouter = require('./routes/cabinet');

const app = express();

app.use(express.json());

app.use('/', healthRouter);
app.use('/admin', adminRouter);
app.use('/cabinet', cabinetRouter);

module.exports = app;
