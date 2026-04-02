const express = require('express');
const healthRouter = require('./routes/health');
const hooksRouter = require('./routes/hooks');
const adminRouter = require('./routes/admin');

const app = express();

app.use(express.json());

app.use('/', healthRouter);
app.use('/hooks', hooksRouter);
app.use('/admin', adminRouter);

module.exports = app;
