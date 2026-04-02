require('dotenv').config();

const app = require('./src/app');
const env = require('./src/config/env');

app.listen(env.PORT, () => {
  console.log(`Server listening on port ${env.PORT}`);
});
