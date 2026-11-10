require('dotenv').config();
const express = require('express');
const cors = require('cors');
const MetaApi = require('metaapi.cloud-sdk').default;

const app = express();
app.use(cors());
const PORT = process.env.PORT || 3000;

const token = process.env.METAAPI_TOKEN;
const accountId = process.env.METAAPI_ACCOUNT_ID;

// Initialize MetaApi here globally
const api = new MetaApi(token);

let latestPrices = {};

async function start() {
  try {
    const account = await api.metatraderAccountApi.getAccount(accountId);
    const connection = account.getStreamingConnection();
    await connection.connect();
    await connection.waitSynchronized();

    console.log('Connected to MetaApi - Real Broker Price Started');

    connection.addSynchronizationListener({
      onSymbolPricesUpdated: (instanceIndex, prices) => {
        for (const price of prices) {
          latestPrices[price.symbol] = { bid: price.bid, ask: price.ask, time: new Date() };
          console.log(price.symbol, 'Bid:', price.bid, 'Ask:', price.ask);
        }
      },
      onSymbolPriceUpdated: (instanceIndex, price) => {
        latestPrices[price.symbol] = { bid: price.bid, ask: price.ask, time: new Date() };
        console.log(price.symbol, 'Bid:', price.bid, 'Ask:', price.ask);
      }
    });

    const symbols = ['XAUUSD', 'XAGUSD', 'BTCUSD', 'EURUSD'];
    for (const symbol of symbols) {
      try {
        await connection.subscribeToMarketData(symbol);
        console.log(`Subscribed to ${symbol}`);
      } catch (e) {
        console.log(`Could not subscribe to ${symbol}:`, e.message);
      }
    }

  } catch (err) {
    console.log('Error connecting to MetaApi: ' + err.message);
  }
}

start();

app.get('/prices', (req, res) => {
  res.json(latestPrices);
});

app.get('/', (req, res) => {
  res.send('Gold Master MetaApi Backend is Running!');
});

app.listen(PORT, () => {
  console.log('Backend running on port ' + PORT);
});
