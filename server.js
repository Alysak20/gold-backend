require('dotenv').config();
const express = require('express');
const cors = require('cors');
const MetaApi = require('metaapi.cloud-sdk').default;

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const token = process.env.METAAPI_TOKEN;
const accountId = process.env.METAAPI_ACCOUNT_ID;

if (!token) console.log('ERROR: METAAPI_TOKEN missing');
if (!accountId) console.log('ERROR: METAAPI_ACCOUNT_ID missing');

const api = new MetaApi(token);

let latestPrices = {};
let connectionStatus = 'starting';
let lastUpdate = null;

const SYMBOLS = ['XAUUSD', 'XAGUSD', 'BTCUSD', 'EURUSD'];

async function start() {
  try {
    console.log('Connecting to MetaApi account: ' + accountId);
    const account = await api.metatraderAccountApi.getAccount(accountId);
    const connection = account.getStreamingConnection();

    connection.addSynchronizationListener({
      onConnected: function() {
        connectionStatus = 'connected';
        console.log('MetaApi STREAM CONNECTED');
      },
      onDisconnected: function() {
        connectionStatus = 'disconnected';
        console.log('MetaApi STREAM DISCONNECTED');
      },
      onSymbolPriceUpdated: function(instanceIndex, price) {
        if (!price || !price.symbol) return;
        latestPrices[price.symbol] = { bid: price.bid, ask: price.ask, time: new Date() };
        lastUpdate = new Date();
        console.log(price.symbol + ' Bid: ' + price.bid + ' Ask: ' + price.ask);
      },
      onSymbolPricesUpdated: function(instanceIndex, prices) {
        for (const price of prices) {
          if (!price || !price.symbol) continue;
          latestPrices[price.symbol] = { bid: price.bid, ask: price.ask, time: new Date() };
          lastUpdate = new Date();
          console.log(price.symbol + ' Bid: ' + price.bid + ' Ask: ' + price.ask);
        }
      }
    });

    await connection.connect();
    console.log('Connected to MetaApi - Real Broker Price Started');
    connectionStatus = 'connected';

    try {
      await Promise.race([
        connection.waitSynchronized(),
        new Promise(function(_, reject) { setTimeout(function() { reject(new Error('sync timeout')); }, 15000); })
      ]);
      console.log('Synchronized OK');
    } catch (e) {
      console.log('Sync timeout, continuing to subscribe anyway: ' + e.message);
    }

    for (const symbol of SYMBOLS) {
      try {
        await connection.subscribeToMarketData(symbol);
        console.log('Subscribed to ' + symbol);
      } catch (e) {
        console.log('Could not subscribe to ' + symbol + ': ' + e.message);
      }
    }

    setInterval(async function() {
      try {
        for (const symbol of SYMBOLS) {
          try {
            const price = await connection.getSymbolPrice(symbol);
            if (price && price.bid) {
              latestPrices[symbol] = { bid: price.bid, ask: price.ask, time: new Date() };
              lastUpdate = new Date();
            }
          } catch (err) {}
        }
        if (latestPrices['XAUUSD']) {
          console.log('Poll update XAUUSD: ' + latestPrices['XAUUSD'].bid + ' / ' + latestPrices['XAUUSD'].ask);
        }
      } catch (e) {
        console.log('Poll error: ' + e.message);
      }
    }, 2000);

  } catch (err) {
    connectionStatus = 'error: ' + err.message;
    console.log('Error connecting to MetaApi: ' + err.message);
    console.log('Retrying in 10 seconds...');
    setTimeout(start, 10000);
  }
}

start();

app.get('/prices', function(req, res) {
  res.json(latestPrices);
});

app.get('/health', function(req, res) {
  res.json({
    status: 'ok',
    connection: connectionStatus,
    lastUpdate: lastUpdate,
    symbols: Object.keys(latestPrices),
    prices: latestPrices
  });
});

app.get('/', function(req, res) {
  res.send('Gold Master MetaApi Backend is Running!');
});

app.listen(PORT, function() {
  console.log('Backend running on port ' + PORT);
});
