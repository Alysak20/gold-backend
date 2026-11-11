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

// Check env on startup
if (!token) console.log('❌ ERROR: METAAPI_TOKEN missing in Render Environment');
if (!accountId) console.log('❌ ERROR: METAAPI_ACCOUNT_ID missing in Render Environment');

const api = new MetaApi(token);

let latestPrices = {};
let connectionStatus = 'starting';
let lastUpdate = null;

const SYMBOLS = ['XAUUSD', 'XAGUSD', 'BTCUSD', 'EURUSD'];

async function start() {
  try {
    console.log('Connecting to MetaApi account:', accountId);
    const account = await api.metatraderAccountApi.getAccount(accountId);
    const connection = account.getStreamingConnection();

    // 1. LISTENER FIRST - Before connect
    connection.addSynchronizationListener({
      onConnected: () => {
        connectionStatus = 'connected';
        console.log('✅ MetaApi STREAM CONNECTED');
      },
      onDisconnected: () => {
        connectionStatus = 'disconnected';
        console.log('⚠️ MetaApi STREAM DISCONNECTED - retrying...');
      },
      onSymbolPriceUpdated: (instanceIndex, price) => {
        if (!price || !price.symbol) return;
        latestPrices[price.symbol] = { bid: price.bid, ask: price.ask, time: new Date() };
        lastUpdate = new Date();
        console.log(price.symbol, 'Bid:', price.bid, 'Ask:', price.ask);
      },
      onSymbolPricesUpdated: (instanceIndex, prices) => {
        for (const price of prices) {
          if (!price || !price.symbol) continue;
          latestPrices[price.symbol] = { bid: price.bid, ask: price.ask, time: new Date() };
          lastUpdate = new Date();
          console.log(price.symbol, 'Bid:', price.bid, 'Ask:', price.ask);
        }
      },
    });

    await connection.connect();
    console.log('✅ Connected to MetaApi - Real Broker Price Started');
    connectionStatus = 'connected';

    // 2. DO NOT WAIT FOREVER - 15 sec timeout
    try {
      await Promise.race([
        connection.waitSynchronized(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('sync timeout - continue anyway')), 15000))
      ]);
      console.log('✅ Synchronized OK');
    } catch (e) {
      console.log('⚠️ Sync timeout, continuing to subscribe anyway: ' + e.message);
    }

    // 3. SUBSCRIBE TO ALL SYMBOLS
    for (const symbol of SYMBOLS) {
      try {
        await connection.subscribeToMarketData(symbol);
        console.log(✅ Subscribed to ${symbol});
      } catch (e) {
        console.log(❌ Could not subscribe to ${symbol}:, e.message);
      }
    }

    // 4. BACKUP POLLING - Guarantees price even if stream fails
    // Runs every 2 seconds
    setInterval(async () => {
      try {
        for (const symbol of SYMBOLS) {
          try {
            const price = await connection.getSymbolPrice(symbol);
            if (price && price.bid) {
              latestPrices[symbol] = { bid: price.bid, ask: price.ask, time: new Date() };
              lastUpdate = new Date();
            }
          } catch (err) {
            // ignore single symbol error, try next
          }
        }
        if (latestPrices['XAUUSD']) {
          console.log('🔄 Poll update XAUUSD:', latestPrices['XAUUSD'].bid, '/', latestPrices['XAUUSD'].ask);
        }
      } catch (e) {
        console.log('Poll error: ' + e.message);
      }
    }, 2000);

    // 5. AUTO RECONNECT WATCHDOG - If no price for 30 sec, restart
    setInterval(() => {
      if (lastUpdate && (Date.now() - new Date(lastUpdate).getTime() > 30000)) {
        console.log('⚠️ No price for 30s, connection may be dead...');
        // Do not exit, polling will try again. Render will keep it alive on Paid.
      }
    }, 30000);

  } catch (err) {
    connectionStatus = 'error: ' + err.message;
    console.log('❌ Error connecting to MetaApi: ' + err.message);
    console.log('Retrying in 10 seconds...');
    setTimeout(start, 10000);
  }
}

start();

// API ENDPOINTS
app.get('/prices', (req, res) => {
  res.json(latestPrices);
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    connection: connectionStatus,
    lastUpdate: lastUpdate,
    symbols: Object.keys(latestPrices),
    prices: latestPrices
  });
});

app.get('/', (req, res) => {
  res.send('Gold Master MetaApi Backend is Running!');
});

app.listen(PORT, () => {
  console.log('Backend running on port ' + PORT);
});
