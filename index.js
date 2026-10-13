"use strict";

const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const {
  INTERVALS,
  intervalMs,
  calculateProgressions
} = require("./flowEngine");

const {
  fetchHistoricalCandles,
  INTERVALS: HISTORY_INTERVALS
} = require("./historyEngine");

const app = express();
const PORT = process.env.PORT || 3000;

const FUTURES_INFO_URL =
  "https://fapi.binance.com/fapi/v1/exchangeInfo";

const FUTURES_WS_URL =
  "wss://fstream.binance.com/ws";

const DURATIONS = Object.freeze({
  "30m": 30 * 60 * 1000,
  "1h": 60 * 60 * 1000,
  "4h": 4 * 60 * 60 * 1000,
  "12h": 12 * 60 * 60 * 1000,
  "24h": 24 * 60 * 60 * 1000,
  "3d": 3 * 24 * 60 * 60 * 1000,
  "7d": 7 * 24 * 60 * 60 * 1000,
  "14d": 14 * 24 * 60 * 60 * 1000,
  "30d": 30 * 24 * 60 * 60 * 1000
});

const streams = new Map();

const MAX_STREAMS = 12;
const STREAM_IDLE_MS = 10 * 60 * 1000;
const MAX_HISTORY_CANDLES = 45000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "frontend")));

/* -------------------------------------------------------
   VALIDATION
------------------------------------------------------- */

function normalizeSymbol(value) {
  const symbol = String(value || "BTCUSDT").toUpperCase();

  if (!/^[A-Z0-9]{4,30}$/.test(symbol)) {
    throw new Error("Invalid trading symbol.");
  }

  return symbol;
}

function normalizeInterval(value) {
  const interval = String(value || "1m");

  if (!HISTORY_INTERVALS.includes(interval) ||
      !INTERVALS[interval]) {
    throw new Error("Unsupported candle interval.");
  }

  return interval;
}

function getRequestedWindow(query) {
  const now = Date.now();

  const endTime = query.endTime === undefined
    ? now
    : Number(query.endTime);

  if (!Number.isFinite(endTime) || endTime <= 0) {
    throw new Error("Invalid endTime.");
  }

  let startTime;

  if (query.startTime !== undefined) {
    startTime = Number(query.startTime);

    if (!Number.isFinite(startTime) ||
        startTime <= 0 ||
        startTime >= endTime) {
      throw new Error("Invalid startTime.");
    }

    if (endTime - startTime > DURATIONS["30d"]) {
      throw new Error("The maximum historical window is 30 days.");
    }
  } else {
    const durationName = String(query.duration || "24h");
    const duration = DURATIONS[durationName];

    if (!duration) {
      throw new Error(
        "Unsupported duration. Use 30m, 1h, 4h, 12h, 24h, 3d, 7d, 14d or 30d."
      );
    }

    startTime = endTime - duration;
  }

  return {
    startTime,
    endTime
  };
}

/* -------------------------------------------------------
   BINANCE FUTURES SYMBOLS
------------------------------------------------------- */

let symbolCache = {
  updatedAt: 0,
  symbols: []
};

async function getFuturesSymbols() {
  const cacheAge = Date.now() - symbolCache.updatedAt;

  if (symbolCache.symbols.length && cacheAge < 60 * 60 * 1000) {
    return symbolCache.symbols;
  }

  const response = await fetch(FUTURES_INFO_URL, {
    headers: {
      "User-Agent": "FootprintFlow/1.0"
    }
  });

  if (!response.ok) {
    throw new Error(
      `Binance exchange information failed: ${response.status}`
    );
  }

  const data = await response.json();

  if (!Array.isArray(data.symbols)) {
    throw new Error("Unexpected Binance exchange information.");
  }

  const symbols = data.symbols
    .filter(item =>
      item.status === "TRADING" &&
      item.quoteAsset === "USDT" &&
      item.contractType === "PERPETUAL"
    )
    .map(item => item.symbol)
    .sort();

  symbolCache = {
    updatedAt: Date.now(),
    symbols
  };

  return symbols;
}

/* -------------------------------------------------------
   CANDLE NORMALIZATION
------------------------------------------------------- */

function normalizeHistoricalCandle(candle) {
  const time = Number(candle.time);
  const open = Number(candle.open);
  const high = Number(candle.high);
  const low = Number(candle.low);
  const close = Number(candle.close);
  const buy = Number(candle.buy);
  const sell = Number(candle.sell);

  if (
    ![time, open, high, low, close, buy, sell].every(Number.isFinite) ||
    time <= 0 ||
    buy < 0 ||
    sell < 0
  ) {
    return null;
  }

  return {
    time,
    open,
    high,
    low,
    close,
    volume: buy + sell,
    buy,
    sell,
    closed: false
  };
}

function normalizeLiveKline(kline) {
  const time = Number(kline.t);
  const open = Number(kline.o);
  const high = Number(kline.h);
  const low = Number(kline.l);
  const close = Number(kline.c);
  const volume = Number(kline.v);
  const buy = Number(kline.V);

  if (
    ![time, open, high, low, close, volume, buy].every(Number.isFinite) ||
    time <= 0 ||
    volume < 0 ||
    buy < 0
  ) {
    return null;
  }

  if (buy > volume + Math.max(1, volume) * 1e-10) {
    return null;
  }

  const boundedBuy = Math.min(buy, volume);

  return {
    time,
    open,
    high,
    low,
    close,
    volume,
    buy: boundedBuy,
    sell: Math.max(0, volume - boundedBuy),
    closed: Boolean(kline.x)
  };
}

/* -------------------------------------------------------
   STREAM MANAGEMENT
------------------------------------------------------- */

function closeStream(entry) {
  if (!entry) return;

  entry.closedByServer = true;

  if (entry.reconnectTimer) {
    clearTimeout(entry.reconnectTimer);
    entry.reconnectTimer = null;
  }

  if (entry.ws) {
    const socket = entry.ws;
    entry.ws = null;

    try {
      socket.removeAllListeners();
      socket.terminate();
    } catch (_) {
      // The socket may already be closed.
    }
  }
}

function scheduleReconnect(entry) {
  if (entry.closedByServer || entry.reconnectTimer) {
    return;
  }

  entry.reconnectTimer = setTimeout(() => {
    entry.reconnectTimer = null;

    if (!entry.closedByServer) {
      connectStream(entry);
    }
  }, 3000);

  if (entry.reconnectTimer.unref) {
    entry.reconnectTimer.unref();
  }
}

function connectStream(entry) {
  if (entry.closedByServer || entry.ws) return;

  const streamName =
    `${entry.symbol.toLowerCase()}@kline_${entry.interval}`;

  const socket = new WebSocket(
    `${FUTURES_WS_URL}/${streamName}`
  );

  entry.ws = socket;

  socket.on("open", () => {
    entry.lastAccess = Date.now();
    entry.lastStreamMessage = Date.now();
    entry.streamConnected = true;
    console.log(
      `Live stream connected: ${entry.symbol} ${entry.interval}`
    );
  });

  socket.on("message", rawMessage => {
    try {
      const message = JSON.parse(rawMessage.toString());

      if (!message.k) return;

      const candle = normalizeLiveKline(message.k);

      if (!candle) return;

      entry.candles.set(candle.time, candle);
      entry.lastAccess = Date.now();
      entry.lastStreamMessage = Date.now();

      // Keep the in-memory candle history bounded.
      if (entry.candles.size > MAX_HISTORY_CANDLES) {
        const oldestTimes = Array.from(entry.candles.keys())
          .sort((a, b) => a - b);

        const removeCount =
          entry.candles.size - MAX_HISTORY_CANDLES;

        for (let i = 0; i < removeCount; i++) {
          entry.candles.delete(oldestTimes[i]);
        }
      }
    } catch (error) {
      console.error(
        "Live candle message error:",
        error.message
      );
    }
  });

  socket.on("error", error => {
    console.error(
      `WebSocket error (${entry.symbol}):`,
      error.message
    );
  });

  socket.on("close", () => {
    if (entry.ws === socket) {
      entry.ws = null;
    }

    entry.streamConnected = false;

    if (!entry.closedByServer) {
      scheduleReconnect(entry);
    }
  });
}

/* -------------------------------------------------------
   HISTORY CACHE
------------------------------------------------------- */

async function loadHistory(entry, startTime, endTime) {
  const needsInitialLoad = !entry.historyLoaded;
  const needsEarlierHistory =
    entry.historyLoaded &&
    startTime < entry.historyStart;

  if (!needsInitialLoad && !needsEarlierHistory) {
    return;
  }

  const fetchStart = needsEarlierHistory
    ? startTime
    : startTime;

  const candles = await fetchHistoricalCandles({
    symbol: entry.symbol,
    interval: entry.interval,
    startTime: fetchStart,
    endTime
  });

  for (const rawCandle of candles) {
    const candle = normalizeHistoricalCandle(rawCandle);

    if (!candle) continue;

    // REST history supplies the initial state.
    // Live WebSocket updates replace matching candle times.
    if (!entry.candles.has(candle.time)) {
      entry.candles.set(candle.time, candle);
    }
  }

  entry.historyStart = needsInitialLoad
    ? startTime
    : Math.min(entry.historyStart, startTime);

  entry.historyLoaded = true;
}

function createStreamEntry(symbol, interval) {
  return {
    symbol,
    interval,
    candles: new Map(),
    historyLoaded: false,
    historyStart: Infinity,
    loading: null,
    ws: null,
    reconnectTimer: null,
    lastAccess: Date.now(),
    lastStreamMessage: 0,
    streamConnected: false,
    closedByServer: false
  };
}

async function getStreamEntry(symbol, interval, startTime, endTime) {
  const key = `${symbol}:${interval}`;

  let entry = streams.get(key);

  if (!entry) {
    entry = createStreamEntry(symbol, interval);
    streams.set(key, entry);
  }

  entry.lastAccess = Date.now();

  if (entry.loading) {
    await entry.loading;
  }

  if (!entry.historyLoaded || startTime < entry.historyStart) {
    entry.loading = loadHistory(entry, startTime, endTime);

    try {
      await entry.loading;
    } finally {
      entry.loading = null;
    }
  }

  if (!entry.ws && !entry.closedByServer) {
    connectStream(entry);
  }

  return entry;
}

/* -------------------------------------------------------
   CACHE CLEANUP
------------------------------------------------------- */

function cleanupStreams() {
  const now = Date.now();

  for (const [key, entry] of streams.entries()) {
    if (now - entry.lastAccess > STREAM_IDLE_MS) {
      closeStream(entry);
      streams.delete(key);
    }
  }

  if (streams.size <= MAX_STREAMS) return;

  const oldestEntries = Array.from(streams.entries())
    .sort((a, b) => a[1].lastAccess - b[1].lastAccess);

  while (streams.size > MAX_STREAMS && oldestEntries.length) {
    const [key, entry] = oldestEntries.shift();

    closeStream(entry);
    streams.delete(key);
  }
}

const cleanupTimer = setInterval(cleanupStreams, 60 * 1000);

if (cleanupTimer.unref) {
  cleanupTimer.unref();
}

/* -------------------------------------------------------
   API ROUTES
------------------------------------------------------- */

app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    exchange: "Binance USDⓈ-M Futures",
    streams: streams.size,
    time: Date.now()
  });
});

app.get("/api/symbols", async (req, res) => {
  try {
    const symbols = await getFuturesSymbols();

    res.json({
      exchange: "Binance USDⓈ-M Futures",
      symbols
    });
  } catch (error) {
    console.error("Symbol API error:", error.message);

    res.status(502).json({
      error: "Unable to retrieve Binance Futures symbols.",
      details: error.message
    });
  }
});

app.get("/api/data/:symbol", async (req, res) => {
  try {
    const symbol = normalizeSymbol(req.params.symbol);
    const interval = normalizeInterval(req.query.interval);
    const { startTime, endTime } = getRequestedWindow(req.query);

    const entry = await getStreamEntry(
      symbol,
      interval,
      startTime,
      endTime
    );

    entry.lastAccess = Date.now();

    const candles = Array.from(entry.candles.values())
      .filter(candle =>
        candle.time >= startTime &&
        candle.time <= endTime
      )
      .sort((a, b) => a.time - b.time);

    // Calculate both progression lines from the same
    // ordered candle sequence. Each requested window
    // starts from a fresh zero baseline.
    const progressionCandles = calculateProgressions(candles);

    res.set("Cache-Control", "no-store");

    res.json({
      symbol,
      interval,
      startTime,
      endTime,
      exchange: "Binance USDⓈ-M Futures",
      streamConnected: entry.streamConnected,
      lastStreamMessage: entry.lastStreamMessage || null,
      candles: progressionCandles.map(candle => ({
        time: candle.time,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: candle.volume,
        buyTakerVolume: candle.buyTakerVolume,
        sellTakerVolume: candle.sellTakerVolume,
        buyTakerProgression: candle.buyTakerProgression,
        sellTakerProgression: candle.sellTakerProgression,
        closed: Boolean(candle.closed)
      }))
    });
  } catch (error) {
    console.error("Data API error:", error.message);

    res.status(400).json({
      error: error.message || "Unable to retrieve flow data."
    });
  }
});

/* -------------------------------------------------------
   FRONTEND ROUTES
------------------------------------------------------- */

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "frontend", "home.html"));
});

app.get("/chart", (req, res) => {
  res.sendFile(path.join(__dirname, "frontend", "chart.html"));
});

/* -------------------------------------------------------
   START SERVER
------------------------------------------------------- */

const server = app.listen(PORT, () => {
  console.log(`Footprint Flow running on port ${PORT}`);
  console.log("Exchange: Binance USDⓈ-M Futures");
  console.log("Historical and live candles use the same exchange.");
});

function shutdown() {
  console.log("Shutting down Footprint Flow...");

  clearInterval(cleanupTimer);

  for (const entry of streams.values()) {
    closeStream(entry);
  }

  streams.clear();

  server.close(() => {
    process.exit(0);
  });

  setTimeout(() => process.exit(1), 5000).unref();
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
