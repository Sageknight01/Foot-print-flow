"use strict";

/*
 * FOOTPRINT FLOW
 * Historical data engine.
 *
 * Source: Binance USDⓈ-M Futures.
 *
 * Kline fields:
 * [0] Open time
 * [1] Open
 * [2] High
 * [3] Low
 * [4] Close
 * [5] Total base-asset volume
 * [9] Taker-buy base-asset volume
 *
 * Buy Taker = field [9]
 * Sell Taker = field [5] - field [9]
 */

const fetch = require("node-fetch");

const BASE_URL =
    "https://fapi.binance.com/fapi/v1/klines";

const MAX_LIMIT = 1500;

const INTERVALS = Object.freeze([
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "4h",
    "1d"
]);


/*
 * Pause between requests to avoid unnecessary API pressure.
 */

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


/*
 * Validate the requested candle interval.
 */

function validateInterval(interval) {
    if (!INTERVALS.includes(interval)) {
        throw new Error(
            `Unsupported interval: ${interval}`
        );
    }
}


/*
 * Fetch historical futures candles.
 *
 * Options:
 * {
 *   symbol: "BTCUSDT",
 *   interval: "1m",
 *   startTime: Unix milliseconds,
 *   endTime: Unix milliseconds
 * }
 */

async function fetchHistoricalCandles(options = {}) {
    const symbol = String(
        options.symbol || "BTCUSDT"
    ).toUpperCase();

    const interval = options.interval || "1m";

    validateInterval(interval);

    const endTime = Number(
        options.endTime || Date.now()
    );

    const startTime = Number(
        options.startTime ||
        endTime - 24 * 60 * 60 * 1000
    );

    if (
        !Number.isFinite(startTime) ||
        !Number.isFinite(endTime) ||
        startTime >= endTime
    ) {
        throw new Error(
            "Invalid historical start/end time."
        );
    }

    const candles = [];

    let cursor = startTime;

    while (cursor < endTime) {
        const params = new URLSearchParams({
            symbol,
            interval,
            startTime: String(cursor),
            endTime: String(endTime),
            limit: String(MAX_LIMIT)
        });

        const url =
            `${BASE_URL}?${params.toString()}`;

        const response = await fetch(url, {
            timeout: 20000
        });

        if (!response.ok) {
            const body = await response.text();

            throw new Error(
                `Binance API error ${response.status}: ${body}`
            );
        }

        const rows = await response.json();

        if (!Array.isArray(rows)) {
            throw new Error(
                "Unexpected Binance kline response."
            );
        }

        if (rows.length === 0) {
            break;
        }

        for (const row of rows) {
            const time = Number(row[0]);

            const totalVolume = Number(row[5]);

            const buyTaker = Number(row[9]);

            if (
                !Number.isFinite(time) ||
                !Number.isFinite(totalVolume) ||
                !Number.isFinite(buyTaker) ||
                totalVolume < 0 ||
                buyTaker < 0
            ) {
                continue;
            }

            /*
             * Protect against floating-point noise.
             * Taker-buy volume cannot exceed total volume.
             */

            const tolerance =
                Math.max(1, totalVolume) * 1e-10;

            if (buyTaker > totalVolume + tolerance) {
                throw new Error(
                    `Invalid taker-buy volume at ${time}`
                );
            }

            const boundedBuy = Math.min(
                buyTaker,
                totalVolume
            );

            const sellTaker =
                Math.max(
                    0,
                    totalVolume - boundedBuy
                );

            candles.push({
                time,

                open: Number(row[1]),
                high: Number(row[2]),
                low: Number(row[3]),
                close: Number(row[4]),

                volume: totalVolume,

                buy: boundedBuy,
                sell: sellTaker
            });
        }

        const lastOpenTime =
            Number(rows[rows.length - 1][0]);

        const nextCursor =
            lastOpenTime + 1;

        if (
            !Number.isFinite(nextCursor) ||
            nextCursor <= cursor
        ) {
            throw new Error(
                "Historical pagination failed to advance."
            );
        }

        cursor = nextCursor;

        /*
         * If fewer than the maximum number of rows arrived,
         * there is usually no more data in this request window.
         */

        if (rows.length < MAX_LIMIT) {
            break;
        }

        await sleep(100);
    }

    /*
     * Deduplicate and sort chronologically.
     */

    const unique = new Map();

    for (const candle of candles) {
        unique.set(candle.time, candle);
    }

    return Array.from(unique.values())
        .sort((a, b) => a.time - b.time);
}


/*
 * Command-line usage:
 *
 * node historyEngine.js BTCUSDT 1m 24
 *
 * This example fetches approximately the last 24 hours
 * of one-minute futures candles.
 *
 * The main server will import fetchHistoricalCandles()
 * directly in the next implementation stage.
 */

if (require.main === module) {
    (async () => {
        try {
            const symbol =
                process.argv[2] || "BTCUSDT";

            const interval =
                process.argv[3] || "1m";

            const hours = Number(
                process.argv[4] || 24
            );

            if (
                !Number.isFinite(hours) ||
                hours <= 0
            ) {
                throw new Error(
                    "Hours must be a positive number."
                );
            }

            const endTime = Date.now();

            const startTime =
                endTime - hours * 60 * 60 * 1000;

            console.log(
                `Fetching ${symbol} ${interval} candles`
            );

            console.log(
                `From ${new Date(startTime).toISOString()}`
            );

            console.log(
                `To   ${new Date(endTime).toISOString()}`
            );

            const candles =
                await fetchHistoricalCandles({
                    symbol,
                    interval,
                    startTime,
                    endTime
                });

            console.log(
                `Retrieved ${candles.length} candles.`
            );

            console.log(
                JSON.stringify(
                    candles.slice(0, 3),
                    null,
                    2
                )
            );

        } catch (error) {
            console.error(
                "HISTORY ENGINE ERROR:",
                error.message
            );

            process.exitCode = 1;
        }
    })();
}


module.exports = {
    INTERVALS,
    fetchHistoricalCandles
};
