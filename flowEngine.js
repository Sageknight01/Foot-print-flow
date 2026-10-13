"use strict";

/*
 * FOOTPRINT FLOW
 * Shared progression calculation engine.
 *
 * Responsibilities:
 * 1. Aggregate candles into a selected timeframe.
 * 2. Aggregate raw trades into candles when required.
 * 3. Calculate Buy Taker Progression.
 * 4. Calculate Sell Taker Progression.
 *
 * All timestamps are Unix milliseconds.
 * All volumes use the base asset quantity.
 */

const INTERVALS = Object.freeze({
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400
});


/*
 * Return interval length in milliseconds.
 */

function intervalMs(interval) {
    const seconds = INTERVALS[interval];

    if (!seconds) {
        throw new Error(
            `Unsupported candle interval: ${interval}`
        );
    }

    return seconds * 1000;
}


/*
 * Sort data chronologically and remove duplicate timestamps.
 *
 * If duplicate candles exist, the last occurrence is retained.
 */

function sortUniqueCandles(candles) {
    const byTime = new Map();

    for (const candle of candles || []) {
        const time = Number(candle.time);

        if (!Number.isFinite(time) || time <= 0) {
            continue;
        }

        byTime.set(time, {
            ...candle,
            time
        });
    }

    return Array.from(byTime.values())
        .sort((a, b) => a.time - b.time);
}


/*
 * Aggregate smaller candles into the selected timeframe.
 *
 * IMPORTANT:
 * Aggregate BUY and SELL volumes first.
 * Calculate progression only after aggregation.
 *
 * Input candle format:
 *
 * {
 *   time: Unix milliseconds,
 *   open: number,
 *   high: number,
 *   low: number,
 *   close: number,
 *   buy: number,
 *   sell: number
 * }
 */

function aggregateCandles(candles, interval) {
    const duration = intervalMs(interval);

    const sorted = sortUniqueCandles(candles);

    const grouped = new Map();

    for (const candle of sorted) {
        const bucketTime =
            Math.floor(candle.time / duration) * duration;

        const open = Number(candle.open);
        const high = Number(candle.high);
        const low = Number(candle.low);
        const close = Number(candle.close);

        const buy = Number(candle.buy);
        const sell = Number(candle.sell);

        if (
            !Number.isFinite(open) ||
            !Number.isFinite(high) ||
            !Number.isFinite(low) ||
            !Number.isFinite(close) ||
            !Number.isFinite(buy) ||
            !Number.isFinite(sell) ||
            buy < 0 ||
            sell < 0
        ) {
            continue;
        }

        let bucket = grouped.get(bucketTime);

        if (!bucket) {
            bucket = {
                time: bucketTime,
                open,
                high,
                low,
                close,
                buy: 0,
                sell: 0
            };

            grouped.set(bucketTime, bucket);
        }

        bucket.high = Math.max(
            bucket.high,
            high
        );

        bucket.low = Math.min(
            bucket.low,
            low
        );

        bucket.close = close;

        bucket.buy += buy;
        bucket.sell += sell;
    }

    return Array.from(grouped.values())
        .sort((a, b) => a.time - b.time);
}


/*
 * Convert raw trades into candles.
 *
 * Expected trade format:
 *
 * {
 *   time: Unix milliseconds,
 *   price: number,
 *   size: number,
 *   side: "Buy" or "Sell"
 * }
 *
 * Side must identify the aggressor side supplied by
 * the exchange's trade feed.
 */

function tradesToCandles(trades, interval) {
    const duration = intervalMs(interval);

    const sorted = [...(trades || [])]
        .filter(trade => {
            return (
                Number.isFinite(Number(trade.time)) &&
                Number.isFinite(Number(trade.price)) &&
                Number.isFinite(Number(trade.size)) &&
                Number(trade.time) > 0 &&
                Number(trade.price) > 0 &&
                Number(trade.size) >= 0 &&
                (
                    trade.side === "Buy" ||
                    trade.side === "Sell"
                )
            );
        })
        .sort((a, b) => {
            return Number(a.time) - Number(b.time);
        });

    const grouped = new Map();

    for (const trade of sorted) {
        const time = Number(trade.time);
        const price = Number(trade.price);
        const size = Number(trade.size);

        const bucketTime =
            Math.floor(time / duration) * duration;

        let candle = grouped.get(bucketTime);

        if (!candle) {
            candle = {
                time: bucketTime,
                open: price,
                high: price,
                low: price,
                close: price,
                buy: 0,
                sell: 0
            };

            grouped.set(bucketTime, candle);
        }

        candle.high = Math.max(
            candle.high,
            price
        );

        candle.low = Math.min(
            candle.low,
            price
        );

        candle.close = price;

        if (trade.side === "Buy") {
            candle.buy += size;
        } else {
            candle.sell += size;
        }
    }

    return Array.from(grouped.values())
        .sort((a, b) => a.time - b.time);
}


/*
 * Calculate the two independent progression lines.
 *
 * Formula:
 *
 * P[t] = P[t-1] + sign(V[t] - V[t-1])
 *
 * The first candle establishes the baseline at zero.
 *
 * An increase adds one.
 * A decrease subtracts one.
 * An unchanged volume preserves the previous value.
 */

function calculateProgressions(candles) {
    const sorted = sortUniqueCandles(candles);

    let buyProgression = 0;
    let sellProgression = 0;

    let previousBuy = null;
    let previousSell = null;

    return sorted.map(candle => {
        const buy = Number(candle.buy);
        const sell = Number(candle.sell);

        if (
            !Number.isFinite(buy) ||
            !Number.isFinite(sell) ||
            buy < 0 ||
            sell < 0
        ) {
            throw new Error(
                `Invalid buy/sell volume at ${candle.time}`
            );
        }

        if (previousBuy !== null) {
            if (buy > previousBuy) {
                buyProgression++;
            } else if (buy < previousBuy) {
                buyProgression--;
            }

            if (sell > previousSell) {
                sellProgression++;
            } else if (sell < previousSell) {
                sellProgression--;
            }
        }

        previousBuy = buy;
        previousSell = sell;

        return {
            ...candle,

            buyTakerVolume: buy,
            sellTakerVolume: sell,

            buyTakerProgression: buyProgression,
            sellTakerProgression: sellProgression
        };
    });
}


/*
 * Complete pipeline:
 *
 * Smaller candles
 *      ↓
 * Selected timeframe aggregation
 *      ↓
 * Buy/Sell progression calculation
 */

function buildProgressions(candles, interval) {
    const aggregated = aggregateCandles(
        candles,
        interval
    );

    return calculateProgressions(aggregated);
}


module.exports = {
    INTERVALS,
    intervalMs,
    sortUniqueCandles,
    aggregateCandles,
    tradesToCandles,
    calculateProgressions,
    buildProgressions
};
