package com.satyam.trading2.strategy;

import com.satyam.trading2.datamodel.EntryType;
import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.service.CandleAggregator;
import com.satyam.trading2.service.MarketContextBuilder;
import com.satyam.trading2.service.RelativeStrengthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalTime;

/**
 * FilterEngine — decides which broker group a TradeSignal routes to.
 *
 * This is SEPARATE from StockFilterService (which filters which stocks to
 * watch for gainers/losers selection). FilterEngine filters which BROKER
 * executes a given signal.
 *
 * Routing rules (per architecture):
 *   Group 1 match  → INDSTOCKS (execution-only broker)
 *   Group 2 match  → SHOONYA   (execution-only broker)
 *   Neither        → KITE      (fallback execution broker)
 *   Both match     → INDSTOCKS (Group 1 has priority)
 *
 * NOTE: The route returned here is the PREFERRED route. The actual route
 * may be overridden by OrderExecutionService if a position for the same
 * symbol+strategy already exists at a different broker (the "stickiness"
 * rule from Decision #2: re-buying the same stock must go to the existing
 * broker so correct averaging happens).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FilterEngine {

    private final CandleAggregator candleAggregator;
    private final RelativeStrengthService relativeStrengthService;
    private final MarketContextBuilder marketContextBuilder;

    // Market opens at 9:15 AM
    private static final LocalTime MARKET_OPEN = LocalTime.of(9, 15);

    /**
     * Check if a signal matches Group 1 rules (→ INDSTOCKS).
     *
     * Group 1 has THREE sub-filters combined with OR:
     *
     *   Sub-filter A (original):
     *     entryType = TOP_GAINER
     *     AND twoGreenCandles = TRUE
     *     AND minutesSinceOpen <= 10
     *
     *   Sub-filter B:
     *     entryType = TOP_GAINER
     *     AND twoGreenCandles = TRUE
     *     AND nifty5DayReturn >= 1.458
     *     AND gapPercent <= 0.048
     *     AND atrPercent >= 0.169
     *
     *   Sub-filter C:
     *     entryType = TOP_GAINER
     *     AND twoGreenCandles = TRUE
     *     AND nifty5DayReturn >= 1.384
     *     AND atrPercent >= 0.169
     *     AND distanceFromDayLow >= 0.213
     *
     * A signal matches Group 1 if it passes Sub-filter A OR B OR C.
     */
    public boolean matchesGroup1(TradeSignal signal) {
        // Common condition: entryType must be TOP_GAINER
        if (signal.getEntryType() != EntryType.TOP_GAINER) {
            return false;
        }

        // Common condition: last two completed candles must be green
        if (!candleAggregator.hasLastTwoCandlesGreen(signal.getSymbol())) {
            return false;
        }

        // ── Sub-filter A: within 10 minutes of market open ──
        LocalTime now = LocalTime.now();
        long minutesSinceOpen = java.time.Duration.between(MARKET_OPEN, now).toMinutes();
        if (minutesSinceOpen >= 0 && minutesSinceOpen <= 10) {
            log.debug("[FilterEngine] {} → Group 1 MATCH (Sub-filter A: TOP_GAINER + 2 green candles + {}min) → INDSTOCKS",
                    signal.getSymbol(), minutesSinceOpen);
            return true;
        }

        String symbol = signal.getSymbol();
        double atr = marketContextBuilder.getATR(symbol);
        double atrPercent = signal.getEntryPrice() > 0 ? (atr / signal.getEntryPrice()) * 100.0 : 0;
        double nifty5DayReturn = relativeStrengthService.getNiftyFiveDayReturn();

        // ── Sub-filter B: Nifty 5-day return + gap + ATR ──
        if (nifty5DayReturn >= 1.458 && atrPercent >= 0.169) {
            double previousClose = marketContextBuilder.getPreviousClose(symbol);
            if (previousClose > 0) {
                double openPrice = getOpenPrice(symbol, previousClose);
                double gapPercent = ((openPrice - previousClose) / previousClose) * 100.0;
                if (gapPercent <= 0.048) {
                    log.debug("[FilterEngine] {} → Group 1 MATCH (Sub-filter B: Nifty5d={:.2f}% + gap={:.3f}% + ATR%={:.3f}) → INDSTOCKS",
                            symbol, nifty5DayReturn, gapPercent, atrPercent);
                    return true;
                }
            }
        }

        // ── Sub-filter C: Nifty 5-day return + ATR + distanceFromDayLow ──
        if (nifty5DayReturn >= 1.384 && atrPercent >= 0.169) {
            double dayLow = getDayLow(symbol);
            if (dayLow > 0) {
                double distanceFromDayLow = ((signal.getEntryPrice() - dayLow) / dayLow) * 100.0;
                if (distanceFromDayLow >= 0.213) {
                    log.debug("[FilterEngine] {} → Group 1 MATCH (Sub-filter C: Nifty5d={:.2f}% + ATR%={:.3f} + distFromDayLow={:.3f}%) → INDSTOCKS",
                            symbol, nifty5DayReturn, atrPercent, distanceFromDayLow);
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Check if a signal matches Group 2 rules (→ SHOONYA).
     *
     * Group 2 has THREE sub-filters combined with OR:
     *
     *   Sub-filter A (original):
     *     entryType = TOP_GAINER
     *     AND twoGreenCandles = TRUE
     *     AND relativeStrength >= 0.84%  (stock's 5-day return >= 0.84%)
     *
     *   Sub-filter B:
     *     entryType = TOP_GAINER
     *     AND twoGreenCandles = TRUE
     *     AND nifty5DayReturn >= 1.384
     *     AND atrPercent >= 0.169
     *
     *   Sub-filter C:
     *     entryType = TOP_GAINER
     *     AND twoGreenCandles = TRUE
     *     AND nifty5DayReturn >= 1.384
     *     AND atrPercent >= 0.169
     *     AND distanceFromDayLow >= 0.213
     *
     * Note: When a signal matches BOTH Group 1 and Group 2, Group 1 (INDSTOCKS)
     * takes priority per the routing rules.
     */
    public boolean matchesGroup2(TradeSignal signal) {
        // Common condition: entryType must be TOP_GAINER
        if (signal.getEntryType() != EntryType.TOP_GAINER) {
            return false;
        }

        // Common condition: last two completed candles must be green
        if (!candleAggregator.hasLastTwoCandlesGreen(signal.getSymbol())) {
            return false;
        }

        String symbol = signal.getSymbol();

        // ── Sub-filter A: stock's relative strength (5-day return) >= 0.84% ──
        double fiveDayReturn = relativeStrengthService.getStockFiveDayReturn(symbol);
        if (fiveDayReturn >= 0.84) {
            log.debug("[FilterEngine] {} → Group 2 MATCH (Sub-filter A: TOP_GAINER + 2 green candles + RS={:.2f}%) → SHOONYA",
                    symbol, fiveDayReturn);
            return true;
        }

        // Shared values for Sub-filters B and C
        double nifty5DayReturn = relativeStrengthService.getNiftyFiveDayReturn();
        double atr = marketContextBuilder.getATR(symbol);
        double atrPercent = signal.getEntryPrice() > 0 ? (atr / signal.getEntryPrice()) * 100.0 : 0;

        // ── Sub-filter B: Nifty 5-day return + ATR ──
        if (nifty5DayReturn >= 1.384 && atrPercent >= 0.169) {
            log.debug("[FilterEngine] {} → Group 2 MATCH (Sub-filter B: Nifty5d={:.2f}% + ATR%={:.3f}) → SHOONYA",
                    symbol, nifty5DayReturn, atrPercent);
            return true;
        }

        // ── Sub-filter C: Nifty 5-day return + ATR + distanceFromDayLow ──
        if (nifty5DayReturn >= 1.384 && atrPercent >= 0.169) {
            double dayLow = getDayLow(symbol);
            if (dayLow > 0) {
                double distanceFromDayLow = ((signal.getEntryPrice() - dayLow) / dayLow) * 100.0;
                if (distanceFromDayLow >= 0.213) {
                    log.debug("[FilterEngine] {} → Group 2 MATCH (Sub-filter C: Nifty5d={:.2f}% + ATR%={:.3f} + distFromDayLow={:.3f}%) → SHOONYA",
                            symbol, nifty5DayReturn, atrPercent, distanceFromDayLow);
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Evaluate a signal against both group rules and return the result
     * with the resolved execution route (Group 1 priority applied).
     */
    public FilterResult evaluate(TradeSignal signal) {
        boolean g1 = matchesGroup1(signal);
        boolean g2 = matchesGroup2(signal);
        FilterResult result = FilterResult.from(g1, g2);

        if (log.isDebugEnabled() && (g1 || g2)) {
            log.debug("[FilterEngine] {} → Group1={} Group2={} → Route={}",
                    signal.getSymbol(), g1, g2, result.getExecutionRoute());
        }

        return result;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Helper: get today's open price (same logic as TradeMetricsCollector)
    // ─────────────────────────────────────────────────────────────────────
    private double getOpenPrice(String symbol, double previousClose) {
        // First, try to get the official opening price from Instrument OHLC data
        com.satyam.trading2.datamodel.Instrument instrument =
                com.satyam.trading2.datamodel.Nifty500Stocks.Nifty500SymbolToInstrument.get(symbol);
        if (instrument != null && instrument.getTodayOpenPrice() != null && instrument.getTodayOpenPrice() > 0) {
            return instrument.getTodayOpenPrice();
        }

        // Fallback: use first candle's open price
        java.util.List<com.satyam.trading2.datamodel.Candle> candles = candleAggregator.getCandles(symbol);
        if (candles != null && !candles.isEmpty()) {
            return candles.get(0).getOpen();
        }

        // Last resort: use previous close
        return previousClose;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Helper: get day low (same logic as TradeMetricsCollector)
    // ─────────────────────────────────────────────────────────────────────
    private double getDayLow(String symbol) {
        com.satyam.trading2.datamodel.Candle latestCandle = candleAggregator.getLatestCandle(symbol);
        java.util.List<com.satyam.trading2.datamodel.Candle> candles = candleAggregator.getCandles(symbol);
        if (candles == null || candles.isEmpty()) {
            return latestCandle != null ? latestCandle.getLow() : 0;
        }

        double dayLow = Double.MAX_VALUE;
        for (com.satyam.trading2.datamodel.Candle c : candles) {
            dayLow = Math.min(dayLow, c.getLow());
        }
        if (latestCandle != null) {
            dayLow = Math.min(dayLow, latestCandle.getLow());
        }
        return dayLow == Double.MAX_VALUE ? 0 : dayLow;
    }
}
