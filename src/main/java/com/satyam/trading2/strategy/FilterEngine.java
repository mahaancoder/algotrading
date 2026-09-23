package com.satyam.trading2.strategy;

import com.satyam.trading2.datamodel.EntryType;
import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.service.CandleAggregator;
import com.satyam.trading2.service.MarketContextBuilder;
import com.satyam.trading2.service.RelativeStrengthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.atomic.AtomicLong;

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

    // ─────────────────────────────────────────────────────────────────────
    // Routing monitoring counters (reset daily). Track how BUY signals split
    // across Group 1 / Group 2 / Neither so we can see if everything is
    // falling through to the default broker instead of matching a group.
    // ─────────────────────────────────────────────────────────────────────
    private final AtomicLong group1Count = new AtomicLong();
    private final AtomicLong group2Count = new AtomicLong();
    private final AtomicLong neitherCount = new AtomicLong();

    // Reason breakdown for the NEITHER bucket
    private final AtomicLong reasonNotTopGainer = new AtomicLong();
    private final AtomicLong reasonNoTwoGreenCandles = new AtomicLong();
    private final AtomicLong reasonThresholdsFailed = new AtomicLong();

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
            System.out.println(String.format("[FilterEngine] %s → Group 1 MATCH (Sub-filter A: TOP_GAINER + 2 green candles + %dmin) → INDSTOCKS",
                    signal.getSymbol(), minutesSinceOpen));
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
                    System.out.println(String.format("[FilterEngine] %s → Group 1 MATCH (Sub-filter B: Nifty5d=%s%% + gap=%s%% + ATR%%=%s) → INDSTOCKS",
                            symbol, String.format("%.2f", nifty5DayReturn),
                            String.format("%.3f", gapPercent), String.format("%.3f", atrPercent)));
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
                    log.info("[FilterEngine] {} → Group 1 MATCH (Sub-filter C: Nifty5d={}% + ATR%={} + distFromDayLow={}%) → INDSTOCKS",
                            symbol, String.format("%.2f", nifty5DayReturn),
                            String.format("%.3f", atrPercent), String.format("%.3f", distanceFromDayLow));
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
            log.info("[FilterEngine] {} → Group 2 MATCH (Sub-filter A: TOP_GAINER + 2 green candles + RS={}%) → SHOONYA",
                    symbol, String.format("%.2f", fiveDayReturn));
            return true;
        }

        // Shared values for Sub-filters B and C
        double nifty5DayReturn = relativeStrengthService.getNiftyFiveDayReturn();
        double atr = marketContextBuilder.getATR(symbol);
        double atrPercent = signal.getEntryPrice() > 0 ? (atr / signal.getEntryPrice()) * 100.0 : 0;

        // ── Sub-filter B: Nifty 5-day return + ATR ──
        if (nifty5DayReturn >= 1.384 && atrPercent >= 0.169) {
            log.info("[FilterEngine] {} → Group 2 MATCH (Sub-filter B: Nifty5d={}% + ATR%={}) → SHOONYA",
                    symbol, String.format("%.2f", nifty5DayReturn), String.format("%.3f", atrPercent));
            return true;
        }

        // ── Sub-filter C: Nifty 5-day return + ATR + distanceFromDayLow ──
        if (nifty5DayReturn >= 1.384 && atrPercent >= 0.169) {
            double dayLow = getDayLow(symbol);
            if (dayLow > 0) {
                double distanceFromDayLow = ((signal.getEntryPrice() - dayLow) / dayLow) * 100.0;
                if (distanceFromDayLow >= 0.213) {
                    log.info("[FilterEngine] {} → Group 2 MATCH (Sub-filter C: Nifty5d={}% + ATR%={} + distFromDayLow={}%) → SHOONYA",
                            symbol, String.format("%.2f", nifty5DayReturn),
                            String.format("%.3f", atrPercent), String.format("%.3f", distanceFromDayLow));
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

        // ── Routing monitoring: bucket this signal (Group1 priority) ──
        if (g1) {
            group1Count.incrementAndGet();
        } else if (g2) {
            group2Count.incrementAndGet();
        } else {
            neitherCount.incrementAndGet();
            classifyNeitherReason(signal);
        }

        if (g1 || g2) {
            System.out.println(String.format("[FilterEngine] %s → Group1=%s Group2=%s → Route=%s",
                    signal.getSymbol(), g1, g2, result.getExecutionRoute()));
        } else {
            // Previously silent — this is the case where the signal falls
            // through to the default/fallback broker (KITE).
            System.out.println(String.format("[FilterEngine] %s → Group1=false Group2=false → NEITHER (fallback Route=%s)",
                    signal.getSymbol(), result.getExecutionRoute()));
        }

        return result;
    }

    /**
     * No-trade veto filter. If a BUY signal matches ANY of these reject rules,
     * we do NOT take the trade — regardless of which broker group it routes to.
     *
     * Reject Filter 1:
     *   distanceFromOpenPrice >= 2.275
     *   AND volumeRatio <= 3.402
     *   AND nifty5DayReturn <= -0.413
     *
     * Reject Filter 2:
     *   distanceFromOpenPrice >= 1.829
     *   AND nifty5DayReturn <= -0.413
     *   AND twoGreenCandles = FALSE
     *
     * @return a human-readable rejection reason if the trade should be blocked,
     *         or null if the trade is allowed.
     */
    public String getNoTradeRejectionReason(TradeSignal signal) {
        String symbol = signal.getSymbol();
        double entryPrice = signal.getEntryPrice();

        double previousClose = marketContextBuilder.getPreviousClose(symbol);
        double openPrice = getOpenPrice(symbol, previousClose);
        double distanceFromOpenPrice = openPrice > 0 ? ((entryPrice - openPrice) / openPrice) * 100.0 : 0;
        double nifty5DayReturn = relativeStrengthService.getNiftyFiveDayReturn();
        double volumeRatio = computeVolumeRatio(symbol);
        boolean twoGreenCandles = candleAggregator.hasLastTwoCandlesGreen(symbol);

        // ── Reject Filter 1 ──
        if (distanceFromOpenPrice >= 2.275 && volumeRatio <= 3.402 && nifty5DayReturn <= -0.413) {
            return String.format(
                    "No-trade filter 1 (distFromOpen=%.3f%% >= 2.275 & volRatio=%.3f <= 3.402 & nifty5d=%.3f%% <= -0.413)",
                    distanceFromOpenPrice, volumeRatio, nifty5DayReturn);
        }

        // ── Reject Filter 2 ──
        if (distanceFromOpenPrice >= 1.829 && nifty5DayReturn <= -0.413 && !twoGreenCandles) {
            return String.format(
                    "No-trade filter 2 (distFromOpen=%.3f%% >= 1.829 & nifty5d=%.3f%% <= -0.413 & twoGreenCandles=false)",
                    distanceFromOpenPrice, nifty5DayReturn);
        }

        return null;
    }

    /**
     * Compute the current candle's volume ratio vs the 20-candle average,
     * mirroring TradeMetricsCollector.calculateVolumeMetrics.
     */
    private double computeVolumeRatio(String symbol) {
        java.util.List<com.satyam.trading2.datamodel.Candle> candles = candleAggregator.getCandles(symbol);
        com.satyam.trading2.datamodel.Candle latestCandle = candleAggregator.getLatestCandle(symbol);
        if (latestCandle == null || candles == null || candles.isEmpty()) {
            return 0;
        }
        double currentVolume = latestCandle.getVolume();
        int count = Math.min(20, candles.size());
        double totalVolume = 0;
        for (int i = candles.size() - count; i < candles.size(); i++) {
            totalVolume += candles.get(i).getVolume();
        }
        double averageVolume20 = count > 0 ? totalVolume / count : 0;
        return averageVolume20 > 0 ? currentVolume / averageVolume20 : 0;
    }

    /**
     * Classify why a signal matched NEITHER group. Uses the same two shared
     * gates the group methods apply first (entryType == TOP_GAINER, then two
     * green candles); if both gates pass but no sub-filter matched, the
     * thresholds themselves are what failed.
     */
    private void classifyNeitherReason(TradeSignal signal) {
        if (signal.getEntryType() != EntryType.TOP_GAINER) {
            reasonNotTopGainer.incrementAndGet();
        } else if (!candleAggregator.hasLastTwoCandlesGreen(signal.getSymbol())) {
            reasonNoTwoGreenCandles.incrementAndGet();
        } else {
            reasonThresholdsFailed.incrementAndGet();
        }
    }

    /**
     * Periodic routing summary (every 15 min during market hours) so it is
     * visible in logs how signals split across groups vs the fallback broker.
     * Mirrors SignalAnalyticsService's scheduled-summary pattern.
     */
    @Scheduled(fixedDelay = 900_000) // 15 minutes
    public void printRoutingSummary() {
        DayOfWeek today = LocalDate.now().getDayOfWeek();
        if (today == DayOfWeek.SATURDAY || today == DayOfWeek.SUNDAY) {
            return;
        }

        LocalTime now = LocalTime.now();
        if (now.isBefore(LocalTime.of(9, 15)) || now.isAfter(LocalTime.of(15, 30))) {
            return;
        }

        long g1 = group1Count.get();
        long g2 = group2Count.get();
        long neither = neitherCount.get();
        long total = g1 + g2 + neither;
        if (total == 0) {
            return;
        }

        System.out.println(String.format(
                "\n🧭 [FilterEngine] Routing summary (today): total=%d | Group1(INDSTOCKS)=%d | Group2(SHOONYA)=%d | Neither(fallback)=%d"
                + "\n   ↳ NEITHER reasons: notTopGainer=%d | noTwoGreenCandles=%d | passedGatesButThresholdsFailed=%d",
                total, g1, g2, neither,
                reasonNotTopGainer.get(), reasonNoTwoGreenCandles.get(), reasonThresholdsFailed.get()));
    }

    /**
     * Reset routing counters at the start of each trading day.
     */
    @Scheduled(cron = "0 0 9 * * MON-FRI")
    public void resetDailyRoutingCounters() {
        group1Count.set(0);
        group2Count.set(0);
        neitherCount.set(0);
        reasonNotTopGainer.set(0);
        reasonNoTwoGreenCandles.set(0);
        reasonThresholdsFailed.set(0);
        System.out.println("🧭 [FilterEngine] Daily routing counters reset");
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
