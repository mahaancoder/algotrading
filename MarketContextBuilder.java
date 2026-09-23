package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.Candle;
import com.satyam.trading2.datamodel.MarketContext;
import com.satyam.trading2.datamodel.Nifty500Stocks;
import com.zerodhatech.models.Tick;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Service
@RequiredArgsConstructor
public class MarketContextBuilder {

    public static Map<String, OpeningRange> openingRangeMap = new ConcurrentHashMap<>(); // open range of 5 min// open range of 5 min
    private final CandleAggregator candleAggregator;
    private final ExecutionEngine  executionEngine;
    private final Map<String, Double> previousCloseMap = new ConcurrentHashMap<>();
    private final Map<String, Double> yesterdayHighMap = new ConcurrentHashMap<>();
    private final Map<String, Double> todayOpenPriceMap = new ConcurrentHashMap<>();

    // ATR Cache: symbol -> ATR value (recalculated only when new candle forms)
    private final Map<String, Double> atrCache = new ConcurrentHashMap<>();

    static double MAX_CAPITAL = 1000000;   // 10L
    static final int MAX_OPEN_POSITIONS = 40;  // Increased to 40 (25K per position)

    /**
     * Build market context for a specific symbol
     * Only called when canGenerateSignals() == true
     */
    public MarketContext build(String symbol, Tick tick) {
        // Fast checks: Data availability
        Candle latest = candleAggregator.getLatestCandle(symbol);
        if (latest == null) return null;

        List<Candle> candles = candleAggregator.getCandles(symbol);
        if (candles == null || candles.isEmpty()) return null;

        // Get or calculate ATR (cached, only recalculates on new candle)
        double atr = getOrCalculateATR(symbol, candles);

        return MarketContext.builder()
                .symbol(symbol)
                .atr(atr)
                .previousClose(previousCloseMap.getOrDefault(symbol, 0.0))
                .candle(latest)
                .tickWindow(TickWindow.getTickWindow(symbol))
                .Vwap(tick.getAverageTradePrice())
                .build();
    }

    /**
     * Check if current time is within BUY hours (9:15 AM - 3:15 PM)
     * This prevents strategies from running outside trading hours
     */
    private boolean isWithinTradingHours() {
        LocalTime now = LocalTime.now();
        DayOfWeek day = LocalDate.now().getDayOfWeek();

        // Block weekends
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return false;
        }

        // Market hours: 9:15 AM - 3:15 PM (BUY cutoff)
        LocalTime marketOpen = LocalTime.of(9, 15);
        LocalTime buyCutoff = LocalTime.of(15, 15);

        return !now.isBefore(marketOpen) && now.isBefore(buyCutoff);
    }


    public static class OpeningRange {
        public double high = 0;
        public double low = 0;
    }

    public static void setOpeningRangeMap(String symbol, OpeningRange range) {
        MarketContextBuilder.openingRangeMap.put(symbol, range);
    }
    /**
     * Get cached ATR or calculate if not available
     * ATR is expensive to calculate, so we cache it and only recalculate when needed
     */
    private double getOrCalculateATR(String symbol, List<Candle> candles) {
        // Check cache first
        Double cached = atrCache.get(symbol);
        if (cached != null) {
            return cached;
        }

        // Calculate and cache
        double atr = calculateATR(candles);
        if (atr > 0) {
            atrCache.put(symbol, atr);
        }
        return atr;
    }

    /**
     * Called by CandleAggregator when a new candle is formed
     * Invalidates ATR cache to force recalculation
     */
    public void invalidateATRCache(String symbol) {
        atrCache.remove(symbol);
    }

    /**
     * Get ATR for a symbol (public method for use in target calculation)
     * @param symbol the trading symbol
     * @return ATR value, or 0.0 if not available
     */
    public double getATR(String symbol) {
        List<Candle> candles = candleAggregator.getCandles(symbol);
        if (candles == null || candles.isEmpty()) {
            return 0.0;
        }
        return getOrCalculateATR(symbol, candles);
    }

    private double calculateATR(List<Candle> candles) {
        try {
            if (candles.size() < 15) return 0; // Need 14 + 1 for TR calculation

            double atr = 0;
            int count = 0;
            for (int i = candles.size() - 14; i < candles.size(); i++) {
                Candle c = candles.get(i);
                Candle prev = candles.get(i - 1);
                if (c != null && prev != null) {
                    double high = c.getHigh();
                    double low = c.getLow();
                    double prevClose = prev.getClose();
                    double tr = Math.max(high - low, Math.max(Math.abs(high - prevClose), Math.abs(low - prevClose)));
                    atr += tr;
                }
                count++;
            }
            return count > 0 ? atr / count : 0;
        } catch (Exception ignored) {
        }
        return 0;
    }

    public void resetForNewDay() {
        previousCloseMap.clear();
        yesterdayHighMap.clear();
        atrCache.clear();
    }

    public void setPreviousClose(String symbol, double price) {
        previousCloseMap.put(symbol, price);
    }

    public double getPreviousClose(String symbol) {
        return previousCloseMap.getOrDefault(symbol, 0.0);
    }

    public void setYesterdayHigh(String symbol, double price) {
        yesterdayHighMap.put(symbol, price);
    }

    public double getYesterdayHigh(String symbol) {
        return yesterdayHighMap.getOrDefault(symbol, 0.0);
    }

}