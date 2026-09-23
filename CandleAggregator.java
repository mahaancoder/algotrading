package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.Candle;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CandleAggregator {

    // symbol → current forming candle
    private final Map<String, Candle> currentCandleMap = new ConcurrentHashMap<>();

    // symbol → completed candles
    private final Map<String, List<Candle>> candleHistoryMap = new ConcurrentHashMap<>();

    private static final int MAX_CANDLES = 100;

    // Lazy injection to avoid circular dependency
    private MarketContextBuilder marketContextBuilder;

    public CandleAggregator(@Lazy MarketContextBuilder marketContextBuilder) {
        this.marketContextBuilder = marketContextBuilder;
    }

    public void onTick(String symbol, double price, long volume) {
        LocalDateTime now = LocalDateTime.now();
        Candle current = currentCandleMap.get(symbol);

        // 🔥 If no candle OR new minute → create new candle
        if (current == null || isNewMinute(current.getTime(), now)) {
            if (current != null) {
                // New candle formed - add to history
                candleHistoryMap.computeIfAbsent(symbol, k -> new ArrayList<>()).add(current);
                trim(symbol);

                // Invalidate ATR cache since we have a new candle
                if (marketContextBuilder != null) {
                    marketContextBuilder.invalidateATRCache(symbol);
                }
            }
            current = new Candle(price, price, price, price, now);
            current.setVolume(volume); // Set initial volume
            currentCandleMap.put(symbol, current);
        } else {
            // 🔥 Update existing candle
            current.setClose(price);
            current.setHigh(Math.max(current.getHigh(), price));
            current.setLow(Math.min(current.getLow(), price));
            current.setVolume(volume); // Update volume with latest tick volume
        }
    }

    public List<Candle> getCandles(String symbol) {
        return candleHistoryMap.getOrDefault(symbol, Collections.emptyList());
    }

    public Candle getLatestCandle(String symbol) {
        return currentCandleMap.get(symbol);
    }

    private boolean isNewMinute(LocalDateTime oldTime, LocalDateTime newTime) {
        return oldTime.getMinute() != newTime.getMinute();
    }

    private void trim(String symbol) {
        List<Candle> list = candleHistoryMap.get(symbol);
        if (list.size() > MAX_CANDLES) {
            list.remove(0);
        }
    }

    /**
     * Check if the last 2 completed 1-minute candles are green (close > open)
     * @param symbol the stock symbol
     * @return true if last 2 candles are green, false otherwise
     */
    public boolean hasLastTwoCandlesGreen(String symbol) {
        List<Candle> candles = candleHistoryMap.getOrDefault(symbol, Collections.emptyList());

        // Need at least 2 completed candles
        if (candles.size() < 2) {
            return false;
        }

        // Get last 2 candles
        Candle lastCandle = candles.get(candles.size() - 1);
        Candle secondLastCandle = candles.get(candles.size() - 2);

        // Check if both are green (close > open)
        boolean lastIsGreen = lastCandle.getClose() > lastCandle.getOpen();
        boolean secondLastIsGreen = secondLastCandle.getClose() > secondLastCandle.getOpen();

        return lastIsGreen && secondLastIsGreen;
    }

}