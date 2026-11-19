package com.satyam.trading2.helpers;

import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class LatestPriceHelper {

    @Getter
    private static final Map<String, Double> latestPriceMap = new ConcurrentHashMap<>();
    private static final Map<String, Long> lastPriceUpdateMap = new ConcurrentHashMap<>();

    public static void updatePrice(String symbol, double price) {
        if (symbol == null || !Double.isFinite(price) || price <= 0) return;
        latestPriceMap.put(symbol, price);
        lastPriceUpdateMap.put(symbol, System.currentTimeMillis());
    }

    public static double getLatestPrice(String symbol) {
        return latestPriceMap.getOrDefault(symbol, 0.0);
    }

    /** Timestamp of the actual market tick, not a periodic dashboard rebroadcast. */
    public static long getLastPriceUpdateTime(String symbol) {
        return lastPriceUpdateMap.getOrDefault(symbol, 0L);
    }

}
