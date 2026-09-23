package com.satyam.trading2.helpers;

import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class to store and retrieve latest VWAP for symbols
 * Similar to LatestPriceHelper, but for VWAP values from tick data
 */
@Service
public class LatestVwapHelper {

    @Getter
    private static final Map<String, Double> latestVwapMap = new ConcurrentHashMap<>();

    /**
     * Update VWAP for a symbol
     * Called from WebSocketService when tick arrives with VWAP data
     */
    public static void updateVwap(String symbol, double vwap) {
        latestVwapMap.put(symbol, vwap);
    }

    /**
     * Get latest VWAP for a symbol
     * Returns 0.0 if not available
     */
    public static double getLatestVwap(String symbol) {
        return latestVwapMap.getOrDefault(symbol, 0.0);
    }
}

