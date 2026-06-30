package com.satyam.trading2.helpers;

import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class LatestPriceHelper {

    @Getter
    private static final Map<String, Double> latestPriceMap = new ConcurrentHashMap<>();

    public static void updatePrice(String symbol, double price) {
        latestPriceMap.put(symbol, price);
    }

    public static double getLatestPrice(String symbol) {
        return latestPriceMap.getOrDefault(symbol, 0.0);
    }

}
