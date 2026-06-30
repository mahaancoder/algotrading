package com.satyam.trading2.risk;

import org.springframework.scheduling.annotation.Scheduled;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Exits {
    private static Map<String, Long> exitedTodayMap = new ConcurrentHashMap<>();

    @Scheduled(cron = "0 0 9 * * MON-FRI")
    public void resetDailyExits() {
        int exitedCount = exitedTodayMap.size();
        exitedTodayMap.clear();
        System.out.println("✅ [RiskManager] Daily reset: Cleared " + exitedCount + " symbols from exited-today list");
    }

    public static boolean isExitedToday(String symbol, String strategy) {
        return exitedTodayMap.containsKey(makeKey(symbol, strategy));
    }

    /**
     * Check if a symbol was exited today by ANY strategy
     */
    public static boolean isSymbolExitedToday(String symbol) {
        return exitedTodayMap.keySet().stream()
                .anyMatch(key -> key.startsWith(symbol + "_"));
    }

    public static void markAsExitedToday(String symbol, String strategy) {
        exitedTodayMap.put(makeKey(symbol, strategy), System.currentTimeMillis());
        System.out.println("🚫 [RiskManager] Marked " + symbol + " as exited today by " + strategy + " - no re-entry allowed until tomorrow");
    }

    private static String makeKey(String symbol, String strategy) {
        return symbol + "_" + strategy;
    }

}
