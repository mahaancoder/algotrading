package com.satyam.trading2.risk;

import org.springframework.scheduling.annotation.Scheduled;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Tracks symbols that were exited today, with price-aware re-entry rules.
 *
 * Decision #1: Re-entry IS allowed (within broker and cross-broker) but ONLY
 * when the new buy price is LOWER than the earlier buy price (the average
 * price of the position that was exited). This prevents re-buying at a higher
 * price right after exiting, while still allowing a genuine lower dip re-entry.
 *
 * The key is symbol+strategy (cross-broker): exiting RELIANCE on INDstocks
 * blocks a same-price re-entry on Kite too. The buyPrice stored is the
 * average price of the exited position, regardless of which broker it was at.
 */
public class Exits {

    private static Map<String, ExitRecord> exitedTodayMap = new ConcurrentHashMap<>();

    @Scheduled(cron = "0 0 9 * * MON-FRI")
    public void resetDailyExits() {
        int exitedCount = exitedTodayMap.size();
        exitedTodayMap.clear();
        System.out.println("✅ [Exits] Daily reset: Cleared " + exitedCount + " symbols from exited-today list");
    }

    public static boolean isExitedToday(String symbol, String strategy) {
        return exitedTodayMap.containsKey(makeKey(symbol, strategy));
    }

    /**
     * Check if a symbol was exited today by ANY strategy.
     */
    public static boolean isSymbolExitedToday(String symbol) {
        return exitedTodayMap.keySet().stream()
                .anyMatch(key -> key.startsWith(symbol + "_"));
    }

    /**
     * Check if re-entry is allowed for an exited symbol+strategy.
     * Per Decision #1: allowed only when newBuyPrice < the exited position's
     * average buy price. If no buy price was recorded (legacy), re-entry is
     * blocked (safe default).
     */
    public static boolean isReEntryAllowed(String symbol, String strategy, double newBuyPrice) {
        ExitRecord record = exitedTodayMap.get(makeKey(symbol, strategy));
        if (record == null) {
            return true; // not exited today at all — re-entry freely allowed
        }
        if (record.buyPrice <= 0) {
            return false; // no price recorded, block re-entry (safe default)
        }
        return newBuyPrice < record.buyPrice;
    }

    /**
     * Get the recorded buy price for an exited symbol+strategy (for logging).
     */
    public static Double getExitedBuyPrice(String symbol, String strategy) {
        ExitRecord record = exitedTodayMap.get(makeKey(symbol, strategy));
        return record != null ? record.buyPrice : null;
    }

    /**
     * Mark a symbol+strategy as exited today.
     * Backward-compat overload — no buy price recorded, so re-entry is blocked
     * (safe default). Existing callers (OrderCompletionProcessor.onSellCompleted)
     * should migrate to the price-aware overload.
     */
    public static void markAsExitedToday(String symbol, String strategy) {
        exitedTodayMap.put(makeKey(symbol, strategy), new ExitRecord(System.currentTimeMillis(), 0));
        System.out.println("🚫 [Exits] Marked " + symbol + " as exited today by " + strategy + " (no buy price recorded - re-entry blocked)");
    }

    /**
     * Mark a symbol+strategy as exited today, recording the average buy price
     * of the exited position. Re-entry is then allowed only at a lower price.
     *
     * @param buyPrice The average buy price of the position that was exited.
     *                 Used by isReEntryAllowed to enforce the "lower price only" rule.
     */
    public static void markAsExitedToday(String symbol, String strategy, double buyPrice) {
        exitedTodayMap.put(makeKey(symbol, strategy), new ExitRecord(System.currentTimeMillis(), buyPrice));
        System.out.println("🚫 [Exits] Marked " + symbol + " as exited today by " + strategy +
                " @ buyPrice=" + buyPrice + " - re-entry allowed only below this price");
    }

    private static String makeKey(String symbol, String strategy) {
        return symbol + "_" + strategy;
    }

    /**
     * Record of an exit event: when it happened and at what average buy price.
     * buyPrice is used by the price-aware re-entry rule (Decision #1).
     */
    private static class ExitRecord {
        final long timestamp;
        final double buyPrice;

        ExitRecord(long timestamp, double buyPrice) {
            this.timestamp = timestamp;
            this.buyPrice = buyPrice;
        }
    }

}
