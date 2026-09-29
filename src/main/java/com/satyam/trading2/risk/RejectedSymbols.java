package com.satyam.trading2.risk;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks symbols that hit a TERMINAL buy rejection today — a rejection that
 * cannot change for the rest of the trading day. Once a symbol is here, the tick
 * path skips it entirely (alongside the exited-today check), avoiding wasted
 * strategy evaluation + repeated log spam for buys that can never succeed again
 * today.
 *
 * IMPORTANT — only TERMINAL rejections belong here. A rejection is terminal only
 * if it is monotonic for the day and can be undone solely by a SELL (which sets
 * exited-today and skips the symbol anyway). Current terminal reasons:
 *   - Max accumulation levels reached (dip-state level only ever increases)
 *   - Total capital exceeds 100K limit (dip-state capital only grows)
 *   - Per-symbol capital cap with no headroom for even one more share
 *
 * Transient rejections must NOT be added, or the dip-accumulator would stop
 * watching the symbol and miss a later dip. Excluded (transient):
 *   - Cooldown (15s between buys)
 *   - Price too close to last entry (< 1%)
 *   - Accumulation not allowed (price condition)
 *   - Insufficient broker capital (frees up during the day)
 *   - Broker toggle disabled (operator can re-enable it)
 *
 * Keyed by symbol (not symbol+strategy) because the tick-path skip is per
 * ticker. Registered as a @Component so the @Scheduled daily reset actually
 * fires (@EnableScheduling is on KiteTraderApplication).
 */
@Component
public class RejectedSymbols {

    // symbol -> terminal rejection reason (for logging/diagnostics)
    private static final Map<String, String> rejectedTodayMap = new ConcurrentHashMap<>();

    @Scheduled(cron = "0 0 9 * * MON-FRI")
    public void resetDailyRejections() {
        int count = rejectedTodayMap.size();
        rejectedTodayMap.clear();
        System.out.println("✅ [RejectedSymbols] Daily reset: cleared " + count + " terminally-rejected symbols");
    }

    /**
     * Mark a symbol as terminally rejected for the rest of the day. Idempotent —
     * only logs the first time a symbol is added so repeated signals don't spam.
     */
    public static void markRejectedToday(String symbol, String reason) {
        if (symbol == null) return;
        String prev = rejectedTodayMap.putIfAbsent(symbol, reason);
        if (prev == null) {
            System.out.println("🚷 [RejectedSymbols] " + symbol +
                    " skipped for the rest of the day (terminal): " + reason);
        }
    }

    public static boolean isRejectedToday(String symbol) {
        return symbol != null && rejectedTodayMap.containsKey(symbol);
    }

    /** Remove a symbol from the terminal-reject list (e.g. if state changes). */
    public static void clear(String symbol) {
        if (symbol != null) rejectedTodayMap.remove(symbol);
    }
}
