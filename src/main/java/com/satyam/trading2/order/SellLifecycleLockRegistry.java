package com.satyam.trading2.order;

import com.satyam.trading2.datamodel.Position;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Process-wide symbol locks for BUY and SELL lifecycle operations.
 *
 * Every target replacement, forced exit and completion handler must use the same
 * key so that independent schedulers/webhooks cannot cancel/replace the same
 * position concurrently. The conservative symbol-only key coordinates broker-routing
 * decisions made before the owning broker is known. This is JVM-local, not a distributed
 * lock and must not be treated as one when running multiple application instances.
 */
public final class SellLifecycleLockRegistry {

    private static final ConcurrentMap<String, Object> LOCKS = new ConcurrentHashMap<>();

    private SellLifecycleLockRegistry() {
    }

    public static Object lockFor(Position position) {
        if (position == null) {
            throw new IllegalArgumentException("position must not be null");
        }
        String broker = position.getBroker();
        if (broker == null || broker.trim().isEmpty()) {
            broker = position.getExecutionRoute() != null
                    ? position.getExecutionRoute().name() : "KITE";
        }
        return lockFor(
                position.getSymbol(),
                position.getStrategy(),
                broker,
                position.getPositionType()
        );
    }

    public static Object lockFor(String symbol, String strategy, String broker,
                                 Position.PositionType positionType) {
        // A BUY can be routed before the broker/position is resolved, while a
        // SELL is keyed by its owning broker. Use a conservative symbol-wide lock
        // so BUY, SELL, webhooks, schedulers, and MIS→CNC conversion serialize
        // even when strategy, broker, or product metadata differs.
        String key = normalize(symbol);
        return LOCKS.computeIfAbsent(key, ignored -> new Object());
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(java.util.Locale.ROOT);
    }
}
