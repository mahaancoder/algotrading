package com.satyam.trading2.order;

import com.satyam.trading2.datamodel.Position;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Process-wide locks for SELL lifecycle operations.
 *
 * Every target replacement, forced exit and completion handler must use the same
 * key so that independent schedulers/webhooks cannot cancel/replace the same
 * position concurrently. This is intentionally JVM-local; it is not a distributed
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
        // Position type is intentionally excluded: an intraday position can be
        // converted to a holding while a lifecycle operation is in progress.
        // The broker owns one physical net position per symbol, even when the
        // application attributes buys to multiple strategies. Strategy-scoped
        // locks allowed two strategies to place SELLs concurrently against the
        // same shares. Serialize at broker + symbol scope.
        String key = normalize(broker) + "::" + normalize(symbol);
        return LOCKS.computeIfAbsent(key, ignored -> new Object());
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(java.util.Locale.ROOT);
    }
}
