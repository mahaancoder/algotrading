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
        return lockFor(
                position.getSymbol(),
                position.getStrategy(),
                position.getBroker(),
                position.getPositionType()
        );
    }

    public static Object lockFor(String symbol, String strategy, String broker,
                                 Position.PositionType positionType) {
        String key = normalize(broker) + "::" + normalize(symbol) + "::"
                + normalize(strategy) + "::" + String.valueOf(positionType);
        return LOCKS.computeIfAbsent(key, ignored -> new Object());
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(java.util.Locale.ROOT);
    }
}
