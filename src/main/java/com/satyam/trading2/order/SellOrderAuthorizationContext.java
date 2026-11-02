package com.satyam.trading2.order;

import java.util.function.Supplier;

/**
 * Narrow, thread-bound capability proving a SELL was submitted by the guarded
 * BrokerOrderRouter after broker inventory and outstanding reservations passed.
 * Direct broker.placeOrder(SELL, ...) calls fail closed.
 */
public final class SellOrderAuthorizationContext {
    private static final ThreadLocal<Integer> DEPTH = ThreadLocal.withInitial(() -> 0);

    private SellOrderAuthorizationContext() { }

    public static boolean isAuthorized() {
        return DEPTH.get() > 0;
    }

    public static <T> T withAuthorization(Supplier<T> action) {
        DEPTH.set(DEPTH.get() + 1);
        try {
            return action.get();
        } finally {
            int depth = DEPTH.get() - 1;
            if (depth <= 0) DEPTH.remove();
            else DEPTH.set(depth);
        }
    }
}
