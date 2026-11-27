package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.order.PendingOrderRepository;

import java.util.Locale;

/** Shared strategy guard for the momentum-only trailing exit policy. */
public final class MomentumTrailingPolicy {
    public static final String STRATEGY = "Dip-Accumulator-Momentum";

    private MomentumTrailingPolicy() {
    }

    /**
     * Only classify a live position as momentum-managed when its recorded BUY
     * order IDs resolve to completed momentum BUYs in the durable order ledger.
     * Broker snapshots can use the momentum strategy as a legacy placeholder;
     * those un-attributed/manual positions must retain the legacy exit policy.
     */
    public static boolean isManagedPosition(Position position, PendingOrderRepository orders) {
        if (position == null || orders == null || !isManagedStrategy(position.getStrategy())
                || position.getBuyOrderIds() == null || position.getBuyOrderIds().isEmpty()) {
            return false;
        }
        String broker = position.getBroker() != null ? position.getBroker() : "KITE";
        for (String orderId : position.getBuyOrderIds()) {
            PendingOrder pending = orders.findByBrokerAndBrokerOrderId(broker, orderId).orElse(null);
            if (pending != null && pending.getSide() == TradeSide.BUY
                    && isManagedStrategy(pending.getStrategy())
                    && position.getSymbol() != null
                    && position.getSymbol().equalsIgnoreCase(pending.getSymbol())
                    && pending.isCompleted() && pending.getFilledQty() > 0) {
                return true;
            }
        }
        return false;
    }

    public static boolean isManagedStrategy(String strategy) {
        if (strategy == null) return false;
        String normalized = strategy.trim();
        if (normalized.endsWith("#MIS")) {
            normalized = normalized.substring(0, normalized.length() - 4);
        }
        return STRATEGY.equalsIgnoreCase(normalized.toUpperCase(Locale.ROOT));
    }
}
