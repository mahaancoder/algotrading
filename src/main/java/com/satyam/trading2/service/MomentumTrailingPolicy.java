package com.satyam.trading2.service;

import java.util.Locale;

/** Shared strategy guard for the momentum-only trailing exit policy. */
public final class MomentumTrailingPolicy {
    public static final String STRATEGY = "Dip-Accumulator-Momentum";

    private MomentumTrailingPolicy() {
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
