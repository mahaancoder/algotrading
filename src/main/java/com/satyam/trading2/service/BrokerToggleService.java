package com.satyam.trading2.service;

import com.satyam.trading2.broker.ExecutionRoute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service to enable/disable trading on a per-broker basis.
 *
 * When a broker is disabled, no NEW buy orders will be routed to it.
 * The system falls back to KITE (which is always enabled).
 * SELL/exits on existing positions at that broker are still allowed —
 * you must be able to exit a position even if the broker is toggled off.
 *
 * KITE is always enabled because it is the market-data source and
 * the fallback broker.
 */
@Slf4j
@Service
public class BrokerToggleService {

    private final Map<ExecutionRoute, AtomicBoolean> enabledByRoute = new ConcurrentHashMap<>();

    public BrokerToggleService() {
        // All brokers enabled by default; KITE is always enabled
        for (ExecutionRoute route : ExecutionRoute.values()) {
            enabledByRoute.put(route, new AtomicBoolean(true));
        }
    }

    public boolean isEnabled(ExecutionRoute route) {
        if (route == ExecutionRoute.KITE) return true; // KITE always enabled
        AtomicBoolean flag = enabledByRoute.get(route);
        return flag == null || flag.get();
    }

    public boolean isEnabled(String brokerName) {
        try {
            return isEnabled(ExecutionRoute.valueOf(brokerName));
        } catch (IllegalArgumentException e) {
            return true; // unknown broker → don't block
        }
    }

    public void enable(ExecutionRoute route) {
        if (route == ExecutionRoute.KITE) {
            log.info("[BrokerToggle] KITE is always enabled — ignoring enable request");
            return;
        }
        enabledByRoute.get(route).set(true);
        log.info("✅ [BrokerToggle] {} trading ENABLED", route);
    }

    public void disable(ExecutionRoute route) {
        if (route == ExecutionRoute.KITE) {
            log.warn("[BrokerToggle] Cannot disable KITE — it is the fallback broker");
            return;
        }
        enabledByRoute.get(route).set(false);
        log.warn("🛑 [BrokerToggle] {} trading DISABLED — new buys will fall back to KITE", route);
    }

    public boolean toggle(ExecutionRoute route) {
        if (route == ExecutionRoute.KITE) {
            log.warn("[BrokerToggle] Cannot toggle KITE — it is always enabled");
            return true;
        }
        AtomicBoolean flag = enabledByRoute.get(route);
        boolean newState = !flag.get();
        flag.set(newState);
        if (newState) {
            log.info("✅ [BrokerToggle] {} trading ENABLED", route);
        } else {
            log.warn("🛑 [BrokerToggle] {} trading DISABLED — new buys will fall back to KITE", route);
        }
        return newState;
    }

    /** Snapshot of all broker toggle states (for dashboard / API). */
    public Map<String, Boolean> getAllStates() {
        Map<String, Boolean> states = new EnumMap<>(ExecutionRoute.class) instanceof EnumMap
                ? new java.util.LinkedHashMap<>() : new java.util.LinkedHashMap<>();
        for (ExecutionRoute route : ExecutionRoute.values()) {
            states.put(route.name(), isEnabled(route));
        }
        return states;
    }
}
