package com.satyam.trading2.service;

import com.satyam.trading2.broker.ExecutionRoute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds the dashboard-selected DEFAULT execution broker.
 *
 * This is the broker used when a signal does NOT match any group filter
 * (i.e. FilterEngine resolves the "neither" case). Historically that always
 * meant KITE; now the operator can pick INDSTOCKS or SHOONYA as the default
 * for a given day directly from the dashboard.
 *
 * Group 1 / Group 2 filter matches still take priority — this only changes
 * the fallback that would otherwise have gone to KITE.
 *
 * Defaults to KITE (the always-available fallback broker).
 */
@Slf4j
@Service
public class DefaultBrokerService {

    private final AtomicReference<ExecutionRoute> defaultRoute =
            new AtomicReference<>(ExecutionRoute.KITE);

    /** @return the currently selected default execution route. */
    public ExecutionRoute getDefaultRoute() {
        return defaultRoute.get();
    }

    /**
     * Set the default execution route from a broker name (case-insensitive).
     *
     * @param brokerName KITE / INDSTOCKS / SHOONYA
     * @return the resolved route that was set
     * @throws IllegalArgumentException if the name is not a valid route
     */
    public ExecutionRoute setDefaultRoute(String brokerName) {
        ExecutionRoute route = ExecutionRoute.valueOf(brokerName.trim().toUpperCase());
        return setDefaultRoute(route);
    }

    /** Set the default execution route. */
    public ExecutionRoute setDefaultRoute(ExecutionRoute route) {
        ExecutionRoute previous = defaultRoute.getAndSet(route);
        log.info("🎯 [DefaultBroker] Default broker changed: {} → {}", previous, route);
        return route;
    }
}
