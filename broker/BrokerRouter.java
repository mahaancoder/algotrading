package com.satyam.trading2.broker;

import com.satyam.trading2.strategy.FilterResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * BrokerRouter — maps an ExecutionRoute to the concrete Broker instance.
 *
 * This is a thin registry: it holds references to all Broker beans and
 * provides getBroker(route). The routing DECISION (which route to use)
 * comes from FilterEngine via FilterResult; BrokerRouter just resolves
 * the route to the broker.
 *
 * Keeping routing logic (FilterEngine) and broker registry (BrokerRouter)
 * separate means:
 *  - FilterEngine can be tested without any broker dependencies
 *  - BrokerRouter can be tested with mock brokers
 *  - Adding a new broker only touches BrokerRouter, not FilterEngine
 */
@Slf4j
@Service
public class BrokerRouter {

    private final Map<ExecutionRoute, Broker> brokersByRoute = new EnumMap<>(ExecutionRoute.class);
    private final List<Broker> allBrokers;

    /**
     * Spring injects all Broker implementations. We index them by their
     * getRoute() so getBroker(route) is a simple map lookup.
     */
    public BrokerRouter(List<Broker> brokers) {
        this.allBrokers = brokers;
        for (Broker broker : brokers) {
            ExecutionRoute route = broker.getRoute();
            Broker previous = brokersByRoute.put(route, broker);
            if (previous != null) {
                log.warn("[BrokerRouter] Duplicate broker for route {} — replaced {} with {}",
                        route, previous.getBrokerName(), broker.getBrokerName());
            }
            log.info("[BrokerRouter] Registered broker {} for route {}", broker.getBrokerName(), route);
        }
    }

    /**
     * Resolve the ExecutionRoute from a FilterResult.
     * This is a passthrough — the route is already decided by FilterEngine.
     * Kept as a separate method so OrderExecutionService can call
     * determineRoute(result) + getBroker(route) in two clear steps.
     */
    public ExecutionRoute determineRoute(FilterResult result) {
        return result.getExecutionRoute();
    }

    /**
     * Get the Broker instance for a given route.
     * Returns null if no broker is registered for that route (e.g. INDstocks
     * or Shoonya not yet activated). Callers should handle null gracefully —
     * typically by falling back to KITE.
     */
    public Broker getBroker(ExecutionRoute route) {
        return brokersByRoute.get(route);
    }

    /**
     * Get the Broker for a route, falling back to KITE if the preferred
     * broker is not registered. This is the safe form for execution paths
     * that must always succeed.
     */
    public Broker getBrokerWithFallback(ExecutionRoute route) {
        Broker broker = brokersByRoute.get(route);
        if (broker != null) {
            return broker;
        }
        log.warn("[BrokerRouter] No broker registered for route {} — falling back to KITE", route);
        return brokersByRoute.get(ExecutionRoute.KITE);
    }

    /** All registered brokers (for reconciliation, dashboard, etc). */
    public List<Broker> getAllBrokers() {
        return allBrokers;
    }

    /** Check if a broker is available for a given route. */
    public boolean isAvailable(ExecutionRoute route) {
        return brokersByRoute.containsKey(route);
    }
}
