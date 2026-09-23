package com.satyam.trading2.broker;

/**
 * Identifies which broker an order/position belongs to.
 *
 * Routing rules (enforced by FilterEngine + BrokerRouter):
 *   KITE      → "Neither" group — fallback execution broker (also the only market-data source)
 *   INDSTOCKS → Group 1 — execution-only broker
 *   SHOONYA   → Group 2 — execution-only broker
 *
 * Group 1 takes priority when both Group 1 and Group 2 match.
 */
public enum ExecutionRoute {
    KITE,
    INDSTOCKS,
    SHOONYA
}
