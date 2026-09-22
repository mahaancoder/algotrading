package com.satyam.trading2.broker.model;

/**
 * Status of a broker order. Broker-neutral; each broker maps its own status
 * strings to these values (e.g. Kite "COMPLETE" → COMPLETE, Shoonya "FILLED" → COMPLETE).
 *
 * Moved from datamodel.BrokerOrderStatus — same enum, new home in the broker package.
 * The datamodel copy is kept temporarily for compile-during-refactor and will be removed.
 */
public enum BrokerOrderStatus {
    OPEN,
    PARTIALLY_FILLED,
    COMPLETE,
    CANCELLED,
    REJECTED
}
