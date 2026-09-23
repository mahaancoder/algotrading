package com.satyam.trading2.broker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Broker-neutral trade (a filled buy or sell leg) returned by a broker's
 * trade-history / order-history REST endpoint.
 *
 * Used by BrokerReconciliationService to cross-check fills against our
 * PendingOrder / Position state. Each broker maps its own trade response
 * into this shape.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BrokerTrade {

    /** Which broker this trade was reported by. */
    private String brokerName;

    /** Broker-assigned order id this trade belongs to. */
    private String brokerOrderId;

    /** Trading symbol. */
    private String symbol;

    /** BUY or SELL. */
    private TransactionType transactionType;

    /** Filled quantity for this trade leg. */
    private int quantity;

    /** Fill price. */
    private double price;

    /** Exchange timestamp of the fill (epoch millis). */
    private long timestamp;
}
