package com.satyam.trading2.broker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Broker-neutral response returned by Broker.placeOrder().
 *
 * Carries the broker-assigned order id (which may collide across brokers, so
 * always pair it with the ExecutionRoute / broker name) plus the internal id
 * our system generated for correlation. Replaces the old OrderResponse which
 * was Kite-shaped (entryOrderId/targetOrderId/stopLossOrderId).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BrokerOrderResponse {

    /** Broker-assigned order id (e.g. Kite's order_id). Unique only within that broker. */
    private String brokerOrderId;

    /** Internal id our system generated for this order, for cross-broker correlation. */
    private String internalOrderId;

    /** Which broker this order was placed at. */
    private String brokerName;

    /** True if the broker accepted the order (even if not yet filled). */
    private boolean accepted;

    /** Rejection reason if the order was not accepted, else null. */
    private String rejectionReason;

    /** Raw broker response, for debugging only. */
    private String rawResponse;
}
