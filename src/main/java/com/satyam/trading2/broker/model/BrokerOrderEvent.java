package com.satyam.trading2.broker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Common order-status event produced by every broker's order-update stream
 * (Kite webhook, INDstocks WebSocket, Shoonya WebSocket) and consumed by
 * BrokerOrderEventProcessor.
 *
 * Replaces datamodel.BrokerWebhookEvent. Differences:
 *  - adds broker + signalId for tracing / cross-broker correlation
 *  - drops symbol and side: those come from the PendingOrder looked up by
 *    brokerOrderId, which is the source of truth. Avoids trusting the inbound
 *    payload for routing decisions.
 *  - drops rawPayload: kept broker-internal.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BrokerOrderEvent {

    /** Which broker emitted this event (e.g. "KITE", "INDSTOCKS", "SHOONYA"). */
    private String broker;

    /** Broker-assigned order id this event refers to. Unique only within that broker. */
    private String brokerOrderId;

    /** Internal id our system generated for the originating order, if known. */
    private String signalId;

    /** New status of the order. */
    private BrokerOrderStatus status;

    /** Quantity filled so far (cumulative). */
    private int filledQuantity;

    /** Average fill price (0 until filled). */
    private double averagePrice;

    /** Rejection / cancellation reason, if any. */
    private String rejectionReason;
}
