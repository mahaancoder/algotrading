package com.satyam.trading2.broker;

import com.satyam.trading2.broker.model.BrokerFunds;
import com.satyam.trading2.broker.model.BrokerOrderResponse;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.broker.model.OrderRequest;

/**
 * Common abstraction for all execution brokers (Kite, INDstocks, Shoonya).
 *
 * Kite additionally serves as the market-data source, but that role is handled
 * by the market-data layer (WebSocketService / CandleAggregator) — NOT by this
 * interface. This interface covers execution only: placing/cancelling orders,
 * querying status/funds, receiving order-update events, and reconciliation.
 *
 * One signal → one broker only. Routing is decided by FilterEngine + BrokerRouter.
 */
public interface Broker {

    /** Stable identifier for this broker, e.g. "KITE", "INDSTOCKS", "SHOONYA". */
    String getBrokerName();

    /** Which ExecutionRoute this broker serves. */
    ExecutionRoute getRoute();

    /**
     * Place an order. The broker converts the broker-neutral OrderRequest into
     * its own API format and returns a BrokerOrderResponse carrying the
     * broker-assigned order id.
     */
    BrokerOrderResponse placeOrder(OrderRequest request);

    /** Cancel an open order by its broker-assigned id. */
    void cancelOrder(String brokerOrderId);

    /**
     * Modify an existing order (price, quantity, etc).
     * The broker maps the new OrderRequest onto the existing broker order.
     */
    void modifyOrder(String brokerOrderId, OrderRequest request);

    /** Query the current status of a single order. */
    BrokerOrderStatus getOrderStatus(String brokerOrderId);

    /**
     * Return the broker-confirmed cumulative filled quantity for an order.
     * Null means the broker/API did not provide a trustworthy value; callers
     * must not treat null as zero when replacing a SELL order.
     */
    default Integer getOrderFilledQuantity(String brokerOrderId) {
        return null;
    }

    /** Fetch available funds/margin at this broker. */
    BrokerFunds getFunds();

    /**
     * Start receiving order-update events (WebSocket for INDstocks/Shoonya,
     * enable webhook processing for Kite). Events are delivered to
     * BrokerOrderEventProcessor as BrokerOrderEvent.
     */
    void startOrderUpdates();

    /** Stop receiving order-update events. */
    void stopOrderUpdates();

    /**
     * Reconcile this broker's orders/positions against our local state.
     * Called periodically by BrokerReconciliationService as a safety net.
     */
    void reconcileOrders();
}
