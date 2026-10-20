package com.satyam.trading2.broker;

import com.satyam.trading2.broker.model.BrokerFunds;
import com.satyam.trading2.broker.model.BrokerOrderResponse;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.broker.model.OrderRequest;

/**
 * Common abstraction for all execution brokers (Kite, INDstocks, Shoonya).
 *
 * A null inventory result means the broker could not authoritatively confirm
 * the quantity. Callers must fail closed instead of interpreting it as zero
 * or falling back to local position state.
 */
public interface Broker {
    String getBrokerName();
    ExecutionRoute getRoute();
    BrokerOrderResponse placeOrder(OrderRequest request);
    void cancelOrder(String brokerOrderId);
    void modifyOrder(String brokerOrderId, OrderRequest request);
    BrokerOrderStatus getOrderStatus(String brokerOrderId);

    /** Cumulative filled quantity, or null when not authoritatively available. */
    default Integer getOrderFilledQuantity(String brokerOrderId) {
        return null;
    }

    /**
     * Fresh broker-confirmed sellable quantity for the given symbol and product.
     * Return 0 only when a successful broker response confirms no sellable
     * inventory. Return null on authentication, transport, API, or parse failure.
     *
     * Implementations must not include unsettled/blocked holding quantities
     * unless the broker API explicitly reports them as sellable.
     */
    default Integer getNetSellableQuantity(String symbol, boolean holding) {
        return null;
    }

    BrokerFunds getFunds();
    void startOrderUpdates();
    void stopOrderUpdates();
    void reconcileOrders();
}