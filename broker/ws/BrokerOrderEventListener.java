package com.satyam.trading2.broker.ws;

import com.satyam.trading2.broker.model.BrokerOrderEvent;

/**
 * Callback interface for receiving normalized BrokerOrderEvents
 * from a broker's WebSocket order-update feed.
 *
 * Implementations (typically the Broker itself or BrokerOrderEventProcessor
 * via the Broker) call process() on each event.
 */
@FunctionalInterface
public interface BrokerOrderEventListener {
    void onOrderEvent(BrokerOrderEvent event);
}
