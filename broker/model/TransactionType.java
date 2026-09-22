package com.satyam.trading2.broker.model;

/**
 * Buy or Sell side of an order.
 * Broker-neutral replacement for the String "BUY"/"SELL" previously used in OrderServiceV2.
 */
public enum TransactionType {
    BUY,
    SELL
}
