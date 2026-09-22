package com.satyam.trading2.broker.model;

/**
 * Broker-neutral order types.
 * Each Broker implementation maps these to its own API-specific values
 * (e.g. Kite: "LIMIT", "MARKET", "SL", "SL-M").
 */
public enum OrderType {
    MARKET,
    LIMIT,
    STOP_LOSS,       // SL — triggers, then LIMIT order
    STOP_LOSS_MARKET // SL-M — triggers, then MARKET order
}
