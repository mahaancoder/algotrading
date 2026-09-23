package com.satyam.trading2.broker.model;

/**
 * Broker-neutral product codes.
 * Each Broker implementation maps these to its own API-specific values
 * (e.g. Kite: "MIS" intraday, "CNC" delivery, "NRML" F&O).
 */
public enum ProductType {
    INTRADAY,   // MIS — squared off same day
    DELIVERY,   // CNC — held overnight / holdings
    MARGIN      // NRML — F&O / positional margin
}
