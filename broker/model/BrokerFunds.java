package com.satyam.trading2.broker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Broker-neutral funds/margin snapshot.
 * Each Broker implementation maps its own funds response (e.g. Kite's
 * com.zerodhatech.models.Margin) into this shape.
 *
 * Per Decision #3 (RiskManager capital tracking is per-broker), each broker
 * reports its own available margin so RiskManager can enforce per-broker limits.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BrokerFunds {

    /** Which broker these funds belong to. */
    private String brokerName;

    /** Total available margin/cash usable for trading. */
    private double availableMargin;

    /** Total margin already used by open positions. */
    private double usedMargin;

    /** Free cash (available - used), i.e. what can be deployed right now. */
    private double freeCash;
}
