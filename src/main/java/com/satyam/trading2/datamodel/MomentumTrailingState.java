package com.satyam.trading2.datamodel;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Durable, broker-independent state for one momentum position's trailing exit.
 * The broker order ID is also mirrored to Position.stopLossOrderId for the
 * existing broker event/reconciliation lifecycle.
 */
@Data
@NoArgsConstructor
public class MomentumTrailingState {
    private String positionKey;
    private String tradingDate;
    private String phase = "INACTIVE";
    private String exitOrderId;
    private boolean activated;
    private boolean exitTriggered;
    private boolean eodExitRequested;
    private boolean exitOrderIsStopLoss;
    private boolean orderResizeRequired;
    private double averageBuyPrice;
    private int trackedQuantity;
    private double activationPrice;
    private double highWaterMark;
    private double desiredTriggerPrice;
    private double desiredLimitPrice;
    private double workingTriggerPrice;
    private double workingLimitPrice;
    private double estimatedNetBreakEvenPrice;
    private long lastOrderActionAt;
    private long lastAlertAt;
}
