package com.satyam.trading2.broker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Broker-neutral order request.
 * The strategy layer builds this; each Broker implementation converts it into
 * its own API-specific format (Kite form-params, Shoonya JSON, INDstocks JSON, etc).
 *
 * Replaces the old datamodel.OrderRequest (String-typed side/orderType/product)
 * with strongly-typed enums. Adds signalId/strategyId for end-to-end tracing.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequest {

    /** Trading symbol, e.g. "RELIANCE" */
    private String symbol;

    /** Exchange, e.g. "NSE". Defaults handled by broker if null. */
    private String exchange;

    /** Buy or Sell */
    private TransactionType transactionType;

    /** MARKET, LIMIT, STOP_LOSS, STOP_LOSS_MARKET */
    private OrderType orderType;

    /** Number of shares */
    private int quantity;

    /** Limit price (0 or ignored for MARKET orders) */
    private double price;

    /** Trigger price for SL/SL-M orders (nullable) */
    private Double triggerPrice;

    /** INTRADAY, DELIVERY, MARGIN */
    private ProductType productType;

    /** Strategy name that produced the signal (e.g. "Dip-Accumulator-Momentum") */
    private String strategyId;

    /** Unique ID of the TradeSignal that originated this order, for tracing */
    private String signalId;
}
