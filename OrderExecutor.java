package com.satyam.trading2.domain.service;

import com.satyam.trading2.datamodel.OrderResponse;

/**
 * OrderExecutor abstraction for placing and managing orders
 * Allows switching between paper trading and live trading implementations
 */
public interface OrderExecutor {
    
    /**
     * Place a BUY order
     * 
     * @param symbol Trading symbol
     * @param qty Quantity to buy
     * @param price Price (0 for market order)
     * @param isHolding True if buying for holding (CNC), false for intraday (MIS)
     * @return OrderResponse with order IDs
     */
    OrderResponse placeBuyOrder(String symbol, int qty, double price, boolean isHolding, String strategy);
    
    /**
     * Place a SELL order
     * 
     * @param symbol Trading symbol
     * @param qty Quantity to sell
     * @param price Price (0 for market order)
     * @param isHolding True if selling holding (CNC), false for intraday (MIS)
     */
    String placeSellOrder(String symbol, int qty, double price, boolean isHolding, String strategy);
    
    /**
     * Place or update a target order
     * 
     * @param symbol Trading symbol
     * @param targetPrice Target price
     * @param qty Quantity
     * @param oldOrderId Previous target order ID (to cancel), null if new
     * @param isHolding True for holdings (CNC), false for intraday (MIS)
     * @return New target order ID
     */
    String placeTargetOrder(String symbol, double targetPrice, int qty,
                           String oldOrderId, boolean isHolding, String strategy);
    
    /**
     * Cancel an order
     * 
     * @param orderId Order ID to cancel
     */
    void cancelOrder(String orderId);
    
    /**
     * Exit a position (market sell)
     * 
     * @param symbol Trading symbol
     * @param qty Quantity to exit
     * @param isHolding True for holdings, false for intraday
     */
    String exitPosition(String symbol, int qty, boolean isHolding);
}

