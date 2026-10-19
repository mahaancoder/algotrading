package com.satyam.trading2.infrastructure.broker;

import com.satyam.trading2.datamodel.OrderResponse;
import com.satyam.trading2.domain.service.OrderExecutor;
import com.satyam.trading2.service.OrderServiceV2;
import com.satyam.trading2.strategy.DipAccumulatorNoRegime;
import com.satyam.trading2.strategy.MomentumDipAccumulatorStrategy;
import com.satyam.trading2.strategy.TradingStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/**
 * Live trading implementation of OrderExecutor using Zerodha Kite API
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "trading.paper.mode", havingValue = "false")
public class KiteOrderExecutor implements OrderExecutor {

    private final OrderServiceV2 orderService;
    
    @Override
    public OrderResponse placeBuyOrder(String symbol, int qty, double price, boolean isHolding, String strategy) {
        String product = isHolding ? "CNC" : "MIS";

//        log.info("🟢 LIVE BUY: {} @ {} Qty={} Product={}", symbol, price, qty, product);

        String orderId = orderService.placeOrder(symbol, "BUY", "LIMIT", qty, price, 0, product, strategy);
        System.out.println("buy order successfully placed in app : "+ orderId);
        OrderResponse response = new OrderResponse();
        response.setEntryOrderId(orderId);
        return response;
    }
    
    /**
     * Legacy interface entry point lacks a Position/broker inventory snapshot, so it
     * cannot safely reserve available long quantity. All live SELLs must go through
     * BrokerOrderRouter, which serializes the lifecycle and checks broker/account
     * inventory plus outstanding SELL reservations.
     */
    @Override
    @Deprecated
    public String placeSellOrder(String symbol, int qty, double price, boolean isHolding, String strategy) {
        log.error("🛑 Refusing legacy direct SELL for {}: Position-aware BrokerOrderRouter is required.", symbol);
        return null;
    }

    /**
     * Target replacement must also pass through BrokerOrderRouter so the old order's
     * fill/cancel status and available long quantity are checked atomically.
     */
    @Override
    @Deprecated
    public String placeTargetOrder(String symbol, double targetPrice, int qty,
                                  String oldOrderId, boolean isHolding, String strategy) {
        log.error("🛑 Refusing legacy direct target SELL for {}: BrokerOrderRouter is required.", symbol);
        return null;
    }
    
    @Override
    public void cancelOrder(String orderId) {
        orderService.cancelOrder(orderId);
    }
    
    @Override
    @Deprecated
    public String exitPosition(String symbol, int qty, boolean isHolding) {
        log.error("🛑 Refusing legacy direct exit for {}: a Position-aware BrokerOrderRouter is required.", symbol);
        return null;
    }
}

