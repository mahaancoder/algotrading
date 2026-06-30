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
    
    @Override
    public String placeSellOrder(String symbol, int qty, double price, boolean isHolding, String strategy) {
        String product = isHolding ? "CNC" : "MIS";
        
        log.info("🔴 LIVE SELL: {} @ {} Qty={} Product={}", symbol, price, qty, product);
        
        return orderService.placeOrder(symbol, "SELL", "LIMIT", qty, price, 0, product, strategy);
    }
    
    @Override
    public String placeTargetOrder(String symbol, double targetPrice, int qty, 
                                  String oldOrderId, boolean isHolding, String strategy) {
        log.info("🎯 LIVE TARGET: {} @ {} Qty={} (updating from {})", 
                symbol, targetPrice, qty, oldOrderId);
        
        return orderService.updateTargetOrder(symbol, targetPrice, qty, oldOrderId, isHolding, strategy);
    }
    
    @Override
    public void cancelOrder(String orderId) {
        orderService.cancelOrder(orderId);
    }
    
    @Override
    public String exitPosition(String symbol, int qty, boolean isHolding) {
        log.info("🚪 LIVE EXIT: {} Qty={}", symbol, qty);
       return orderService.exitPosition(symbol, qty, isHolding, "Dip-Accumulator-Momentum");
    }
}

