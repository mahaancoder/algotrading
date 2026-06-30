package com.satyam.trading2.infrastructure.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyam.trading2.application.dto.BroadcastMessage;
import com.satyam.trading2.application.dto.PositionDto;
import com.satyam.trading2.application.dto.TradeEventDto;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.websocket.WebSocketBroadcaster;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.security.interfaces.DSAPublicKey;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.satyam.trading2.datamodel.Position.PositionType.HOLDING;
import static com.satyam.trading2.helpers.LatestPriceHelper.getLatestPrice;

/**
 * BroadcastService handles all WebSocket broadcasts with type-safe DTOs
 * Replaces manual JSON string construction with proper serialization
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BroadcastService {
    
    private final WebSocketBroadcaster broadcaster;
    private final ObjectMapper objectMapper;
    private final PositionManager positionManager; private volatile long lastBroadcastTime = 0;
    private static final long BROADCAST_THROTTLE_MS = 1000;

    // Async executor for non-blocking broadcasts
    private final ExecutorService broadcastExecutor = Executors.newFixedThreadPool(
            2,
            r -> {
                Thread t = new Thread(r);
                t.setName("broadcast-worker");
                t.setDaemon(true); // Don't block JVM shutdown
                return t;
            }
    );
    
    /**
     * Broadcast a trade event (BUY/SELL)
     */
    public void broadcastTradeEvent(TradeEventDto event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            broadcaster.broadcast(json);
            log.debug("Broadcast trade event: {} {}", event.getEtype(), event.getSymbol());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize trade event", e);
        }
    }
    
    /**
     * Broadcast a position update
     */
    public void broadcastPosition(PositionDto position) {
        try {
            String json = objectMapper.writeValueAsString(position);
            broadcaster.broadcast(json);
//            log.debug("Broadcast position: {} {}", position.getType(), position.getSymbol());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize position", e);
        }
    }
    
    /**
     * Broadcast a log message
     */
    public void broadcastLog(String message) {
//        broadcastMessage(BroadcastMessage.log(message));
    }
    
    /**
     * Broadcast total P&L
     */
    public void broadcastTotalPnL(double pnl) {
        broadcastMessage(BroadcastMessage.pnl(pnl));
    }
    
    /**
     * Broadcast today's P&L
     */
    public void broadcastTodaysPnL(double pnl) {
        broadcastMessage(BroadcastMessage.todaysPnl(pnl));
    }
    
    /**
     * Broadcast strategy P&L
     */
    public void broadcastStrategyPnL(String strategy, double pnl) {
        broadcastMessage(BroadcastMessage.strategyPnl(strategy, pnl));
    }

    /**
     * Broadcast risk data
     */
    public void broadcastRisk(Map<String, Object> riskData) {
        broadcastMessage(BroadcastMessage.risk(riskData));
    }

    /**
     * Broadcast entry type analytics
     */
    public void broadcastEntryTypeAnalytics(com.satyam.trading2.application.dto.EntryTypeAnalyticsDto analytics) {
        try {
            String json = objectMapper.writeValueAsString(analytics);
            broadcaster.broadcast(json);
            log.debug("Broadcast entry type analytics");
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize entry type analytics", e);
        }
    }
    
    /**
     * Generic message broadcast
     */
    private void broadcastMessage(BroadcastMessage message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            broadcaster.broadcast(json);
            log.debug("Broadcast message: {}", message.getType());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize broadcast message", e);
        }
    }
    
    /**
     * Broadcast BUY event
     */
    public void broadcastBuy(String symbol, String strategy, double price, int qty, String entryType) {
        TradeEventDto event = TradeEventDto.createBuyEvent(symbol, strategy, price, qty, entryType);
        broadcastTradeEvent(event);
    }

    /**
     * Broadcast SELL event
     */
    public void broadcastSell(String symbol, String strategy, double entryPrice,
                             double exitPrice, int qty, double pnl, String reason, String entryType) {
        TradeEventDto event = TradeEventDto.createSellEvent(
            symbol, strategy, entryPrice, exitPrice, qty, pnl, reason, entryType
        );
        broadcastTradeEvent(event);
    }

    /**
     * Convert object to JSON string
     */
    public String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize object to JSON", e);
            return "{}";
        }
    }

    /**
     * Broadcast holding removed (when a holding is sold)
     */
    public void broadcastHoldingRemoved(String symbol, String strategy) {
        broadcastMessage(BroadcastMessage.holdingRemoved(symbol, strategy));
        log.debug("Broadcast holding removed: {} - {}", symbol, strategy);
    }

    /**
     * Broadcast kill switch status change
     */
    public void broadcastKillSwitch(boolean active) {
        broadcastMessage(BroadcastMessage.killSwitch(active));
        log.info("Broadcast kill switch status: {}", active ? "ACTIVE" : "INACTIVE");
    }

    /**
     * Broadcast product toggle status change (MIS/CNC)
     */
    public void broadcastProductToggle(String productType, boolean enabled) {
        broadcastMessage(BroadcastMessage.productToggle(productType, enabled));
        log.info("Broadcast product toggle: {} - {}", productType, enabled ? "ENABLED" : "DISABLED");
    }

    private void broadcastHoldings(String symbol, Double price) {
        Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);
        if (strategyPositions.isEmpty()) return;

        for (Position p : strategyPositions.values()) {
            if (p.getPositionType() != HOLDING || !p.isOpen()) continue;
            PositionDto dto = PositionDto.fromHolding(p, price);
            broadcastPosition(dto);
        }
    }

    private void broadcastOpenPositions(String symbol, Double price) {
        Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);
        if (strategyPositions.isEmpty()) return;

        // Broadcast positions for the updated symbol
        for (Position p : strategyPositions.values()) {
            if (!p.isOpen() || p.getPositionType() != Position.PositionType.INTRADAY) continue;

            // Use type-safe DTO
            PositionDto dto = PositionDto.fromIntradayPosition(p, price);
            broadcastPosition(dto);
        }
    }

    public void DoBroadcashOnEachTickReceiving(String symbol, Double price) {
        long now = System.currentTimeMillis();
        if (now - lastBroadcastTime >= BROADCAST_THROTTLE_MS) {
            lastBroadcastTime = now;

            // Async broadcast - doesn't block tick processing
            CompletableFuture.runAsync(() -> {
                try {
                    broadcastOpenPositions(symbol, price);
                    broadcastHoldings(symbol, price);
                } catch (Exception e) {
                    System.out.println("Error in async broadcast: " + e.getMessage());
                }
            }, broadcastExecutor);
        }
    }
}

