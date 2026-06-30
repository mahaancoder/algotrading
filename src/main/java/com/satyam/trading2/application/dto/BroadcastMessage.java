package com.satyam.trading2.application.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Generic DTO for simple broadcast messages
 * Used for logs, metrics, and other simple data
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BroadcastMessage {
    
    // Message type
    private String type;
    
    // Message content (for LOG type)
    private String message;
    
    // Numeric value (for PNL, ANALYTICS, etc.)
    private double value;
    
    // Strategy name (for STRATEGY_PNL)
    private String strategy;
    
    // P&L value (for STRATEGY_PNL)
    private double pnl;
    
    // Additional data
    private Map<String, Object> data;

    // Symbol (for HOLDING_REMOVED)
    private String symbol;

    /**
     * Create a log message
     */
    public static BroadcastMessage log(String message) {
        return BroadcastMessage.builder()
                .type("LOG")
                .message(message)
                .build();
    }
    
    /**
     * Create a P&L update
     */
    public static BroadcastMessage pnl(double value) {
        return BroadcastMessage.builder()
                .type("PNL")
                .value(value)
                .build();
    }
    
    /**
     * Create a today's P&L update
     */
    public static BroadcastMessage todaysPnl(double value) {
        return BroadcastMessage.builder()
                .type("TODAYS_PNL")
                .value(value)
                .build();
    }
    
    /**
     * Create a strategy P&L update
     */
    public static BroadcastMessage strategyPnl(String strategy, double pnl) {
        return BroadcastMessage.builder()
                .type("STRATEGY_PNL")
                .strategy(strategy)
                .pnl(pnl)
                .build();
    }
    
    /**
     * Create a risk update
     */
    public static BroadcastMessage risk(Map<String, Object> data) {
        return BroadcastMessage.builder()
                .type("RISK")
                .data(data)
                .build();
    }

    /**
     * Create a holding removed message
     */
    public static BroadcastMessage holdingRemoved(String symbol, String strategy) {
        return BroadcastMessage.builder()
                .type("HOLDING_REMOVED")
                .symbol(symbol)
                .strategy(strategy)
                .build();
    }

    /**
     * Create a kill switch status message
     */
    public static BroadcastMessage killSwitch(boolean active) {
        return BroadcastMessage.builder()
                .type("KILL_SWITCH")
                .message(active ? "active" : "inactive")
                .build();
    }

    /**
     * Create a product toggle status message (MIS/CNC)
     */
    public static BroadcastMessage productToggle(String productType, boolean enabled) {
        return BroadcastMessage.builder()
                .type("PRODUCT_TOGGLE")
                .strategy(productType)  // Reuse 'strategy' field for product type (MIS/CNC)
                .message(enabled ? "enabled" : "disabled")
                .build();
    }
}

