package com.satyam.trading2.application.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for trade events broadcast via WebSocket
 * Represents BUY/SELL events in the trade history
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TradeEventDto {
    
    // Message type - always "EVENT" for trade events
    private String type;
    
    // Event type - "BUY" or "SELL"
    private String etype;
    
    // Trading symbol
    private String symbol;
    
    // Strategy name
    private String strategy;
    
    // Entry price (for BUY events)
    private Double entry;

    // Exit price (for SELL events)
    private Double exit;

    // Price (for UI display - maps to entry for BUY, exit for SELL)
    private Double price;

    // Quantity
    private Integer qty;
    
    // P&L (for SELL events)
    private Double pnl;
    
    // Timestamp
    private Long time;

    // Additional reason/message
    private String reason;

    // Analytics: Entry type (TOP_GAINER, TOP_LOSER, ACCUMULATION)
    private String entryType;

    /**
     * Create a BUY event
     */
    public static TradeEventDto createBuyEvent(String symbol, String strategy,
                                               double buyPrice, int qty, String entryType) {
        return TradeEventDto.builder()
                .type("EVENT")
                .etype("BUY")
                .symbol(symbol)
                .strategy(strategy)
                .entry(buyPrice)
                .price(buyPrice)  // For UI display
                .qty(qty)
                .pnl(0.0)  // BUY events have 0 P&L
                .time(System.currentTimeMillis())
                .entryType(entryType)
                .build();
    }

    /**
     * Create a SELL event
     */
    public static TradeEventDto createSellEvent(String symbol, String strategy,
                                                double entryPrice, double exitPrice,
                                                int qty, double pnl, String reason, String entryType) {
        return TradeEventDto.builder()
                .type("EVENT")
                .etype("SELL")
                .symbol(symbol)
                .strategy(strategy)
                .entry(entryPrice)
                .exit(exitPrice)
                .price(exitPrice)  // For UI display
                .qty(qty)
                .pnl(pnl)
                .reason(reason)
                .time(System.currentTimeMillis())
                .entryType(entryType)
                .build();
    }
}

