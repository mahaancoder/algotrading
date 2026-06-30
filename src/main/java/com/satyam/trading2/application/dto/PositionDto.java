package com.satyam.trading2.application.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.satyam.trading2.datamodel.Position;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for position updates broadcast via WebSocket
 * Used for both INTRADAY positions and HOLDINGS
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PositionDto {
    
    // Message type - "POSITION" for intraday, "HOLDING" for holdings
    private String type;
    
    // Trading symbol
    private String symbol;
    
    // Entry price
    private Double entry;
    
    // Last traded price
    private Double last;
    
    // Quantity
    private Integer qty;
    
    // Unrealized P&L (for POSITION type)
    private Long unrealized;
    
    // Realized P&L percentage (for HOLDING type)
    private Double pnl;
    
    // Strategy name
    private String strategy;

    // Target price
    private Double target;

    // Analytics: Entry type (TOP_GAINER, TOP_LOSER, ACCUMULATION)
    private String entryType;
    
    /**
     * Create from Position entity for INTRADAY
     */
    public static PositionDto fromIntradayPosition(Position position, double currentPrice) {
        long unrealizedPnL = Math.round((currentPrice - position.getAveragePrice()) * position.getTotalQuantity());

        return PositionDto.builder()
                .type(position.getPositionType().name())
                .symbol(position.getSymbol())
                .entry(position.getAveragePrice())
                .last(currentPrice)
                .qty(position.getTotalQuantity())
                .unrealized(unrealizedPnL)
                .strategy(position.getStrategy())
                .target(position.getTarget())
                .entryType(position.getEntryType() != null ? position.getEntryType().name() : null)
                .build();
    }

    /**
     * Create from Position entity for HOLDING
     */
    public static PositionDto fromHolding(Position position, double currentPrice) {
        double pnl = (currentPrice - position.getAveragePrice())* position.getTotalQuantity();

        return PositionDto.builder()
                .type(position.getPositionType().name())
                .symbol(position.getSymbol())
                .entry(position.getAveragePrice())
                .last(currentPrice)
                .qty(position.getTotalQuantity())
                .pnl(pnl)
                .strategy(position.getStrategy())
                .target(position.getTarget())
                .entryType(position.getEntryType() != null ? position.getEntryType().name() : null)
                .build();
    }
}

