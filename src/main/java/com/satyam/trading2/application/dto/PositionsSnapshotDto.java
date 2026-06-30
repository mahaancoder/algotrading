package com.satyam.trading2.application.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * DTO for sending batch position snapshots via WebSocket
 * Used to send all positions in a single message instead of one-by-one
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PositionsSnapshotDto {
    
    // Message type - "POSITIONS_SNAPSHOT" or "HOLDINGS_SNAPSHOT"
    private String type;
    
    // List of positions
    private List<PositionDto> positions;
    
    /**
     * Create intraday positions snapshot
     */
    public static PositionsSnapshotDto intradaySnapshot(List<PositionDto> positions) {
        return PositionsSnapshotDto.builder()
                .type("POSITIONS_SNAPSHOT")
                .positions(positions)
                .build();
    }
    
    /**
     * Create holdings snapshot
     */
    public static PositionsSnapshotDto holdingsSnapshot(List<PositionDto> positions) {
        return PositionsSnapshotDto.builder()
                .type("HOLDINGS_SNAPSHOT")
                .positions(positions)
                .build();
    }
}

