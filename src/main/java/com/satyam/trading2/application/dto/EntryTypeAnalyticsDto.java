package com.satyam.trading2.application.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.satyam.trading2.service.EntryTypeAnalyticsService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * DTO for broadcasting entry type analytics to dashboard
 * Shows P&L, order counts, and win rates by entry type
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntryTypeAnalyticsDto {
    
    // Message type - "ENTRY_TYPE_ANALYTICS"
    private String type;
    
    // Analytics by entry type (TOP_GAINER, TOP_LOSER, ACCUMULATION)
    private Map<String, EntryTypeAnalyticsService.EntryTypeStats> analytics;
    
    /**
     * Create from analytics summary
     */
    public static EntryTypeAnalyticsDto from(Map<String, EntryTypeAnalyticsService.EntryTypeStats> summary) {
        return EntryTypeAnalyticsDto.builder()
                .type("ENTRY_TYPE_ANALYTICS")
                .analytics(summary)
                .build();
    }
}

