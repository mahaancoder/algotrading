package com.satyam.trading2.datamodel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for stock selection filters
 * Contains multiple filter conditions combined with AND/OR logic
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FilterConfig {
    
    /**
     * List of filter conditions
     */
    @Builder.Default
    private List<FilterCondition> conditions = new ArrayList<>();
    
    /**
     * Operator to combine conditions: "AND" or "OR"
     * - AND: All conditions must be true
     * - OR: At least one condition must be true
     */
    @Builder.Default
    private String combineOperator = "AND"; // Default to AND
    
    /**
     * Whether filters are enabled (default: false, so no filters applied initially)
     */
    @Builder.Default
    private boolean enabled = false;
    
    /**
     * Apply to gainers, losers, or both
     * Values: "GAINERS", "LOSERS", "BOTH"
     */
    @Builder.Default
    private String applyTo = "BOTH";
    
    /**
     * Evaluate if the given metrics pass all filter conditions
     * @param metrics The stock metrics to evaluate
     * @return true if stock passes the filter, false otherwise
     */
    public boolean evaluate(TradeEntryMetrics metrics) {
        // If filters are disabled, all stocks pass
        if (!enabled || conditions.isEmpty()) {
            return true;
        }
        
        // Apply combine operator
        if ("OR".equalsIgnoreCase(combineOperator)) {
            // OR: At least one condition must be true
            return conditions.stream().anyMatch(condition -> condition.evaluate(metrics));
        } else {
            // AND: All conditions must be true
            return conditions.stream().allMatch(condition -> condition.evaluate(metrics));
        }
    }
    
    /**
     * Check if filters should be applied to gainers
     */
    public boolean shouldApplyToGainers() {
        return enabled && ("GAINERS".equalsIgnoreCase(applyTo) || "BOTH".equalsIgnoreCase(applyTo));
    }
    
    /**
     * Check if filters should be applied to losers
     */
    public boolean shouldApplyToLosers() {
        return enabled && ("LOSERS".equalsIgnoreCase(applyTo) || "BOTH".equalsIgnoreCase(applyTo));
    }
}

