package com.satyam.trading2.datamodel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a single filter condition for stock selection
 * Example: gapPercent > -5.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FilterCondition {
    
    /**
     * Field name from TradeEntryMetrics (e.g., "gapPercent", "volumeRatio", "relativeStrength")
     */
    private String field;
    
    /**
     * Operator: ">", ">=", "<", "<=", "==", "!="
     */
    private String operator;
    
    /**
     * Value to compare against
     */
    private Double value;
    
    /**
     * For boolean fields like "aboveEma20", "twoGreenCandles"
     */
    private Boolean booleanValue;
    
    /**
     * Evaluate this condition against actual metrics data
     * @param metrics The metrics containing the field values
     * @return true if condition matches, false otherwise
     */
    public boolean evaluate(TradeEntryMetrics metrics) {
        try {
            // Handle boolean fields
            if (isBooleanField(field)) {
                Boolean actualValue = getBooleanFieldValue(metrics, field);
                if (actualValue == null) return false;
                return booleanValue != null && actualValue.equals(booleanValue);
            }
            
            // Handle numeric fields
            Double actualValue = getNumericFieldValue(metrics, field);
            if (actualValue == null || value == null) return false;

            switch (operator) {
                case ">":
                    return actualValue > value;
                case ">=":
                    return actualValue >= value;
                case "<":
                    return actualValue < value;
                case "<=":
                    return actualValue <= value;
                case "==":
                    return Math.abs(actualValue - value) < 0.0001; // Float equality
                case "!=":
                    return Math.abs(actualValue - value) >= 0.0001;
                default:
                    return false;
            }
        } catch (Exception e) {
            System.err.println("❌ Error evaluating filter condition: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Check if a field is boolean type
     */
    private boolean isBooleanField(String fieldName) {
        return fieldName.equals("aboveEma20") || 
               fieldName.equals("aboveEma50") || 
               fieldName.equals("twoGreenCandles") || 
               fieldName.equals("buyAboveOpeningRangeHigh") || 
               fieldName.equals("buyAboveYesterdayHigh") ||
               fieldName.equals("atrTargetTouched");
    }
    
    /**
     * Get boolean field value from metrics
     */
    private Boolean getBooleanFieldValue(TradeEntryMetrics metrics, String fieldName) {
        switch (fieldName) {
            case "aboveEma20":
                return metrics.isAboveEma20();
            case "aboveEma50":
                return metrics.isAboveEma50();
            case "twoGreenCandles":
                return metrics.isTwoGreenCandles();
            case "buyAboveOpeningRangeHigh":
                return metrics.isBuyAboveOpeningRangeHigh();
            case "buyAboveYesterdayHigh":
                return metrics.isBuyAboveYesterdayHigh();
            case "atrTargetTouched":
                return metrics.isAtrTargetTouched();
            default:
                return null;
        }
    }
    
    /**
     * Get numeric field value from metrics
     */
    private Double getNumericFieldValue(TradeEntryMetrics metrics, String fieldName) {
        switch (fieldName) {
            case "gapPercent":
                return metrics.getGapPercent();
            case "distanceFromOpenPrice":
                return metrics.getDistanceFromOpenPrice();
            case "distanceFromVwap":
                return metrics.getDistanceFromVwap();
            case "volumeRatio":
                return metrics.getVolumeRatio();
            case "stock5DayReturn":
                return metrics.getStock5DayReturn();
            case "nifty5DayReturn":
                return metrics.getNifty5DayReturn();
            case "relativeStrength":
                return metrics.getRelativeStrength();
            case "distanceFromDayLow":
                return metrics.getDistanceFromDayLow();
            case "atrPercent":
                return metrics.getAtrPercent();
            case "ema20":
                return metrics.getEma20();
            case "ema50":
                return metrics.getEma50();
            case "currentVolume":
                return metrics.getCurrentVolume();
            case "averageVolume20":
                return metrics.getAverageVolume20();
            case "buyPrice":
                return metrics.getBuyPrice();
            case "previousClose":
                return metrics.getPreviousClose();
            case "openPrice":
                return metrics.getOpenPrice();
            case "vwap":
                return metrics.getVwap();
            case "dayLow":
                return metrics.getDayLow();
            case "atr":
                return metrics.getAtr();
            case "openingRangeHigh":
                return metrics.getOpeningRangeHigh();
            case "openingRangeLow":
                return metrics.getOpeningRangeLow();
            case "yesterdayHigh":
                return metrics.getYesterdayHigh();
            case "atrTarget":
                return metrics.getAtrTarget();
            default:
                return null;
        }
    }
}

