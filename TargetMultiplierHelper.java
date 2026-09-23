package com.satyam.trading2.helpers;

import java.time.LocalTime;

/**
 * Utility class to calculate target profit multipliers based on time of day
 * This ensures consistent target pricing across all services
 * 
 * Intraday targets start at 1% and reduce throughout the day:
 * - 09:15 - 10:15: 1.00% (original target)
 * - 10:15 - 10:30: 0.70%
 * - 10:30 - 11:00: 0.60%
 * - 11:00 - 12:00: 0.50%
 * - 12:00 - 13:00: 0.40%
 * - 13:00 - 14:00: 0.30%
 * - 14:00 - 15:00: 0.20%
 * - 15:00 - 15:30: 0.15%
 * 
 * Holdings always use 0.3% regardless of time
 */
public class TargetMultiplierHelper {

    /**
     * Get the current target multiplier for INTRADAY positions based on time
     * Returns the multiplier to use (e.g., 0.7 for 0.7% profit)
     */
    public static double getCurrentIntradayMultiplier() {
        LocalTime now = LocalTime.now();
        
        // Before 10:15 AM - use full 1% target
        if (now.isBefore(LocalTime.of(10, 15))) {
            return 1.0;
        }
        // 10:15 - 10:30 AM
        else if (now.isBefore(LocalTime.of(10, 30))) {
            return 0.7;
        }
        // 10:30 - 11:00 AM
        else if (now.isBefore(LocalTime.of(11, 0))) {
            return 0.6;
        }
        // 11:00 - 12:00 PM
        else if (now.isBefore(LocalTime.of(12, 0))) {
            return 0.5;
        }
        // 12:00 - 1:00 PM
        else if (now.isBefore(LocalTime.of(13, 0))) {
            return 0.4;
        }
        // 1:00 - 2:00 PM
        else if (now.isBefore(LocalTime.of(14, 0))) {
            return 0.3;
        }
        // 2:00 - 3:00 PM
        else if (now.isBefore(LocalTime.of(15, 0))) {
            return 0.2;
        }
        // After 3:00 PM
        else {
            return 0.15;
        }
    }
    
    /**
     * Get the current target multiplier for HOLDING positions
     * Holdings always use 0.3% regardless of time
     */
    public static double getCurrentHoldingMultiplier() {
        return 0.3;
    }
    
    /**
     * Calculate target price for a position based on current time
     * 
     * @param avgPrice Average entry price
     * @param isHolding True if this is a holding position, false for intraday
     * @return Target price
     */
    public static double calculateTargetPrice(double avgPrice, boolean isHolding) {
        double multiplier = isHolding ? getCurrentHoldingMultiplier() : getCurrentIntradayMultiplier();
        return avgPrice * (1.0 + (0.01 * multiplier));
    }
    
    /**
     * Get a human-readable description of the current time window
     */
    public static String getCurrentTimeWindow() {
        LocalTime now = LocalTime.now();
        
        if (now.isBefore(LocalTime.of(10, 15))) {
            return "09:15-10:15 (1.0%)";
        } else if (now.isBefore(LocalTime.of(10, 30))) {
            return "10:15-10:30 (0.7%)";
        } else if (now.isBefore(LocalTime.of(11, 0))) {
            return "10:30-11:00 (0.6%)";
        } else if (now.isBefore(LocalTime.of(12, 0))) {
            return "11:00-12:00 (0.5%)";
        } else if (now.isBefore(LocalTime.of(13, 0))) {
            return "12:00-13:00 (0.4%)";
        } else if (now.isBefore(LocalTime.of(14, 0))) {
            return "13:00-14:00 (0.3%)";
        } else if (now.isBefore(LocalTime.of(15, 0))) {
            return "14:00-15:00 (0.2%)";
        } else {
            return "15:00-15:30 (0.15%)";
        }
    }
}

