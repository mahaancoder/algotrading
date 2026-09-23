package com.satyam.trading2.helpers;

import com.satyam.trading2.datamodel.Instrument;
import com.satyam.trading2.datamodel.Nifty500Stocks;

/**
 * Utility class to check if target prices would hit circuit limits
 * Used to prevent placing orders that would definitely get rejected
 */
public class CircuitLimitChecker {

    /**
     * Check if the target price would be within circuit limits
     * Calculates expected target price and compares with upper circuit limit
     *
     * NOTE: This method is DEPRECATED - use isSpecificTargetWithinCircuitLimits instead
     * This still uses fixed % targets (0.3% for holdings, 1% for intraday)
     *
     * @param symbol Trading symbol
     * @param entryPrice Entry price for the position
     * @param isHolding True for holdings (0.3% target), false for intraday (1% target)
     * @return true if target price is safe, false if it would hit upper circuit
     */
    public static boolean isTargetPriceWithinCircuitLimits(String symbol, double entryPrice, boolean isHolding) {
        try {
            // Get instrument data with circuit limits
            Instrument instrument = Nifty500Stocks.Nifty500SymbolToInstrument.get(symbol);
            if (instrument == null) {
                System.out.println("⚠️ [CircuitCheck] Instrument not found for " + symbol + " - allowing trade");
                return true; // Allow trade if instrument data not available
            }

            Double upperCircuit = instrument.getUpperCircuitLimit();
            if (upperCircuit == null) {
                System.out.println("⚠️ [CircuitCheck] Upper circuit not available for " + symbol + " - allowing trade");
                return true; // Allow trade if circuit data not available
            }

            // Calculate expected target price
            double targetPrice;
            if (isHolding) {
                // Holdings: 0.3% profit target
                targetPrice = entryPrice * 1.003;
            } else {
                // Intraday: 1% profit target (fallback - actual is 0.5*ATR)
                targetPrice = entryPrice * 1.01;
            }
            
            // Check if target would hit or exceed upper circuit (with 0.1% safety margin)
            if (targetPrice >= upperCircuit * 0.999) {
                System.out.println("🚫 [CircuitCheck] " + symbol + " - Target ₹" + String.format("%.2f", targetPrice) + 
                                 " would hit upper circuit ₹" + String.format("%.2f", upperCircuit));
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            System.err.println("⚠️ [CircuitCheck] Error checking circuit limits for " + symbol + ": " + e.getMessage());
            return true; // Allow trade on error to avoid blocking legitimate trades
        }
    }

    /**
     * Check if a specific target price would be within upper circuit limits
     * 
     * @param symbol Trading symbol
     * @param targetPrice The target price to check
     * @return true if target price is safe, false if it would hit upper circuit
     */
    public static boolean isSpecificTargetWithinCircuitLimits(String symbol, double targetPrice) {
        try {
            // Get instrument data with circuit limits
            Instrument instrument = Nifty500Stocks.Nifty500SymbolToInstrument.get(symbol);
            if (instrument == null) {
                System.out.println("⚠️ [CircuitCheck] Instrument not found for " + symbol + " - allowing trade");
                return true; // Allow trade if instrument data not available
            }
            
            Double upperCircuit = instrument.getUpperCircuitLimit();
            if (upperCircuit == null) {
                System.out.println("⚠️ [CircuitCheck] Upper circuit not available for " + symbol + " - allowing trade");
                return true; // Allow trade if circuit data not available
            }
            
            // Check if target would hit or exceed upper circuit (with 0.1% safety margin)
            if (targetPrice >= upperCircuit * 0.999) {
                System.out.println("🚫 [CircuitCheck] " + symbol + " - Target ₹" + String.format("%.2f", targetPrice) + 
                                 " would hit upper circuit ₹" + String.format("%.2f", upperCircuit));
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            System.err.println("⚠️ [CircuitCheck] Error checking circuit limits for " + symbol + ": " + e.getMessage());
            return true; // Allow trade on error to avoid blocking legitimate trades
        }
    }
}

