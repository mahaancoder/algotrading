package com.satyam.trading2.domain.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PnLCalculator handles all profit/loss calculations and tracking:
 * - Trade P&L calculations
 * - Total and daily P&L tracking
 * - Drawdown calculations
 * - Strategy-wise P&L tracking
 */
@Slf4j
@Service
@Getter
public class PnLCalculator {
    
    // Total cumulative P&L across all time
    private double totalPnL = 0;
    
    // P&L for today only
    private double todaysPnL = 0;
    
    // Peak P&L achieved (for drawdown calculation)
    private double peakPnL = 0;
    
    // Maximum drawdown from peak
    private double maxDrawdown = 0;
    
    // P&L per strategy
    private final Map<String, Double> strategyPnL = new ConcurrentHashMap<>();
    
    /**
     * Calculate P&L for a trade
     */
    public long calculateTradePnL(double entryPrice, double exitPrice, int quantity) {
        return Math.round((exitPrice - entryPrice) * quantity);
    }
    
    /**
     * Record a completed trade and update all P&L metrics
     */
    public void recordTrade(double pnl, String strategyName) {
        // Update totals
        totalPnL += pnl;
        todaysPnL += pnl;
        
        // Update strategy P&L
        strategyPnL.merge(strategyName, pnl, Double::sum);
        
        // Update drawdown
        updateDrawdown();
        
        log.info("📊 Trade recorded: PnL={}, Total={}, Today={}, Strategy={} ({})", 
                pnl, totalPnL, todaysPnL, strategyName, strategyPnL.get(strategyName));
    }
    
    /**
     * Update drawdown metrics
     * Drawdown = Peak - Current
     */
    private void updateDrawdown() {
        // Update peak if we've reached a new high
        if (totalPnL > peakPnL) {
            peakPnL = totalPnL;
        }
        
        // Calculate current drawdown
        double currentDrawdown = peakPnL - totalPnL;
        
        // Update max drawdown if this is worse
        if (currentDrawdown > maxDrawdown) {
            maxDrawdown = currentDrawdown;
            log.warn("⚠️ New max drawdown: {}", maxDrawdown);
        }
    }
    
    /**
     * Get P&L for a specific strategy
     */
    public Double getStrategyPnL(String strategyName) {
        return strategyPnL.getOrDefault(strategyName, 0.0);
    }
    
    /**
     * Get all strategy P&Ls
     */
    public Map<String, Double> getAllStrategyPnLs() {
        return new ConcurrentHashMap<>(strategyPnL);
    }
    
    /**
     * Calculate unrealized P&L for an open position
     */
    public long calculateUnrealizedPnL(double entryPrice, double currentPrice, int quantity) {
        return Math.round((currentPrice - entryPrice) * quantity);
    }
    
    /**
     * Calculate profit percentage
     */
    public double calculateProfitPercentage(double entryPrice, double exitPrice) {
        return ((exitPrice - entryPrice) / entryPrice) * 100.0;
    }
    
//    /**
//     * Reset today's P&L at market open (9:15 AM)
//     */
//    @Scheduled(cron = "0 15 9 * * MON-FRI")
//    public void resetTodaysPnL() {
//        log.info("🔔 Daily reset: Today's PnL was {}, resetting to 0", todaysPnL);
//        todaysPnL = 0;
//    }
    
    /**
     * Get drawdown percentage from peak
     */
    public double getDrawdownPercentage() {
        if (peakPnL == 0) return 0.0;
        return (maxDrawdown * 100.0) / peakPnL;
    }
    
    /**
     * Check if we're in profit or loss
     */
    public boolean isInProfit() {
        return totalPnL > 0;
    }
    
    /**
     * Get win/loss ratio (requires tracking wins/losses separately)
     * This is a placeholder for future enhancement
     */
    public String getPerformanceSummary() {
        return String.format(
            "Total: ₹%,d | Today: ₹%,d | Peak: ₹%,d | Max DD: ₹%,d (%.2f%%)",
            totalPnL, todaysPnL, peakPnL, maxDrawdown, getDrawdownPercentage()
        );
    }

    /**
     * Restore strategy P&L from historical trades
     * Called during application startup to rebuild strategy P&L from CSV
     */
    public void restoreStrategyPnL(String strategyName, double pnl) {
        strategyPnL.merge(strategyName, pnl, Double::sum);
        log.debug("Restored strategy P&L: {} = {}", strategyName, strategyPnL.get(strategyName));
    }

    /**
     * Restore total P&L from historical trades
     * Called during application startup
     */
    public void restoreTotalPnL(double pnl) {
        totalPnL += pnl;

        // Update peak if needed
        if (totalPnL > peakPnL) {
            peakPnL = totalPnL;
        }

        log.debug("Restored total P&L: {}, Peak: {}", totalPnL, peakPnL);
    }
}

