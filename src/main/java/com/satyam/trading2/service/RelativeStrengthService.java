package com.satyam.trading2.service;

import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.HistoricalData;
import com.satyam.trading2.config.KiteConfig;
import com.satyam.trading2.datamodel.Nifty500Stocks;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service to calculate 5-day returns for stocks and compare with Nifty 50 return
 * Used to filter stocks that outperform the index over 5 days
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RelativeStrengthService {

    private final KiteConfig kiteConfig;
    
    // Nifty 50 instrument token (from NSE)
    private static final String NIFTY_50_TOKEN = "256265"; // Nifty 50 index token
    
    // Cache: symbol -> 5-day return percentage
    private final Map<String, Double> fiveDayReturns = new ConcurrentHashMap<>();
    
    // Nifty 50's 5-day return
    private volatile double niftyFiveDayReturn = 0.0;
    
    // Instrument token map (symbol -> token)
    private final Map<String, String> instrumentTokenMap = new ConcurrentHashMap<>();
    
    /**
     * Set instrument token for a symbol (called during startup)
     */
    public void setInstrumentToken(String symbol, String token) {
        instrumentTokenMap.put(symbol, token);
    }
    
    /**
     * Initialize on startup - calculate 5-day returns for all stocks and Nifty
     */
    @PostConstruct
    public void init() {
        log.info("🚀 Initializing RelativeStrengthService...");
        // Wait 30 seconds for instrument tokens to be populated
        new Thread(() -> {
            try {
                Thread.sleep(30000);
                refreshFiveDayReturns();
            } catch (InterruptedException e) {
                log.error("Failed to initialize RelativeStrengthService: {}", e.getMessage());
            }
        }).start();
    }
    
    /**
     * Refresh 5-day returns every 30 minutes during market hours
     */
    @Scheduled(fixedDelay = 1800000) // 30 minutes
    public void scheduledRefresh() {
        // Check if today is a weekday
        java.time.DayOfWeek today = java.time.LocalDate.now().getDayOfWeek();
        if (today == java.time.DayOfWeek.SATURDAY || today == java.time.DayOfWeek.SUNDAY) {
            return;
        }
        
        // Check if during market hours (9:15 AM - 3:30 PM)
        java.time.LocalTime now = java.time.LocalTime.now();
        if (now.isBefore(java.time.LocalTime.of(9, 15)) || now.isAfter(java.time.LocalTime.of(15, 30))) {
            return;
        }
        
        refreshFiveDayReturns();
    }
    
    /**
     * Calculate 5-day returns for all stocks and Nifty 50
     */
    public void refreshFiveDayReturns() {
        log.info("📊 Refreshing 5-day returns for {} stocks and Nifty 50...", instrumentTokenMap.size());
        
        // First, calculate Nifty 50's 5-day return
        try {
            Double niftyReturn = calculate5DayReturn(NIFTY_50_TOKEN);
            if (niftyReturn != null) {
                niftyFiveDayReturn = niftyReturn;
                log.info("📈 Nifty 50 5-day return: {:.2f}%", niftyFiveDayReturn);
            } else {
                log.warn("⚠️ Failed to calculate Nifty 50 5-day return");
            }
        } catch (Exception e) {
            log.error("❌ Error calculating Nifty 50 5-day return: {}", e.getMessage());
        }
        
        // Now calculate for all stocks
        int successCount = 0;
        int failureCount = 0;
        
        for (Map.Entry<String, String> entry : instrumentTokenMap.entrySet()) {
            String symbol = entry.getKey();
            String token = entry.getValue();
            
            try {
                Double fiveDayReturn = calculate5DayReturn(token);
                if (fiveDayReturn != null) {
                    fiveDayReturns.put(symbol, fiveDayReturn);
                    successCount++;
                }
            } catch (Exception e) {
                failureCount++;
                if (failureCount <= 5) { // Log only first 5 failures
                    log.debug("Failed to calculate 5-day return for {}: {}", symbol, e.getMessage());
                }
            }
        }
        
        log.info("✅ 5-day return refresh complete: {} success, {} failures", successCount, failureCount);
        log.info("📊 Stocks outperforming Nifty 50: {}", getOutperformingStockCount());
    }
    
    /**
     * Calculate 5-day return for a given instrument token
     * Returns percentage change from 5 days ago to current close
     */
    private Double calculate5DayReturn(String instrumentToken) {
        try {
            // Create KiteConnect instance
            KiteConnect kiteConnect = new KiteConnect(kiteConfig.getApiKey());
            kiteConnect.setAccessToken(kiteConfig.getAccessToken());
            
            // Fetch last 10 days of daily candles (to ensure we get at least 5 trading days)
            Calendar cal = Calendar.getInstance();
            Date toDate = new Date();
            cal.add(Calendar.DAY_OF_MONTH, -10);
            Date fromDate = cal.getTime();
            
            // Get historical data
            HistoricalData historicalData = kiteConnect.getHistoricalData(
                    fromDate,
                    toDate,
                    instrumentToken,
                    "day",
                    false,
                    false
            );
            
            if (historicalData == null || historicalData.dataArrayList == null || 
                historicalData.dataArrayList.size() < 6) { // Need at least 6 days (5 days + current)
                return null;
            }
            
            List<HistoricalData> candles = historicalData.dataArrayList;
            
            // Get current close (most recent day)
            double currentClose = candles.get(candles.size() - 1).close;
            
            // Get close from 5 trading days ago
            double fiveDaysAgoClose = candles.get(candles.size() - 6).close;
            
            // Calculate percentage return
            double returnPct = ((currentClose - fiveDaysAgoClose) / fiveDaysAgoClose) * 100.0;
            
            return returnPct;
            
        } catch (KiteException | IOException e) {
            // Silently ignore errors (too noisy to log for every stock)
            return null;
        }
    }
    
    /**
     * Check if a stock outperforms Nifty 50 over last 5 days
     * Returns true if stock's 5-day return > Nifty's 5-day return
     */
    public boolean outperformsNifty(String symbol) {
        Double stockReturn = fiveDayReturns.get(symbol);
        
        if (stockReturn == null) {
            return false; // No data available, exclude stock
        }
        
        // Stock must outperform Nifty
        return stockReturn > niftyFiveDayReturn;
    }
    
    /**
     * Get 5-day return for a stock (for debugging/logging)
     */
    public Double getFiveDayReturn(String symbol) {
        return fiveDayReturns.get(symbol);
    }

    /**
     * Get stock's 5-day return (returns 0 if not available)
     */
    public double getStockFiveDayReturn(String symbol) {
        return fiveDayReturns.getOrDefault(symbol, 0.0);
    }

    /**
     * Get Nifty 50's 5-day return
     */
    public double getNiftyFiveDayReturn() {
        return niftyFiveDayReturn;
    }
    
    /**
     * Get count of stocks that outperform Nifty 50
     */
    public int getOutperformingStockCount() {
        return (int) fiveDayReturns.entrySet().stream()
                .filter(e -> e.getValue() > niftyFiveDayReturn)
                .count();
    }
    
    /**
     * Clear cache (for testing)
     */
    public void clearCache() {
        fiveDayReturns.clear();
        niftyFiveDayReturn = 0.0;
    }
    
    /**
     * Data holder for 5-day return info (for logging/debugging)
     */
    public static class ReturnData {
        public final double fiveDayReturn;
        public final double niftyReturn;
        public final boolean outperforms;
        
        public ReturnData(double fiveDayReturn, double niftyReturn) {
            this.fiveDayReturn = fiveDayReturn;
            this.niftyReturn = niftyReturn;
            this.outperforms = fiveDayReturn > niftyReturn;
        }
    }
    
    /**
     * Get complete return data for a stock (for logging/debugging)
     */
    public ReturnData getReturnData(String symbol) {
        Double stockReturn = fiveDayReturns.get(symbol);
        if (stockReturn == null) {
            return null;
        }
        return new ReturnData(stockReturn, niftyFiveDayReturn);
    }
}

