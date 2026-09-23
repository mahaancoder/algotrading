package com.satyam.trading2.service;

import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.HistoricalData;
import com.satyam.trading2.config.KiteConfig;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.satyam.trading2.datamodel.Nifty500Stocks.instrumentTokenMap;

/**
 * Service to calculate Exponential Moving Averages (EMA) for stocks.
 * Fetches historical daily candles from Kite API and calculates 20 EMA and 50 EMA.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EMAService {

    private final KiteConfig kiteConfig;

    // Cache: symbol -> EMAData (20 EMA, 50 EMA, current price)
    private final Map<String, EMAData> emaCache = new ConcurrentHashMap<>();

    // Counter for logging errors
    private int totalErrorsLogged = 0;

    // Map: tradingSymbol -> instrumentToken (required for historical API)

    
    /**
     * EMA data for a stock
     */
    @Data
    public static class EMAData {
        public double ema20;
        public double ema50;
        public double currentPrice;
        public long lastUpdated;
        
        public EMAData(double ema20, double ema50, double currentPrice) {
            this.ema20 = ema20;
            this.ema50 = ema50;
            this.currentPrice = currentPrice;
            this.lastUpdated = System.currentTimeMillis();
        }
    }
    
    /**
     * Initialize instrument token map (called once at startup)
     */


    /**
     * Initialize EMA data on startup (eager initialization)
     * This ensures EMA data is available before trades execute
     */
    @PostConstruct
    public void init() {
        log.info("🚀 Initializing EMAService...");
        // Wait 30 seconds for instrument tokens to be populated
        new Thread(() -> {
            try {
                Thread.sleep(30000);
                refreshEMAData();  // ✅ Eager initialization - populate cache before trading starts
            } catch (InterruptedException e) {
                log.error("Failed to initialize EMAService: {}", e.getMessage());
            }
        }).start();
    }

    /**
     * Refresh EMA data for all stocks
     * Runs every 5 minutes during market hours
     */
    @Scheduled(fixedDelay = 300000, initialDelay = 60000) // 5 minutes, 60 seconds initial delay (after @PostConstruct)
    public void refreshEMAData() {
        int successCount = 0;
        int failureCount = 0;
        
        for (Map.Entry<String, String> entry : instrumentTokenMap.entrySet()) {
            String symbol = entry.getKey();
            String token = entry.getValue();
            
            try {
                EMAData emaData = calculateEMA(symbol, token);
                if (emaData != null) {
                    emaCache.put(symbol, emaData);
                    successCount++;
                }
            } catch (Exception e) {
                failureCount++;
                if (failureCount <= 5) { // Log only first 5 failures to avoid spam
                    log.debug("Failed to calculate EMA for {}: {}", symbol, e.getMessage());
                }
            }
        }
        
        log.info("✅ EMA refresh complete: {} success, {} failures", successCount, failureCount);
    }
    
    /**
     * Calculate 20 EMA and 50 EMA for a stock
     */
    private EMAData calculateEMA(String symbol, String instrumentToken) {
        try {
            // Create KiteConnect instance
            KiteConnect kiteConnect = new KiteConnect(kiteConfig.getApiKey());
            kiteConnect.setAccessToken(kiteConfig.getAccessToken());
            
            // Fetch last 60 days of daily candles (enough for 50 EMA calculation)
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date toDate = new Date();
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DAY_OF_MONTH, -60);
            Date fromDate = cal.getTime();

            // Get historical data - using 'day' interval for daily EMA calculation
            HistoricalData historicalData = kiteConnect.getHistoricalData(
                    fromDate,
                    toDate,
                    instrumentToken,
                    "day",
                    false,
                    false
            );
            
            if (historicalData == null || historicalData.dataArrayList == null || 
                historicalData.dataArrayList.size() < 50) {
                return null;
            }
            
            List<HistoricalData> candles = historicalData.dataArrayList;
            
            // Extract close prices
            List<Double> closePrices = new ArrayList<>();
            for (HistoricalData candle : candles) {
                closePrices.add(candle.close);
            }
            
            // Calculate EMAs
            double ema20 = calculateEMAValue(closePrices, 20);
            double ema50 = calculateEMAValue(closePrices, 50);
            double currentPrice = closePrices.get(closePrices.size() - 1);
            
            return new EMAData(ema20, ema50, currentPrice);
            
        } catch (KiteException | IOException e) {
            // Log first few errors to help debug issues
            if (totalErrorsLogged < 5) {
                log.warn("Failed to calculate EMA for {}: {}", symbol, e.getMessage());
                totalErrorsLogged++;
            }
            return null;
        }
    }
    
    /**
     * Calculate EMA value from price list
     * Formula: EMA = Price(t) * k + EMA(y) * (1 - k)
     * where k = 2 / (N + 1)
     */
    private double calculateEMAValue(List<Double> prices, int period) {
        if (prices.size() < period) {
            return 0;
        }
        
        // Calculate multiplier
        double multiplier = 2.0 / (period + 1);
        
        // Start with SMA (Simple Moving Average) for first EMA value
        double sma = 0;
        for (int i = 0; i < period; i++) {
            sma += prices.get(i);
        }
        sma = sma / period;
        
        // Calculate EMA from SMA
        double ema = sma;
        for (int i = period; i < prices.size(); i++) {
            ema = (prices.get(i) * multiplier) + (ema * (1 - multiplier));
        }
        
        return ema;
    }
    
    /**
     * Check if stock meets EMA criteria:
     * - Price > 20 EMA
     * - 20 EMA > 50 EMA
     */
    public boolean meetsEMACriteria(String symbol) {
        EMAData data = emaCache.get(symbol);
        
        if (data == null) {
            return false; // No EMA data available, exclude stock
        }
        
        // Check conditions
        boolean priceAbove20EMA = data.currentPrice > data.ema20;
        boolean ema20Above50EMA = data.ema20 > data.ema50;
        
        return priceAbove20EMA && ema20Above50EMA;
    }
    
    /**
     * Get EMA data for a stock (for debugging/logging)
     */
    public EMAData getEMAData(String symbol) {
        return emaCache.get(symbol);
    }
    
    /**
     * Get count of stocks that meet EMA criteria
     */
    public int getEMAQualifiedCount() {
        return (int) emaCache.entrySet().stream()
                .filter(e -> {
                    EMAData data = e.getValue();
                    return data.currentPrice > data.ema20 && data.ema20 > data.ema50;
                })
                .count();
    }
    
    /**
     * Clear cache (for testing)
     */
    public void clearCache() {
        emaCache.clear();
    }
}

