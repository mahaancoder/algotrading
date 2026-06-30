package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.Candle;
import com.satyam.trading2.datamodel.FilterCondition;
import com.satyam.trading2.datamodel.FilterConfig;
import com.satyam.trading2.datamodel.TradeEntryMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.satyam.trading2.service.MarketContextBuilder.openingRangeMap;

/**
 * Service to manage and apply dynamic stock filters
 * Filters are based on metrics collected in TradeEntryMetrics
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StockFilterService {
    
    private final CandleAggregator candleAggregator;
    private final MarketContextBuilder marketContextBuilder;
    private final EMAService emaService;
    private final RelativeStrengthService relativeStrengthService;
    
    // Current filter configuration
    private FilterConfig currentConfig = FilterConfig.builder()
            .enabled(false)  // Disabled by default
            .build();
    
    /**
     * Update the filter configuration
     */
    public synchronized void updateFilterConfig(FilterConfig config) {
        this.currentConfig = config;
        log.info("📊 Filter configuration updated: {} conditions, enabled={}, combineOperator={}, applyTo={}", 
                 config.getConditions().size(), config.isEnabled(), config.getCombineOperator(), config.getApplyTo());
    }
    
    /**
     * Get current filter configuration
     */
    public synchronized FilterConfig getFilterConfig() {
        return this.currentConfig;
    }
    
    /**
     * Clear all filters (disable filtering)
     */
    public synchronized void clearFilters() {
        this.currentConfig = FilterConfig.builder().enabled(false).build();
        log.info("🗑️ All filters cleared");
    }
    
    /**
     * Check if a stock passes the current filter criteria for gainers
     * This is called during stock selection for gainers
     * 
     * @param symbol The stock symbol
     * @param ltp Current price
     * @param changePercent Percentage change from previous close
     * @return true if stock passes filter, false otherwise
     */
    public boolean passesGainerFilter(String symbol, double ltp, double changePercent) {
        if (!currentConfig.shouldApplyToGainers()) {
            return true; // Filters not enabled for gainers
        }
        
        // Build real-time metrics for this stock
        TradeEntryMetrics metrics = buildRealTimeMetrics(symbol, ltp, changePercent);
        if (metrics == null) {
            log.debug("⚠️ Could not build metrics for {} - excluding from filter", symbol);
            return false; // If we can't build metrics, exclude the stock
        }
        
        boolean passes = currentConfig.evaluate(metrics);
        
        if (!passes) {
            log.debug("🔻 {} filtered out (gainer) - did not meet filter criteria", symbol);
        }
        
        return passes;
    }
    
    /**
     * Check if a stock passes the current filter criteria for losers
     * This is called during stock selection for losers
     * 
     * @param symbol The stock symbol
     * @param ltp Current price
     * @param changePercent Percentage change from previous close
     * @return true if stock passes filter, false otherwise
     */
    public boolean passesLoserFilter(String symbol, double ltp, double changePercent) {
        if (!currentConfig.shouldApplyToLosers()) {
            return true; // Filters not enabled for losers
        }
        
        // Build real-time metrics for this stock
        TradeEntryMetrics metrics = buildRealTimeMetrics(symbol, ltp, changePercent);
        if (metrics == null) {
            log.debug("⚠️ Could not build metrics for {} - excluding from filter", symbol);
            return false; // If we can't build metrics, exclude the stock
        }
        
        boolean passes = currentConfig.evaluate(metrics);
        
        if (!passes) {
            log.debug("🔺 {} filtered out (loser) - did not meet filter criteria", symbol);
        }
        
        return passes;
    }
    
    /**
     * Build real-time metrics for a stock
     * This mimics what TradeMetricsCollector does, but for filtering purposes
     */
    private TradeEntryMetrics buildRealTimeMetrics(String symbol, double currentPrice, double changePercent) {
        try {
            // Get previous close
            double previousClose = marketContextBuilder.getPreviousClose(symbol);
            if (previousClose <= 0) return null;
            
            // Get yesterday's high
            double yesterdayHigh = marketContextBuilder.getYesterdayHigh(symbol);
            
            // Get candles
            Candle latestCandle = candleAggregator.getLatestCandle(symbol);
            List<Candle> candles = candleAggregator.getCandles(symbol);
            if (latestCandle == null || candles == null || candles.isEmpty()) return null;
            
            // Get open price (first candle's open or previous close)
            double openPrice = candles.get(0).getOpen();
            if (openPrice <= 0) openPrice = previousClose;

            // Get VWAP from latest tick data
            double vwap = com.satyam.trading2.helpers.LatestVwapHelper.getLatestVwap(symbol);
            
            // Get EMA data
            EMAService.EMAData emaData = emaService.getEMAData(symbol);
            double ema20 = emaData != null ? emaData.ema20 : 0;
            double ema50 = emaData != null ? emaData.ema50 : 0;
            
            // Volume metrics
            double currentVolume = latestCandle.getVolume();
            double averageVolume20 = calculateAverageVolume(candles);
            double volumeRatio = averageVolume20 > 0 ? currentVolume / averageVolume20 : 0;
            
            // Relative strength
            double stock5DayReturn = relativeStrengthService.getStockFiveDayReturn(symbol);
            double nifty5DayReturn = relativeStrengthService.getNiftyFiveDayReturn();
            
            // Recovery indicators
            boolean twoGreenCandles = candleAggregator.hasLastTwoCandlesGreen(symbol);
            double dayLow = getDayLow(candles, latestCandle);
            
            // ATR
            double atr = marketContextBuilder.getATR(symbol);
            
            // Opening range
            MarketContextBuilder.OpeningRange openingRange = openingRangeMap.getOrDefault(symbol, new MarketContextBuilder.OpeningRange());
            
            // Build metrics object (simplified - only fields needed for filtering)
            return TradeEntryMetrics.builder()
                    .symbol(symbol)
                    .buyPrice(currentPrice)
                    .previousClose(previousClose)
                    .openPrice(openPrice)
                    .gapPercent(calculatePercentDiff(openPrice, previousClose))
                    .distanceFromOpenPrice(calculatePercentDiff(currentPrice, openPrice))
                    .vwap(vwap)
                    .distanceFromVwap(calculatePercentDiff(currentPrice, vwap))
                    .ema20(ema20)
                    .ema50(ema50)
                    .aboveEma20(currentPrice > ema20)
                    .aboveEma50(currentPrice > ema50)
                    .currentVolume(currentVolume)
                    .averageVolume20(averageVolume20)
                    .volumeRatio(volumeRatio)
                    .stock5DayReturn(stock5DayReturn)
                    .nifty5DayReturn(nifty5DayReturn)
                    .relativeStrength(stock5DayReturn - nifty5DayReturn)
                    .twoGreenCandles(twoGreenCandles)
                    .dayLow(dayLow)
                    .distanceFromDayLow(calculatePercentDiff(currentPrice, dayLow))
                    .atr(atr)
                    .atrPercent(currentPrice > 0 ? (atr / currentPrice) * 100 : 0)
                    .openingRangeHigh(openingRange.high)
                    .openingRangeLow(openingRange.low)
                    .buyAboveOpeningRangeHigh(currentPrice > openingRange.high)
                    .yesterdayHigh(yesterdayHigh)
                    .buyAboveYesterdayHigh(currentPrice > yesterdayHigh)
                    .build();
                    
        } catch (Exception e) {
            log.error("❌ Error building real-time metrics for {}: {}", symbol, e.getMessage());
            return null;
        }
    }
    
    /**
     * Calculate average volume from last 20 candles
     */
    private double calculateAverageVolume(List<Candle> candles) {
        if (candles.size() < 2) return 0;
        
        int start = Math.max(0, candles.size() - 20);
        double sum = 0;
        int count = 0;
        
        for (int i = start; i < candles.size(); i++) {
            sum += candles.get(i).getVolume();
            count++;
        }
        
        return count > 0 ? sum / count : 0;
    }
    
    /**
     * Get day's low price from candles
     */
    private double getDayLow(List<Candle> candles, Candle latestCandle) {
        double low = latestCandle.getLow();
        for (Candle c : candles) {
            if (c.getLow() < low) {
                low = c.getLow();
            }
        }
        return low;
    }
    
    /**
     * Calculate percentage difference
     */
    private double calculatePercentDiff(double current, double reference) {
        if (reference <= 0) return 0;
        return ((current - reference) / reference) * 100;
    }
}

