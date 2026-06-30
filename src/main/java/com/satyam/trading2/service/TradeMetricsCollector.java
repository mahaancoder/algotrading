package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import static com.satyam.trading2.datamodel.Nifty500Stocks.Nifty500SymbolToInstrument;
import static com.satyam.trading2.service.MarketContextBuilder.openingRangeMap;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Collects comprehensive metrics for each executed trade
 * Stores data for post-trade analysis and filtering
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TradeMetricsCollector {
    
    private final CandleAggregator candleAggregator;
    private final MarketContextBuilder marketContextBuilder;
    private final EMAService emaService;
    private final RelativeStrengthService relativeStrengthService;
    
    // In-memory storage (limited to prevent memory issues)
    private static final int MAX_METRICS_IN_MEMORY = 500;
    private final List<TradeEntryMetrics> metricsHistory = new CopyOnWriteArrayList<>();

    // File persistence
    private static final String METRICS_FILE = "/home/ec2-user/trade_entry_metrics.csv";

    // Load existing metrics on service startup
    @javax.annotation.PostConstruct
    public void init() {
        loadMetricsFromFile();
    }
    
    /**
     * Collect all metrics at the time of buy execution
     */
    public TradeEntryMetrics collectMetrics(
        String symbol,
        String strategy,
        double entryPrice,
        int quantity,
        String orderId,
        EntryType entryType,
        boolean isHolding,
        MarketContext ctx
    ) {
        try {
            // Get previous close
            double previousClose = marketContextBuilder.getPreviousClose(symbol);

            // Get yesterday's high
            double yesterdayHigh = marketContextBuilder.getYesterdayHigh(symbol);

            // Get current candle for open price
            Candle latestCandle = candleAggregator.getLatestCandle(symbol);
            List<Candle> candles = candleAggregator.getCandles(symbol);

            // Calculate open price (first candle's open of the day, or use previous close)
            double openPrice = getOpenPrice(symbol, candles, previousClose);

            // Get VWAP
            double vwap = ctx.getVwap();

            // Get EMA data
            EMAService.EMAData emaData = emaService.getEMAData(symbol);
            double ema20 = emaData != null ? emaData.getEma20() : 0;
            double ema50 = emaData != null ? emaData.getEma50() : 0;

            log.info("📊 EMA Data for {}: EMA20={}, EMA50={}, EMAData={}",
                symbol, ema20, ema50, emaData != null ? "available" : "null");

            // Get volume data
            VolumeMetrics volumeMetrics = calculateVolumeMetrics(symbol, candles, latestCandle);

            // Get relative strength
            double stock5DayReturn = relativeStrengthService.getStockFiveDayReturn(symbol);
            double nifty5DayReturn = relativeStrengthService.getNiftyFiveDayReturn();

            // Get recovery indicators
            boolean twoGreenCandles = candleAggregator.hasLastTwoCandlesGreen(symbol);
            double dayLow = getDayLow(candles, latestCandle);

            // Get ATR
            double atr = marketContextBuilder.getATR(symbol);

            // Get opening range
            MarketContextBuilder.OpeningRange openingRange = openingRangeMap.getOrDefault(symbol, new MarketContextBuilder.OpeningRange());

            // Build metrics object
            TradeEntryMetrics metrics = TradeEntryMetrics.builder()
                .symbol(symbol)
                .strategy(strategy)
                .buyPrice(Math.round(entryPrice * 100.0) / 100.0 ) // rounding to 2 digits
                .sellPrice(null)
                .quantity(quantity)
                .buyTime(TradeEntryMetrics.formatCurrentTimeGMT530())
                .sellTime(null)
                .orderId(orderId)
                .status("open")

                // Gap
                .previousClose(previousClose)
                .openPrice(openPrice)
                .gapPercent(TradeEntryMetrics.percentDiff(openPrice, previousClose))
                .distanceFromOpenPrice(TradeEntryMetrics.percentDiff(entryPrice, openPrice))

                // VWAP
                .vwap(vwap)
                .distanceFromVwap(TradeEntryMetrics.percentDiff(entryPrice, vwap))

                // EMA
                .ema20(ema20)
                .ema50(ema50)
                .aboveEma20(entryPrice > ema20)
                .aboveEma50(entryPrice > ema50)

                // Volume
                .currentVolume(volumeMetrics.currentVolume)
                .averageVolume20(volumeMetrics.averageVolume20)
                .volumeRatio(volumeMetrics.volumeRatio)

                // Relative Strength
                .stock5DayReturn(stock5DayReturn)
                .nifty5DayReturn(nifty5DayReturn)
                .relativeStrength(stock5DayReturn - nifty5DayReturn)

                // Recovery
                .twoGreenCandles(twoGreenCandles)
                .dayLow(dayLow)
                .distanceFromDayLow(TradeEntryMetrics.percentDiff(entryPrice, dayLow))

                // Volatility
                .atr(atr)
                .atrPercent(entryPrice > 0 ? (atr / entryPrice) * 100 : 0)

                // Opening Range
                .openingRangeHigh(openingRange.high)
                .openingRangeLow(openingRange.low)
                .buyAboveOpeningRangeHigh(entryPrice > openingRange.high)

                // Yesterday's High
                .yesterdayHigh(yesterdayHigh)
                .buyAboveYesterdayHigh(entryPrice > yesterdayHigh)

                // Position Info
                .entryType(entryType)
                .convertedToHolding(null)  // Will be set later if position is converted

                // ATR Target Tracking
                .atrTarget((atr * 0.5) + (entryPrice * 1.001))
                .atrTargetTouched(false)  // Will be set to true if price touches atrTarget
                    .positionType(isHolding? "HOLDING" : "INTRADAY")
                .build();

            // Store in memory
            storeMetrics(metrics);

            // Append to file (efficient - no need to rewrite)
            appendMetricsToFile(metrics);

            log.info("📊 Collected trade metrics for {} @ {} - Gap: {:.2f}%, VWAP Dist: {:.2f}%, Vol Ratio: {:.2f}x",
                symbol, entryPrice, metrics.getGapPercent(), metrics.getDistanceFromVwap(), metrics.getVolumeRatio());

            return metrics;

        } catch (Exception e) {
            log.error("❌ Failed to collect metrics for {} {}: {}", symbol, strategy, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Update metrics when trade exits
     */
    public void updateExitMetrics(String symbol, String strategy, double exitPrice, double pnl, boolean convertedToHolding, Integer daysToTarget) {
        try {
            // Find the most recent metrics for this symbol+strategy
            // We search in reverse order to get the most recent trade
            TradeEntryMetrics metrics = null;
            for (int i = metricsHistory.size() - 1; i >= 0; i--) {
                TradeEntryMetrics m = metricsHistory.get(i);
                if (m.getSymbol().equals(symbol) && m.getStrategy().equals(strategy) && "open".equals(m.getStatus())) {
                    metrics = m;
                    break;
                }
            }

            if (metrics != null) {
                metrics.setSellPrice(Math.round(exitPrice * 100.0) / 100.0); // round to 2 digits
                metrics.setPnl(pnl);
                metrics.setConvertedToHolding(convertedToHolding);
                metrics.setDaysToTarget(daysToTarget);
                metrics.setSellTime(TradeEntryMetrics.formatCurrentTimeGMT530());
                metrics.setStatus("closed");

                // Rewrite entire file with updated metrics
                rewriteMetricsFile();

                log.info("📊 Updated exit metrics for {} {} - Exit: {}, PnL: {}, ConvertedToHolding: {}, Days: {}",
                    symbol, strategy, exitPrice, pnl, convertedToHolding, daysToTarget);
            } else {
                log.warn("⚠️ No open trade metrics found for {} {}", symbol, strategy);
            }
        } catch (Exception e) {
            log.error("❌ Failed to update exit metrics for {} {}: {}", symbol, strategy, e.getMessage());
        }
    }

    /**
     * Check if current price touches ATR target and update metrics accordingly
     * Called from ticker/websocket updates
     */
    public void checkAndUpdateATRTarget(String symbol, String strategy, double currentPrice) {
        try {
            // Find the most recent open metrics for this symbol+strategy
            TradeEntryMetrics metrics = null;
            for (int i = metricsHistory.size() - 1; i >= 0; i--) {
                TradeEntryMetrics m = metricsHistory.get(i);
                if (m.getSymbol().equals(symbol) && m.getStrategy().equals(strategy) && "open".equals(m.getStatus())) {
                    metrics = m;
                    break;
                }
            }

            if (metrics != null && !metrics.isAtrTargetTouched()) {
                // Check if current price has touched or exceeded ATR target
                if (currentPrice >= metrics.getAtrTarget()) {
                    metrics.setAtrTargetTouched(true);

                    // Rewrite file to persist the update
                    rewriteMetricsFile();

                    log.info("🎯 ATR Target touched for {} {} - Current Price: {}, ATR Target: {}",
                        symbol, strategy, currentPrice, metrics.getAtrTarget());
                }
            }
        } catch (Exception e) {
            log.debug("❌ Failed to check ATR target for {} {}: {}", symbol, strategy, e.getMessage());
        }
    }
    
    /**
     * Reset opening range at market open
     */
    @Scheduled(cron = "0 15 9 * * MON-FRI")
    public void resetOpeningRange() {
        openingRangeMap.clear();
        log.info("🔄 Opening range reset for new trading day");
    }
    
    // ========== Helper Methods ==========
    
    private void storeMetrics(TradeEntryMetrics metrics) {
        metricsHistory.add(metrics);
        
        // Trim if exceeds max size
        if (metricsHistory.size() > MAX_METRICS_IN_MEMORY) {
            metricsHistory.remove(0);
            log.debug("🧹 Trimmed metrics list to {}", MAX_METRICS_IN_MEMORY);
        }
    }
    
    /**
     * Load metrics from file on startup (only most recent entries to limit memory)
     */
    private void loadMetricsFromFile() {
        File file = new File(METRICS_FILE);
        if (!file.exists()) {
            log.info("📂 No existing metrics file found, will create new one");
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            boolean isHeader = true;
            int loadedCount = 0;

            // Read all lines first to get only the last MAX_METRICS_IN_MEMORY entries
            List<String> allLines = new java.util.ArrayList<>();
            while ((line = br.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                allLines.add(line);
            }

            // Take only the last MAX_METRICS_IN_MEMORY entries and parse them
            int startIndex = Math.max(0, allLines.size() - MAX_METRICS_IN_MEMORY);
            for (int i = startIndex; i < allLines.size(); i++) {
                String csvLine = allLines.get(i);
                TradeEntryMetrics metrics = TradeEntryMetrics.fromCsvLine(csvLine);
                if (metrics != null) {
                    metricsHistory.add(metrics);
                    loadedCount++;
                }
            }

            log.info("📊 Loaded {} metrics entries from file (total {} entries in file)",
                loadedCount, allLines.size());

        } catch (IOException e) {
            log.error("❌ Failed to load metrics from file: {}", e.getMessage());
        }
    }

    /**
     * Append a single metrics entry to the file
     * This is efficient for new entries
     */
    private synchronized void appendMetricsToFile(TradeEntryMetrics metrics) {
        try {
            // Create parent directory and file if they don't exist
            File file = new File(METRICS_FILE);
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            boolean fileExists = file.exists();

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(METRICS_FILE, true))) {
                // Write header if file is new
                if (!fileExists || file.length() == 0) {
                    bw.write(TradeEntryMetrics.csvHeader());
                    bw.newLine();
                }

                // Append the metrics entry
                bw.write(metrics.toCsvLine());
                bw.newLine();
                bw.flush();
            }

            log.debug("✅ Appended metrics entry to file for {}", metrics.getSymbol());

        } catch (IOException e) {
            log.error("❌ Failed to append metrics to file: {}", e.getMessage());
        }
    }

    /**
     * Rewrite entire metrics file from disk
     * This is necessary when updating existing rows (exit metrics, ATR target touched)
     * Reads all existing data from file, updates it, and rewrites
     */
    private synchronized void rewriteMetricsFile() {
        try {
            // Read all existing data from file first
            File file = new File(METRICS_FILE);
            List<TradeEntryMetrics> allMetrics = new java.util.ArrayList<>();

            if (file.exists()) {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    boolean isHeader = true;

                    while ((line = br.readLine()) != null) {
                        if (isHeader) {
                            isHeader = false;
                            continue;
                        }
                        TradeEntryMetrics metrics = TradeEntryMetrics.fromCsvLine(line);
                        if (metrics != null) {
                            allMetrics.add(metrics);
                        }
                    }
                }
            }

            // Update the loaded metrics with current in-memory state
            // Match by symbol, strategy, buyTime (unique identifier for a trade)
            for (TradeEntryMetrics memMetrics : metricsHistory) {
                boolean found = false;
                for (int i = 0; i < allMetrics.size(); i++) {
                    TradeEntryMetrics diskMetrics = allMetrics.get(i);
                    if (diskMetrics.getSymbol().equals(memMetrics.getSymbol()) &&
                        diskMetrics.getStrategy().equals(memMetrics.getStrategy()) &&
                        diskMetrics.getBuyTime().equals(memMetrics.getBuyTime())) {
                        // Replace with in-memory version (which may have updates)
                        allMetrics.set(i, memMetrics);
                        found = true;
                        break;
                    }
                }

                // If not found in disk metrics, it's a new entry (shouldn't happen in rewrite scenario)
                if (!found) {
                    allMetrics.add(memMetrics);
                }
            }

            // Create parent directory if it doesn't exist
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            // Write all metrics back to file
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(METRICS_FILE, false))) {
                // Write header
                bw.write(TradeEntryMetrics.csvHeader());
                bw.newLine();

                // Write all metrics
                for (TradeEntryMetrics metrics : allMetrics) {
                    bw.write(metrics.toCsvLine());
                    bw.newLine();
                }

                bw.flush();
            }

            log.debug("✅ Rewrote metrics file with {} total entries (from {} in-memory entries)",
                allMetrics.size(), metricsHistory.size());

        } catch (IOException e) {
            log.error("❌ Failed to rewrite metrics file: {}", e.getMessage());
        }
    }
    
    private double getOpenPrice(String symbol, List<Candle> candles, double previousClose) {
        // First, try to get the official opening price from Instrument OHLC data (from exchange)
        Instrument instrument = Nifty500SymbolToInstrument.get(symbol);
        if (instrument != null && instrument.getTodayOpenPrice() != null && instrument.getTodayOpenPrice() > 0) {
            return instrument.getTodayOpenPrice();
        }

        // Fallback: use first candle's open price
        if (candles != null && !candles.isEmpty()) {
            return candles.get(0).getOpen();
        }

        // Last resort: use previous close
        return previousClose;
    }
    
    private double getDayLow(List<Candle> candles, Candle latestCandle) {
        if (candles == null || candles.isEmpty()) {
            return latestCandle != null ? latestCandle.getLow() : 0;
        }
        
        double dayLow = Double.MAX_VALUE;
        for (Candle c : candles) {
            dayLow = Math.min(dayLow, c.getLow());
        }
        if (latestCandle != null) {
            dayLow = Math.min(dayLow, latestCandle.getLow());
        }
        return dayLow == Double.MAX_VALUE ? 0 : dayLow;
    }
    
    private VolumeMetrics calculateVolumeMetrics(String symbol, List<Candle> candles, Candle latestCandle) {
        VolumeMetrics metrics = new VolumeMetrics();
        
        if (latestCandle != null) {
            metrics.currentVolume = latestCandle.getVolume();
        }
        
        if (candles != null && !candles.isEmpty()) {
            // Calculate average of last 20 candles
            int count = Math.min(20, candles.size());
            long totalVolume = 0;
            
            for (int i = candles.size() - count; i < candles.size(); i++) {
                totalVolume += candles.get(i).getVolume();
            }
            
            metrics.averageVolume20 = count > 0 ? (double) totalVolume / count : 0;
            metrics.volumeRatio = metrics.averageVolume20 > 0 ? 
                (double) metrics.currentVolume / metrics.averageVolume20 : 0;
        }
        
        return metrics;
    }
    
    // ========== Inner Classes ==========
    

    
    private static class VolumeMetrics {
        double currentVolume = 0;
        double averageVolume20 = 0;
        double volumeRatio = 0;
    }
    
    public List<TradeEntryMetrics> getMetricsHistory() {
        return metricsHistory;
    }
}

