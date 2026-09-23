package com.satyam.trading2.scheduler;

import com.satyam.trading2.config.KiteConfig;
import com.satyam.trading2.datamodel.Instrument;
import com.satyam.trading2.datamodel.Nifty500Stocks;
import com.satyam.trading2.service.*;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.HistoricalData;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.satyam.trading2.datamodel.Nifty500Stocks.*;

@Service
@RequiredArgsConstructor
public class InstrumentDataFetchScheduler {

    private final OrderServiceV2 orderServiceV2;
    private final EMAService emaService;
    private final RelativeStrengthService relativeStrengthService;
    private final TradeMetricsCollector tradeMetricsCollector;
    private final MarketContextBuilder marketContextBuilder;
    private final StockSelectionService stockSelectionService;
    private final KiteConfig kiteConfig;


    @Scheduled(cron = "0 03 09 * * MON-FRI")
    public void fetchInstrumentsTokenCircuitLimitTodayOpen() {
        fetchInstrumentTokenCircuitLimitsTodayOpen();
    }

    @Scheduled(cron = "0 10 09 * * MON-FRI")
    public void fetchPreviousCloseData() {
        loadPreviousCloseForNIfty500(Nifty500Stocks.Nifty500tokenToInstrument.keySet());
    }

    @Scheduled(cron = "0 20 09 * * MON-FRI")
    public void fetchOpeningRangeAndVwap() {
        try {
            collectOpeningRangeHighLow();
        } catch (Exception e) {
            System.err.println("❌ Failed to fetch opening range data: " + e.getMessage());
            e.printStackTrace();
        }
    }



    public void fetchInstrumentTokenCircuitLimitsTodayOpen(){
        // Step 1: Load basic instrument data from /instruments endpoint
        List<Instrument> instrumentList = orderServiceV2.populateInstrumentsData();
        instrumentList.forEach(instrument -> {
            if(tradingSymbols.contains(instrument.getTradingSymbol())) {
                Nifty500Stocks.Nifty500tokenToInstrument.put(Long.valueOf(instrument.getInstrumentToken()), instrument);
                Nifty500Stocks.Nifty500SymbolToInstrument.put(instrument.getTradingSymbol(), instrument);

                // Step 1.5: Populate EMA service and RelativeStrengthService with instrument tokens
                Nifty500Stocks.setInstrumentToken(instrument.getTradingSymbol(), instrument.getInstrumentToken());
                relativeStrengthService.setInstrumentToken(instrument.getTradingSymbol(), instrument.getInstrumentToken());
            }
        });
        fetchCircuitLimitsAndTodayOpenPrice();
    }

    public void loadPreviousCloseForNIfty500(Set<Long> symbols) {
        System.out.println("📊 [09:10] Loading previous close and yesterday's high for " + symbols.size() + " symbols...");
        try {
            // Convert tokens to NSE:SYMBOL format
            List<String> nseSymbols = symbols.stream()
                    .map(token -> {
                        String symbol = Nifty500Stocks.getSymbolFromToken(token);
                        return "NSE:" + symbol;
                    })
                    .collect(Collectors.toList());

            System.out.println("🔍 First 5 NSE symbols to fetch: " + nseSymbols.stream().limit(5).collect(Collectors.toList()));

            Map<String, Double> quotes = extracted(nseSymbols);
            Map<String, Double> yesterdayHighPrices = extractedYesterdayHigh(nseSymbols);

            if (quotes.isEmpty()) {
                System.err.println("❌ Failed to fetch previous close data - quotes map is empty!");
                return;
            }

            System.out.println("✅ Received " + quotes.size() + " quotes from API");
            System.out.println("✅ Received " + yesterdayHighPrices.size() + " yesterday's high prices");
            System.out.println("🔍 First 5 quotes keys: " + quotes.keySet().stream().limit(5).collect(Collectors.toList()));

            int loadedCount = 0;
            int yesterdayHighLoadedCount = 0;
            int failedCount = 0;
            List<String> failedSymbols = new ArrayList<>();

            for (Long instrument : symbols) {
                String symbol = Nifty500Stocks.getSymbolFromToken(instrument);
                String nseSymbol = "NSE:" + symbol;
                Double prevClose = quotes.get(nseSymbol);
                Double yesterdayHigh = yesterdayHighPrices.get(nseSymbol);

                if (prevClose == null) {
                    failedCount++;
                    if (failedSymbols.size() < 10) {
                        failedSymbols.add(symbol);
                    }
                    continue;
                }

                // Set previous close for both MarketContextBuilder (strategies) and StockSelectionService
                marketContextBuilder.setPreviousClose(symbol, prevClose);
                stockSelectionService.setPreviousClose(symbol, prevClose);
                loadedCount++;

                // Set yesterday's high if available
                if (yesterdayHigh != null && yesterdayHigh > 0) {
                    marketContextBuilder.setYesterdayHigh(symbol, yesterdayHigh);
                    yesterdayHighLoadedCount++;
                }
            }

            System.out.println("✅ Loaded prevClose for " + loadedCount + " symbols (out of " + symbols.size() + ")");
            System.out.println("✅ Loaded yesterday's high for " + yesterdayHighLoadedCount + " symbols");
            if (failedCount > 0) {
                System.out.println("⚠️ Failed to load " + failedCount + " symbols");
                System.out.println("   First 10 failed symbols: " + failedSymbols);
            }
        } catch (Exception e) {
            System.err.println("❌ Error loading previous close: " + e.getMessage());
            e.printStackTrace();
        }
    }


    private Map<String, Double> extracted(List<String> newSymbols) throws InterruptedException {
        Map<String, Double> quotes = new HashMap<>();

        System.out.println("🔍 Attempting to fetch previous close for " + newSymbols.size() + " symbols...");

        for(int i=0;i<3;i++) {
            System.out.println("   Attempt " + (i+1) + " of 3...");
            quotes = orderServiceV2.getPreviousClosePrices(newSymbols);

            if(!quotes.isEmpty()) {
                System.out.println("   ✅ Got " + quotes.size() + " quotes on attempt " + (i+1));
                return quotes;
            }

            System.out.println("   ⚠️ Attempt " + (i+1) + " returned empty, retrying...");
            Thread.sleep(2000);
        }

        System.err.println("   ❌ All 3 attempts failed - returning empty map");
        return quotes;
    }

    private Map<String, Double> extractedYesterdayHigh(List<String> newSymbols) throws InterruptedException {
        Map<String, Double> yesterdayHighPrices = new HashMap<>();

        System.out.println("🔍 Attempting to fetch yesterday's high for " + newSymbols.size() + " symbols...");

        for(int i=0;i<3;i++) {
            System.out.println("   Attempt " + (i+1) + " of 3...");
            yesterdayHighPrices = orderServiceV2.getYesterdayHighPrices(newSymbols);

            if(!yesterdayHighPrices.isEmpty()) {
                System.out.println("   ✅ Got " + yesterdayHighPrices.size() + " yesterday's high prices on attempt " + (i+1));
                return yesterdayHighPrices;
            }

            System.out.println("   ⚠️ Attempt " + (i+1) + " returned empty, retrying...");
            Thread.sleep(2000);
        }

        System.err.println("   ❌ All 3 attempts failed - returning empty map");
        return yesterdayHighPrices;
    }

    /**
     * Enrich instrument data with circuit limits and OHLC from /quote API
     * Fetches data in batches to respect API rate limits
     */
    private void fetchCircuitLimitsAndTodayOpenPrice() {
        List<String> symbols = new ArrayList<>(Nifty500Stocks.Nifty500SymbolToInstrument.keySet());

        // Process in batches of 200 symbols to avoid URL length limits
        int batchSize = 200;
        AtomicInteger enrichedCount = new AtomicInteger();

        for (int i = 0; i < symbols.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, symbols.size());
            List<String> batch = symbols.subList(i, endIndex);

            // Prepend "NSE:" to each symbol for quote API
            List<String> nseSymbols = batch.stream()
                    .map(s -> "NSE:" + s)
                    .collect(Collectors.toList());

            try {
                Map<String, Instrument> quoteData = orderServiceV2.getQuoteData(nseSymbols);

                // Merge quote data into existing instruments
                quoteData.forEach((symbol, quoteInstrument) -> {
                    Instrument existing = Nifty500Stocks.Nifty500SymbolToInstrument.get(symbol);
                    if (existing != null) {
                        existing.setUpperCircuitLimit(quoteInstrument.getUpperCircuitLimit());
                        existing.setLowerCircuitLimit(quoteInstrument.getLowerCircuitLimit());
                        existing.setTodayOpenPrice(quoteInstrument.getTodayOpenPrice());
                        enrichedCount.getAndIncrement();
                    }
                });

                // Small delay between batches to respect rate limits
                if (endIndex < symbols.size()) {
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                System.err.println("⚠️ Failed to fetch quote data for batch " + (i/batchSize + 1) + ": " + e.getMessage());
            }
        }
        System.out.println("📊 Enriched " + enrichedCount + " instruments with circuit limits and OHLC data");
    }

    private void collectOpeningRangeHighLow() {
        System.out.println("📊 [09:20] Starting opening range collection (9:15 - 9:20 only)...");
        System.out.println("📊 Total stocks to process: " + instrumentTokenMap.size());

        KiteConnect kiteConnect = new KiteConnect(kiteConfig.getApiKey());
        kiteConnect.setAccessToken(kiteConfig.getAccessToken());

        // Set fromDate to today at 09:15:00 AM
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 9);
        cal.set(Calendar.MINUTE, 15);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date fromDate = cal.getTime();

        // Set toDate to today at 09:20:00 AM
        cal.set(Calendar.MINUTE, 20);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date toDate = cal.getTime();

        int successCount = 0;
        int failureCount = 0;
        int processedCount = 0;
        int totalSymbols = instrumentTokenMap.size();
        List<String> failedSymbols = new ArrayList<>();
        List<String> noDataSymbols = new ArrayList<>();

        for (Map.Entry<String, String> entry : instrumentTokenMap.entrySet()) {
            String symbol = entry.getKey();
            String token = entry.getValue();
            processedCount++;

            // Progress indicator every 50 stocks
            if (processedCount % 50 == 0) {
                System.out.println("📊 Progress: " + processedCount + "/" + totalSymbols +
                                 " (Success: " + successCount + ", Failed: " + failureCount + ")");
            }

            try {
                // ===== RATE LIMITING: Add small delay every 3 requests to avoid hitting API limits =====
                if (processedCount % 3 == 0) {
                    Thread.sleep(200); // 200ms delay every 3 stocks
                }

                HistoricalData historicalData = kiteConnect.getHistoricalData(
                        fromDate,
                        toDate,
                        token,
                        "minute",  // Use 1-minute interval to get more granular data
                        false,
                        false
                );

                if (historicalData == null || historicalData.dataArrayList == null || historicalData.dataArrayList.isEmpty()) {
                    failureCount++;
                    if (noDataSymbols.size() < 10) {
                        noDataSymbols.add(symbol);
                    }
                    continue;
                }

                List<HistoricalData> candles = historicalData.dataArrayList;

                // Calculate high and low from candles ONLY in the 9:15 - 9:20 range
                double high = Double.MIN_VALUE;
                double low = Double.MAX_VALUE;
                int candlesProcessed = 0;

                // Create SimpleDateFormat to parse candle timestamps
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                for (HistoricalData candle : candles) {
                    // Filter: Only include candles with timestamp between 9:15 and 9:20
                    try {
                        if (candle.timeStamp != null) {
                            Date candleTime = sdf.parse(candle.timeStamp);
                            // Check if candle is within 9:15 - 9:20 window (candleTime >= fromDate && candleTime < toDate)
                            if (!candleTime.before(fromDate) && candleTime.before(toDate)) {
                                if (candle.high > high) high = candle.high;
                                if (candle.low < low) low = candle.low;
                                candlesProcessed++;
                            }
                        }
                    } catch (Exception e) {
                        // Skip candles with invalid timestamps
                        continue;
                    }
                }

                // Only set the range if we actually processed candles in the time window
                if (candlesProcessed > 0 && high != Double.MIN_VALUE && low != Double.MAX_VALUE) {
                    MarketContextBuilder.OpeningRange range = new MarketContextBuilder.OpeningRange();
                    range.high = high;
                    range.low = low;
                    MarketContextBuilder.setOpeningRangeMap(symbol, range);
                    successCount++;
                } else {
                    failureCount++;
                    if (noDataSymbols.size() < 10) {
                        noDataSymbols.add(symbol);
                    }
                }

            } catch (KiteException e) {
                failureCount++;
                if (failedSymbols.size() < 10) {
                    failedSymbols.add(symbol + " (KiteEx: " + e.getMessage() + ")");
                }

                // If rate limit error, add longer delay
                if (e.getMessage() != null &&
                    (e.getMessage().contains("Too many requests") ||
                     e.getMessage().contains("rate limit"))) {
                    System.out.println("⚠️ Rate limit hit, pausing for 2 seconds...");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (IOException e) {
                failureCount++;
                if (failedSymbols.size() < 10) {
                    failedSymbols.add(symbol + " (IO: " + e.getMessage() + ")");
                }
            } catch (Exception e) {
                failureCount++;
                if (failedSymbols.size() < 10) {
                    failedSymbols.add(symbol + " (Other: " + e.getMessage() + ")");
                }
            }
        }

        // ===== FINAL SUMMARY =====
        System.out.println("========================================");
        System.out.println("✅ Opening range collection complete:");
        System.out.println("   Total stocks: " + totalSymbols);
        System.out.println("   ✅ Success: " + successCount + " (" +
                         String.format("%.1f", (successCount * 100.0 / totalSymbols)) + "%)");
        System.out.println("   ❌ Failed: " + failureCount + " (" +
                         String.format("%.1f", (failureCount * 100.0 / totalSymbols)) + "%)");

        if (!noDataSymbols.isEmpty()) {
            System.out.println("   📋 First 10 symbols with no data: " + noDataSymbols);
        }
        if (!failedSymbols.isEmpty()) {
            System.out.println("   📋 First 10 failed symbols: " + failedSymbols);
        }
        System.out.println("========================================");
    }
}
