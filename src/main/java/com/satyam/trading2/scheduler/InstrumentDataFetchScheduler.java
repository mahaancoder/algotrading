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
        if (symbols == null || symbols.isEmpty()) {
            System.err.println("❌ Cannot load previous close: instrument token set is empty. "
                    + "Verify the 09:03 instrument-loading job completed successfully.");
            return;
        }

        System.out.println("📊 Loading previous close for " + symbols.size() + " Nifty 500 symbols...");

        List<String> nseSymbols = new ArrayList<>();
        Map<String, String> symbolByQuoteKey = new HashMap<>();
        for (Long token : symbols) {
            String symbol = Nifty500Stocks.getSymbolFromToken(token);
            if (symbol == null || symbol.trim().isEmpty()) {
                continue;
            }
            String quoteKey = "NSE:" + symbol;
            nseSymbols.add(quoteKey);
            symbolByQuoteKey.put(quoteKey, symbol);
        }

        if (nseSymbols.isEmpty()) {
            System.err.println("❌ No valid NSE symbols could be resolved from instrument tokens.");
            return;
        }

        System.out.println("🔍 First 5 NSE symbols: "
                + nseSymbols.stream().limit(5).collect(Collectors.toList()));

        Map<String, Double> quotes = extracted(nseSymbols);
        int loadedCount = 0;
        List<String> failedSymbols = new ArrayList<>();

        for (String quoteKey : nseSymbols) {
            Double previousClose = quotes.get(quoteKey);
            String symbol = symbolByQuoteKey.get(quoteKey);

            if (previousClose == null || !Double.isFinite(previousClose) || previousClose <= 0) {
                if (failedSymbols.size() < 20) {
                    failedSymbols.add(quoteKey);
                }
                continue;
            }

            marketContextBuilder.setPreviousClose(symbol, previousClose);
            stockSelectionService.setPreviousClose(symbol, previousClose);
            loadedCount++;
        }

        int failedCount = nseSymbols.size() - loadedCount;
        System.out.println("========================================");
        System.out.println("📊 Previous-close loading completed:");
        System.out.println("   Requested: " + nseSymbols.size());
        System.out.println("   ✅ Valid prices loaded: " + loadedCount);
        System.out.println("   ❌ Missing/invalid prices: " + failedCount);
        if (!failedSymbols.isEmpty()) {
            System.out.println("   First missing symbols: " + failedSymbols);
        }
        if (failedCount > 0) {
            System.err.println("⚠️ Previous-close data is incomplete. Symbols without valid prices "
                    + "will remain ineligible for percentage-based stock selection.");
        }
        System.out.println("========================================");
    }

    /**
     * Retry only symbols whose previous-close quote is still missing.
     * getPreviousClosePrices performs bounded API batching on every attempt.
     */
    private Map<String, Double> extracted(List<String> requestedSymbols) {
        Map<String, Double> resolved = new HashMap<>();
        if (requestedSymbols == null || requestedSymbols.isEmpty()) {
            return resolved;
        }

        Set<String> unresolved = new LinkedHashSet<>(requestedSymbols);
        final int maxAttempts = 3;

        for (int attempt = 1; attempt <= maxAttempts && !unresolved.isEmpty(); attempt++) {
            List<String> attemptSymbols = new ArrayList<>(unresolved);
            System.out.println("🔍 Previous-close attempt " + attempt + "/" + maxAttempts
                    + " for " + attemptSymbols.size() + " unresolved symbols");

            Map<String, Double> fetched = orderServiceV2.getPreviousClosePrices(attemptSymbols);
            if (fetched != null && !fetched.isEmpty()) {
                for (Map.Entry<String, Double> entry : fetched.entrySet()) {
                    Double value = entry.getValue();
                    if (entry.getKey() != null && value != null
                            && Double.isFinite(value) && value > 0) {
                        resolved.put(entry.getKey(), value);
                        unresolved.remove(entry.getKey());
                    }
                }
            }

            if (!unresolved.isEmpty() && attempt < maxAttempts) {
                System.err.println("⚠️ Still missing " + unresolved.size()
                        + " previous-close prices; retrying only unresolved symbols.");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("❌ Interrupted during previous-close retry; stopping retries.");
                    break;
                }
            }
        }

        if (!unresolved.isEmpty()) {
            System.err.println("❌ Previous-close fetch exhausted retries. Unresolved count="
                    + unresolved.size() + ", first symbols="
                    + unresolved.stream().limit(20).collect(Collectors.toList()));
        }
        return resolved;
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
