package com.satyam.trading2.broker.auth;

import com.satyam.trading2.broker.IndstocksBroker;
import com.satyam.trading2.config.IndstocksConfig;
import com.satyam.trading2.datamodel.Nifty500Stocks;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Fetches the INDstocks instruments master CSV and populates the
 * symbol→securityId map on IndstocksBroker.
 *
 * Endpoint: GET /market/instruments?source=equity
 * Returns a CSV (not JSON) with columns including SECURITY_ID and SYMBOL_NAME.
 * We only register rows for symbols in our Nifty500Stocks.tradingSymbols list
 * to keep the map small.
 *
 * Only created when broker.indstocks.enabled=true.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "broker.indstocks.enabled", havingValue = "true")
public class IndstocksInstrumentsService {

    private final IndstocksConfig indstocksConfig;
    private final IndstocksBroker indstocksBroker;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(2000))
            .build();

    /**
     * Self-heal: runs shortly after startup and every 5 minutes. If INDstocks is
     * authenticated but no symbols are mapped yet (startup fetch failed, returned
     * 0, or the token was set outside the /auth/indstocks/token flow), retry the
     * instruments fetch so orders don't keep getting rejected with
     * "No security_id mapped". No-op once the map is populated or when not
     * authenticated.
     */
    @Scheduled(initialDelay = 45_000, fixedDelay = 300_000)
    public void ensureInstrumentsPopulated() {
        if (!indstocksConfig.isAuthenticated()) {
            return;
        }
        if (indstocksBroker.getMappedSymbolCount() > 0) {
            return;
        }
        System.out.println("🔄 [IndstocksInstrumentsService] Authenticated but 0 symbols mapped — retrying instruments fetch");
        int mapped = populateInstruments();
        System.out.println("🔄 [IndstocksInstrumentsService] Retry mapped " + mapped + " symbols");
    }

    /**
     * Fetch the instruments CSV and populate the symbol→securityId map.
     * @return number of symbols mapped
     */
    public int populateInstruments() {
        if (!indstocksConfig.isAuthenticated()) {
            log.warn("[IndstocksInstrumentsService] Not authenticated — skipping instruments fetch");
            return 0;
        }

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(indstocksConfig.getBaseUrl() + "/market/instruments?source=equity"))
                    .timeout(Duration.ofMillis(10000))  // CSV can be large
                    .header("Authorization", indstocksConfig.getAuthHeader())
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                System.out.println("❌ [IndstocksInstrumentsService] Instruments fetch failed: HTTP "
                        + response.statusCode() + " body=" + truncate(response.body(), 300));
                return 0;
            }

            String csv = response.body();
            int mapped = parseAndRegister(csv);
            System.out.println("✅ [IndstocksInstrumentsService] Instruments master loaded: " + mapped
                    + " symbols mapped out of " + Nifty500Stocks.tradingSymbols.size() + " watchlist symbols");
            return mapped;
        } catch (Exception e) {
            System.out.println("❌ [IndstocksInstrumentsService] Instruments fetch exception: " + e.getMessage());
            log.error("[IndstocksInstrumentsService] Instruments fetch exception: {}", e.getMessage(), e);
            return 0;
        }
    }

    /** Truncate a string for safe diagnostic logging. */
    private static String truncate(String s, int max) {
        if (s == null) return "<null>";
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }

    /**
     * Parse the CSV and register mappings for symbols in our watchlist.
     * CSV columns (per docs): EXCH, SEGMENT, SECURITY_ID, INSTRUMENT_NAME,
     * EXPIRY_CODE, TRADING_SYMBOL, LOT_UNITS, CUSTOM_SYMBOL, EXPIRY_DATE,
     * STRIKE_PRICE, OPTION_TYPE, TICK_SIZE, EXPIRY_FLAG, SEM_EXCH_INSTRUMENT_TYPE, SERIES, SYMBOL_NAME
     *
     * We match on SYMBOL_NAME (base symbol like "HDFCBANK") against our
     * Nifty500 watchlist, and only take rows where SERIES == "EQ".
     */
    private int parseAndRegister(String csv) {
        int mapped = 0;
        try (BufferedReader reader = new BufferedReader(new StringReader(csv))) {
            String headerLine = reader.readLine();
            if (headerLine == null) return 0;

            // Strip a UTF-8 BOM if present on the header line.
            if (!headerLine.isEmpty() && headerLine.charAt(0) == '\uFEFF') {
                headerLine = headerLine.substring(1);
            }

            // Build column index map from the header row. Tolerate quoted headers.
            String[] headers = headerLine.split(",");
            int idxSecurityId = -1, idxSymbolName = -1, idxSeries = -1, idxExch = -1, idxTradingSymbol = -1;
            for (int i = 0; i < headers.length; i++) {
                String h = headers[i].trim().replaceAll("^\"|\"$", "").toUpperCase();
                switch (h) {
                    case "SECURITY_ID":  idxSecurityId = i; break;
                    case "SYMBOL_NAME":  idxSymbolName = i; break;
                    case "SERIES":       idxSeries = i; break;
                    case "EXCH":         idxExch = i; break;
                    case "TRADING_SYMBOL":
                    case "TRADINGSYMBOL":
                    case "TRADESYMBOL":
                    case "TSYM":
                    case "SEM_TRADING_SYMBOL":
                        idxTradingSymbol = i; break;
                }
            }
            System.out.println("🔍 [IndstocksInstrumentsService] CSV header: " + headerLine);
            System.out.println("🔍 [IndstocksInstrumentsService] resolved indices → SECURITY_ID=" + idxSecurityId
                    + " TRADING_SYMBOL=" + idxTradingSymbol + " SYMBOL_NAME=" + idxSymbolName
                    + " SERIES=" + idxSeries + " EXCH=" + idxExch);
            if (idxSecurityId < 0 || (idxTradingSymbol < 0 && idxSymbolName < 0)) {
                System.out.println("❌ [IndstocksInstrumentsService] CSV missing required columns "
                        + "(SECURITY_ID and one of TRADING_SYMBOL/SYMBOL_NAME). Header: " + headerLine);
                return 0;
            }

            // The watchlist (Nifty500Stocks.tradingSymbols) holds base NSE trading
            // symbols like "HDFCBANK". In this CSV that is the TRADING_SYMBOL column;
            // SYMBOL_NAME holds the full company name (e.g. "HDFC Bank Ltd"), which
            // never matches the watchlist. Match TRADING_SYMBOL first, then fall
            // back to SYMBOL_NAME for resilience.
            java.util.List<Integer> symbolCols = new java.util.ArrayList<>();
            if (idxTradingSymbol >= 0) symbolCols.add(idxTradingSymbol);
            if (idxSymbolName >= 0 && idxSymbolName != idxTradingSymbol) symbolCols.add(idxSymbolName);

            java.util.Set<String> matched = new java.util.HashSet<>();
            java.util.List<String> sampleRows = new java.util.ArrayList<>();
            java.util.List<String> sampleSymbols = new java.util.ArrayList<>();
            int totalRows = 0, afterSeries = 0, afterExch = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) continue;
                totalRows++;
                if (sampleRows.size() < 3) sampleRows.add(line);

                String[] cols = line.split(",");
                if (cols.length <= idxSecurityId) continue;

                String securityId = cols[idxSecurityId].trim().replaceAll("^\"|\"$", "");

                // Filter: only EQ series on NSE (if columns are present)
                if (idxSeries >= 0 && cols.length > idxSeries) {
                    String series = cols[idxSeries].trim().replaceAll("^\"|\"$", "");
                    if (!"EQ".equalsIgnoreCase(series)) continue;
                }
                afterSeries++;
                if (idxExch >= 0 && cols.length > idxExch) {
                    String exch = cols[idxExch].trim().replaceAll("^\"|\"$", "");
                    if (!"NSE".equalsIgnoreCase(exch)) continue;
                }
                afterExch++;

                // Match the watchlist against the candidate symbol columns
                // (TRADING_SYMBOL first, then SYMBOL_NAME). Normalize each value
                // (uppercase, strip surrounding quotes and a trailing "-EQ") so it
                // lines up with the watchlist and with resolveSecurityId's lookup.
                boolean sampled = false;
                for (int ci : symbolCols) {
                    if (cols.length <= ci) continue;
                    String candidate = cols[ci].trim().replaceAll("^\"|\"$", "").toUpperCase();
                    if (candidate.endsWith("-EQ")) {
                        candidate = candidate.substring(0, candidate.length() - 3);
                    }
                    if (candidate.isEmpty()) continue;
                    if (!sampled && sampleSymbols.size() < 20) { sampleSymbols.add(candidate); sampled = true; }
                    if (Nifty500Stocks.tradingSymbols.contains(candidate) && !matched.contains(candidate)) {
                        indstocksBroker.registerSecurityId(candidate, securityId);
                        matched.add(candidate);
                        mapped++;
                        break;
                    }
                }
            }

            System.out.println("🔍 [IndstocksInstrumentsService] rows: total=" + totalRows
                    + " afterSeries(EQ)=" + afterSeries + " afterExch(NSE)=" + afterExch
                    + " matchedWatchlist=" + mapped);
            System.out.println("🔍 [IndstocksInstrumentsService] sample data rows: " + sampleRows);
            System.out.println("🔍 [IndstocksInstrumentsService] sample post-filter symbol values (TRADING_SYMBOL): " + sampleSymbols);

            // Diagnostic: if some watchlist symbols didn't match a CSV row, list a
            // sample so symbol-naming differences (e.g. M&M vs MM, BAJAJ-AUTO vs
            // BAJAJAUTO) are visible and fixable.
            if (mapped < Nifty500Stocks.tradingSymbols.size()) {
                StringBuilder sample = new StringBuilder();
                int shown = 0;
                for (String w : Nifty500Stocks.tradingSymbols) {
                    if (!matched.contains(w)) {
                        if (shown > 0) sample.append(", ");
                        sample.append(w);
                        if (++shown >= 15) break;
                    }
                }
                System.out.println("⚠️ [IndstocksInstrumentsService] " + shown
                        + "+ watchlist symbols not matched in CSV (sample): " + sample);
            }
        } catch (Exception e) {
            System.out.println("❌ [IndstocksInstrumentsService] CSV parse exception: " + e.getMessage());
            log.error("[IndstocksInstrumentsService] CSV parse exception: {}", e.getMessage(), e);
        }
        return mapped;
    }
}
