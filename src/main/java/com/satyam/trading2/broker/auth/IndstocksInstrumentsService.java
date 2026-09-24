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
                log.error("[IndstocksInstrumentsService] Instruments fetch failed: HTTP {}",
                        response.statusCode());
                return 0;
            }

            String csv = response.body();
            int mapped = parseAndRegister(csv);
            log.info("[IndstocksInstrumentsService] Instruments master loaded: {} symbols mapped "
                    + "out of {} watchlist symbols", mapped, Nifty500Stocks.tradingSymbols.size());
            return mapped;
        } catch (Exception e) {
            log.error("[IndstocksInstrumentsService] Instruments fetch exception: {}", e.getMessage(), e);
            return 0;
        }
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

            // Build column index map from the header row
            String[] headers = headerLine.split(",");
            int idxSecurityId = -1, idxSymbolName = -1, idxSeries = -1, idxExch = -1;
            for (int i = 0; i < headers.length; i++) {
                String h = headers[i].trim();
                switch (h) {
                    case "SECURITY_ID":  idxSecurityId = i; break;
                    case "SYMBOL_NAME":  idxSymbolName = i; break;
                    case "SERIES":       idxSeries = i; break;
                    case "EXCH":         idxExch = i; break;
                }
            }
            if (idxSecurityId < 0 || idxSymbolName < 0) {
                log.error("[IndstocksInstrumentsService] CSV missing required columns. "
                        + "Headers: {}", headerLine);
                return 0;
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] cols = line.split(",");
                if (cols.length <= Math.max(idxSecurityId, idxSymbolName)) continue;

                String symbolName = cols[idxSymbolName].trim();
                String securityId = cols[idxSecurityId].trim();

                // Filter: only EQ series on NSE (if columns are present)
                if (idxSeries >= 0 && cols.length > idxSeries) {
                    String series = cols[idxSeries].trim();
                    if (!"EQ".equals(series)) continue;
                }
                if (idxExch >= 0 && cols.length > idxExch) {
                    String exch = cols[idxExch].trim();
                    if (!"NSE".equals(exch)) continue;
                }

                // Only register if the symbol is in our watchlist
                if (Nifty500Stocks.tradingSymbols.contains(symbolName)) {
                    indstocksBroker.registerSecurityId(symbolName, securityId);
                    mapped++;
                }
            }
        } catch (Exception e) {
            log.error("[IndstocksInstrumentsService] CSV parse exception: {}", e.getMessage(), e);
        }
        return mapped;
    }
}
