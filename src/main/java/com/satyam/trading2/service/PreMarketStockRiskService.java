package com.satyam.trading2.service;

import com.satyam.trading2.config.KiteConfig;
import com.satyam.trading2.datamodel.Nifty500Stocks;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.HistoricalData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Evaluates momentum BUY candidates using completed daily candles only.
 *
 * The historical query is deliberately capped at yesterday's close. It never
 * reads today's developing daily candle, so a 09:15 decision cannot leak
 * intraday/future information into the pre-market classification.
 *
 * News integration is represented by explicit operator-maintained symbol lists
 * until a verified, timestamped news provider is connected. An empty news list
 * is reported as NOT_CONFIGURED, never as "no adverse news".
 */
@Slf4j
@Service
public class PreMarketStockRiskService {
    private static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");
    private static final String NIFTY_50_TOKEN = "256265";
    private static final long MIN_HISTORICAL_REQUEST_INTERVAL_MS = 400L;

    private final KiteConfig kiteConfig;
    private final Map<String, DailyRiskAssessment> dailyCache = new ConcurrentHashMap<>();
    private volatile LocalDate cacheDate;
    private volatile Double niftyFiveSessionReturn;
    private volatile long lastHistoricalRequestAt;

    @Value("${premarket.risk.block-on-unavailable:true}")
    private boolean blockOnUnavailable;

    @Value("${premarket.risk.news-blocked-symbols:}")
    private String configuredNewsBlockedSymbols;

    @Value("${premarket.risk.news-caution-symbols:}")
    private String configuredNewsCautionSymbols;

    public PreMarketStockRiskService(KiteConfig kiteConfig) {
        this.kiteConfig = kiteConfig;
    }

    /**
     * Returns candidates that have passed the pre-entry risk gate.
     * BLOCK, CAUTION and (by default) UNKNOWN candidates are excluded.
     */
    public synchronized FilterResult filterCandidates(List<String> symbols) {
        rollDate();
        Set<String> unique = new LinkedHashSet<>();
        if (symbols != null) {
            for (String symbol : symbols) {
                String normalized = normalize(symbol);
                if (normalized != null) unique.add(normalized);
            }
        }

        List<String> accepted = new ArrayList<>();
        Map<String, DailyRiskAssessment> assessments = new LinkedHashMap<>();
        int blocked = 0;
        int caution = 0;
        int unknown = 0;

        for (String symbol : unique) {
            DailyRiskAssessment assessment = assess(symbol);
            assessments.put(symbol, assessment);
            switch (assessment.status) {
                case NORMAL:
                    accepted.add(symbol);
                    break;
                case BLOCK:
                    blocked++;
                    break;
                case CAUTION:
                    caution++;
                    break;
                case UNKNOWN:
                    unknown++;
                    if (!blockOnUnavailable) accepted.add(symbol);
                    break;
                default:
                    unknown++;
                    if (!blockOnUnavailable) accepted.add(symbol);
            }
        }

        return new FilterResult(accepted, assessments, blocked, caution, unknown,
                blockOnUnavailable, getNewsCoverageStatus());
    }

    public DailyRiskAssessment assess(String rawSymbol) {
        rollDate();
        String symbol = normalize(rawSymbol);
        if (symbol == null) return DailyRiskAssessment.unknown("", "Invalid symbol");

        DailyRiskAssessment cached = dailyCache.get(symbol);
        if (cached != null) return cached;

        DailyRiskAssessment assessment = assessUncached(symbol);
        dailyCache.put(symbol, assessment);
        return assessment;
    }

    private DailyRiskAssessment assessUncached(String symbol) {
        String newsStatus = configuredNewsStatus(symbol);
        if (isConfiguredNewsBlocked(symbol)) {
            return new DailyRiskAssessment(symbol, Status.BLOCK,
                    "MANUAL_NEWS_BLOCKLIST", null, null, null, null, newsStatus);
        }
        if (isConfiguredNewsCaution(symbol)) {
            return new DailyRiskAssessment(symbol, Status.CAUTION,
                    "MANUAL_NEWS_CAUTION", null, null, null, null, newsStatus);
        }

        String token = Nifty500Stocks.instrumentTokenMap.get(symbol);
        if (token == null || token.trim().isEmpty()) {
            return unavailable(symbol, "Instrument token unavailable", newsStatus);
        }
        if (!kiteConfig.isAuthenticated()) {
            return unavailable(symbol, "Kite historical-data session unavailable", newsStatus);
        }

        try {
            List<HistoricalData> candles = fetchCompletedDailyCandles(token);
            if (candles == null || candles.size() < 6) {
                return unavailable(symbol, "Fewer than six completed daily candles", newsStatus);
            }

            int size = candles.size();
            double lastClose = candles.get(size - 1).close;
            double close1 = candles.get(size - 2).close;
            double close2 = candles.get(size - 3).close;
            double close3 = candles.get(size - 4).close;
            double close5 = candles.get(size - 6).close;

            if (!validPrice(lastClose) || !validPrice(close1) || !validPrice(close2)
                    || !validPrice(close3) || !validPrice(close5)) {
                return unavailable(symbol, "Invalid historical close in risk window", newsStatus);
            }

            boolean red1 = lastClose < close1;
            boolean red2 = close1 < close2;
            boolean red3 = close2 < close3;
            int consecutiveRedSessions = red1 ? (red2 ? (red3 ? 3 : 2) : 1) : 0;
            double threeSessionReturn = ((lastClose - close3) / close3) * 100.0;
            double fiveSessionReturn = ((lastClose - close5) / close5) * 100.0;
            Double benchmarkReturn = getNiftyFiveSessionReturn();

            boolean severeThreeDaySelloff = consecutiveRedSessions >= 3
                    && threeSessionReturn <= -6.0;
            boolean severeFiveDaySelloff = fiveSessionReturn <= -8.0;
            if (severeThreeDaySelloff || severeFiveDaySelloff) {
                String reason = severeThreeDaySelloff
                        ? "THREE_CONSECUTIVE_RED_SESSIONS"
                        : "SEVERE_FIVE_SESSION_DRAWDOWN";
                return new DailyRiskAssessment(symbol, Status.BLOCK, reason,
                        consecutiveRedSessions, threeSessionReturn, fiveSessionReturn,
                        benchmarkReturn, newsStatus);
            }

            boolean weakThreeDayTrend = consecutiveRedSessions >= 2
                    && threeSessionReturn <= -4.0;
            boolean weakFiveDayTrend = fiveSessionReturn <= -5.0;
            boolean relativeWeakness = benchmarkReturn != null
                    && fiveSessionReturn <= benchmarkReturn - 3.0;
            if (weakThreeDayTrend || weakFiveDayTrend || relativeWeakness) {
                String reason = weakThreeDayTrend ? "WEAK_THREE_SESSION_TREND"
                        : weakFiveDayTrend ? "WEAK_FIVE_SESSION_TREND"
                        : "UNDERPERFORMS_NIFTY_50";
                return new DailyRiskAssessment(symbol, Status.CAUTION, reason,
                        consecutiveRedSessions, threeSessionReturn, fiveSessionReturn,
                        benchmarkReturn, newsStatus);
            }

            return new DailyRiskAssessment(symbol, Status.NORMAL, "NO_TECHNICAL_RED_FLAG",
                    consecutiveRedSessions, threeSessionReturn, fiveSessionReturn,
                    benchmarkReturn, newsStatus);
        } catch (Exception e) {
            log.warn("[PreMarketRisk] Historical assessment failed for {}: {}",
                    symbol, e.getMessage());
            return unavailable(symbol, "Historical-data request failed", newsStatus);
        }
    }

    private List<HistoricalData> fetchCompletedDailyCandles(String token)
            throws IOException, KiteException, InterruptedException {
        throttleHistoricalRequest();
        KiteConnect kite = new KiteConnect(kiteConfig.getApiKey());
        kite.setAccessToken(kiteConfig.getAccessToken());

        Calendar to = Calendar.getInstance(TimeZone.getTimeZone(ZONE));
        to.setTime(new Date());
        to.add(Calendar.DAY_OF_MONTH, -1);
        to.set(Calendar.HOUR_OF_DAY, 23);
        to.set(Calendar.MINUTE, 59);
        to.set(Calendar.SECOND, 59);
        to.set(Calendar.MILLISECOND, 999);

        Calendar from = (Calendar) to.clone();
        from.add(Calendar.DAY_OF_MONTH, -20);

        HistoricalData data = kite.getHistoricalData(from.getTime(), to.getTime(),
                token, "day", false, false);
        if (data == null || data.dataArrayList == null) return Collections.emptyList();
        return data.dataArrayList;
    }

    private synchronized void throttleHistoricalRequest() throws InterruptedException {
        long now = System.currentTimeMillis();
        long wait = MIN_HISTORICAL_REQUEST_INTERVAL_MS - (now - lastHistoricalRequestAt);
        if (wait > 0) Thread.sleep(wait);
        lastHistoricalRequestAt = System.currentTimeMillis();
    }

    private Double getNiftyFiveSessionReturn() {
        Double cached = niftyFiveSessionReturn;
        if (cached != null) return cached;
        try {
            List<HistoricalData> candles = fetchCompletedDailyCandles(NIFTY_50_TOKEN);
            if (candles == null || candles.size() < 6) return null;
            double latest = candles.get(candles.size() - 1).close;
            double prior = candles.get(candles.size() - 6).close;
            if (!validPrice(latest) || !validPrice(prior)) return null;
            double calculated = ((latest - prior) / prior) * 100.0;
            niftyFiveSessionReturn = calculated;
            return calculated;
        } catch (Exception e) {
            log.warn("[PreMarketRisk] Nifty 50 benchmark unavailable: {}", e.getMessage());
            return null;
        }
    }

    private DailyRiskAssessment unavailable(String symbol, String reason, String newsStatus) {
        return new DailyRiskAssessment(symbol, Status.UNKNOWN, reason,
                null, null, null, null, newsStatus);
    }

    private void rollDate() {
        LocalDate today = LocalDate.now(ZONE);
        if (!today.equals(cacheDate)) {
            synchronized (this) {
                if (!today.equals(cacheDate)) {
                    dailyCache.clear();
                    niftyFiveSessionReturn = null;
                    cacheDate = today;
                    lastHistoricalRequestAt = 0L;
                }
            }
        }
    }

    private boolean isConfiguredNewsBlocked(String symbol) {
        return parseSymbols(configuredNewsBlockedSymbols).contains(symbol);
    }

    private boolean isConfiguredNewsCaution(String symbol) {
        return parseSymbols(configuredNewsCautionSymbols).contains(symbol);
    }

    private String configuredNewsStatus(String symbol) {
        if (isConfiguredNewsBlocked(symbol)) return "MANUAL_BLOCKLIST";
        if (isConfiguredNewsCaution(symbol)) return "MANUAL_CAUTION";
        return "NOT_CONFIGURED";
    }

    private String getNewsCoverageStatus() {
        return parseSymbols(configuredNewsBlockedSymbols).isEmpty()
                && parseSymbols(configuredNewsCautionSymbols).isEmpty()
                ? "NOT_CONFIGURED" : "MANUAL_SYMBOL_OVERRIDES_ONLY";
    }

    private Set<String> parseSymbols(String raw) {
        Set<String> result = new LinkedHashSet<>();
        if (raw == null || raw.trim().isEmpty()) return result;
        for (String item : raw.split(",")) {
            String symbol = normalize(item);
            if (symbol != null) result.add(symbol);
        }
        return result;
    }

    private String normalize(String raw) {
        if (raw == null) return null;
        String symbol = raw.trim().toUpperCase(Locale.ROOT);
        if (symbol.startsWith("NSE:")) symbol = symbol.substring(4);
        return symbol.isEmpty() ? null : symbol;
    }

    private boolean validPrice(double value) {
        return Double.isFinite(value) && value > 0.0;
    }

    public Map<String, DailyRiskAssessment> getDailySnapshot() {
        rollDate();
        return Collections.unmodifiableMap(new LinkedHashMap<>(dailyCache));
    }

    public enum Status { NORMAL, CAUTION, BLOCK, UNKNOWN }

    public static class DailyRiskAssessment {
        public final String symbol;
        public final Status status;
        public final String reason;
        public final Integer consecutiveRedSessions;
        public final Double threeSessionReturn;
        public final Double fiveSessionReturn;
        public final Double niftyFiveSessionReturn;
        public final String newsStatus;

        DailyRiskAssessment(String symbol, Status status, String reason,
                Integer consecutiveRedSessions, Double threeSessionReturn,
                Double fiveSessionReturn, Double niftyFiveSessionReturn,
                String newsStatus) {
            this.symbol = symbol;
            this.status = status;
            this.reason = reason;
            this.consecutiveRedSessions = consecutiveRedSessions;
            this.threeSessionReturn = threeSessionReturn;
            this.fiveSessionReturn = fiveSessionReturn;
            this.niftyFiveSessionReturn = niftyFiveSessionReturn;
            this.newsStatus = newsStatus;
        }

        static DailyRiskAssessment unknown(String symbol, String reason) {
            return new DailyRiskAssessment(symbol, Status.UNKNOWN, reason,
                    null, null, null, null, "NOT_CONFIGURED");
        }
    }

    public static class FilterResult {
        public final List<String> acceptedSymbols;
        public final Map<String, DailyRiskAssessment> assessments;
        public final int blockedCount;
        public final int cautionCount;
        public final int unknownCount;
        public final boolean blockOnUnavailable;
        public final String newsCoverageStatus;

        FilterResult(List<String> acceptedSymbols,
                Map<String, DailyRiskAssessment> assessments,
                int blockedCount, int cautionCount, int unknownCount,
                boolean blockOnUnavailable, String newsCoverageStatus) {
            this.acceptedSymbols = Collections.unmodifiableList(new ArrayList<>(acceptedSymbols));
            this.assessments = Collections.unmodifiableMap(new LinkedHashMap<>(assessments));
            this.blockedCount = blockedCount;
            this.cautionCount = cautionCount;
            this.unknownCount = unknownCount;
            this.blockOnUnavailable = blockOnUnavailable;
            this.newsCoverageStatus = newsCoverageStatus;
        }
    }
}
