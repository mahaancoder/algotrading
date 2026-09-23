package com.satyam.trading2.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Central trading configuration — loaded from application.properties.
 *
 * KEY CHANGE from v1:
 *   Removed single `trading.instrument` — replaced by `trading.watchlist`
 *   which supports 1 to 500+ instruments simultaneously.
 *
 * For 100+ stocks, set:
 *   trading.watchlist=NSE:RELIANCE,NSE:TCS,NSE:INFY,... (comma-separated)
 *   trading.capital.per.instrument=10000  (Rs10k risk capital per stock)
 *   trading.max.open.positions=10         (max simultaneous open trades)
 */
@Component
@Data
public class TradingConfig {

    // ── Watchlist ─────────────────────────────────────────────────────────

    @Value("${trading.watchlist:NSE:RELIANCE}")
    private String watchlistRaw;          // comma-separated "NSE:RELIANCE,NSE:TCS,..."

    @Value("${trading.lot.sizes:100}")
    private String lotSizesRaw;           // comma-separated lot sizes matching watchlist order

    // ── Capital ───────────────────────────────────────────────────────────

    @Value("${trading.capital.total:1000000}")
    private double totalCapital;          // Rs 12 Lakhs total

    @Value("${trading.capital.intraday:300000}")
    private double intradayCapital;       // Rs 3L for active intraday trading

    @Value("${trading.capital.per.instrument:5000}")
    private double capitalPerInstrument;  // risk capital allocated per stock

    // ── Risk ──────────────────────────────────────────────────────────────

    @Value("${trading.risk.percent:0.5}")
    private double riskPercent;           // % of per-instrument capital to risk per trade

    @Value("${trading.atr.multiplier:2.0}")
    private double atrMultiplier;         // stop-loss = entry - (ATR × this)

    @Value("${trading.rr.ratio:2.0}")
    private double rrRatio;               // risk:reward — target = entry + (risk × this)

    @Value("${trading.max.open.positions:10}")
    private int maxOpenPositions;         // max simultaneous open trades across all instruments

    @Value("${trading.max.portfolio.risk.percent:3.0}")
    private double maxPortfolioRiskPercent;

    @Value("${trading.daily.loss.limit.percent:1.5}")
    private double dailyLossLimitPercent;

    @Value("${trading.weekly.loss.limit.percent:3.0}")
    private double weeklyLossLimitPercent;

    @Value("${trading.confluence.required:2}")
    private int confluenceRequired;

    // ── Trailing stop ─────────────────────────────────────────────────────

    @Value("${trading.trailing.stop.enabled:true}")
    private boolean trailingStopEnabled;

    @Value("${trading.trailing.breakeven.at.r:1.0}")
    private double trailingBreakevenAtR;

    @Value("${trading.trailing.lock.profit.at.r:2.0}")
    private double trailingLockProfitAtR;

    @Value("${trading.trailing.lock.profit.percent:0.5}")
    private double trailingLockProfitPercent;

    // ── Timing ────────────────────────────────────────────────────────────

    @Value("${trading.squareoff.time:15:15}")
    private String squareOffTime;

    // ── Mode ──────────────────────────────────────────────────────────────

    @Value("${trading.paper.mode:false}")
    private boolean paperMode;

    // ═════════════════════════════════════════════════════════════════════
    //  Watchlist helpers
    // ═════════════════════════════════════════════════════════════════════

    /** Full watchlist as a list: ["NSE:RELIANCE", "NSE:TCS", ...] */
    public List<String> getWatchlist() {
        return Arrays.stream(watchlistRaw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /** Lot sizes as an int array, matched by index to the watchlist */
    public int[] getLotSizes() {
        String[] parts = lotSizesRaw.split(",");
        int[] sizes = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try { sizes[i] = Integer.parseInt(parts[i].trim()); }
            catch (NumberFormatException e) { sizes[i] = 1; }
        }
        return sizes;
    }

    /** Lot size for a specific instrument (by index in watchlist, default 1) */
    public int getLotSizeForInstrument(String instrument) {
        List<String> wl = getWatchlist();
        int[] ls = getLotSizes();
        int idx = wl.indexOf(instrument);
        if (idx >= 0 && idx < ls.length) return ls[idx];
        return 1;
    }

    /** Extract exchange from "NSE:RELIANCE" → "NSE" */
    public static String extractExchange(String instrument) {
        return instrument.contains(":") ? instrument.split(":")[0] : "NSE";
    }

    /** Extract symbol from "NSE:RELIANCE" → "RELIANCE" */
    public static String extractSymbol(String instrument) {
        return instrument.contains(":") ? instrument.split(":")[1] : instrument;
    }

    /** Max INR risk per trade for a given instrument */
    public double getMaxRiskAmount() {
        return capitalPerInstrument * (riskPercent / 100.0);
    }


    // Legacy compatibility — used by some strategy classes expecting a single instrument
    public double  getCapital()                  { return capitalPerInstrument; }
    public String  getInstrument()               { return getWatchlist().isEmpty() ? "NSE:RELIANCE" : getWatchlist().get(0); }
    public String  getExchange()                 { return extractExchange(getInstrument()); }
    public String  getTradingSymbol()            { return extractSymbol(getInstrument()); }
}
