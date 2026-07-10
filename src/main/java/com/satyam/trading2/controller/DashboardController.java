package com.satyam.trading2.controller;


import com.satyam.trading2.config.KiteAuthService;
import com.satyam.trading2.config.KiteConfig;
import com.satyam.trading2.datamodel.FilterConfig;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.Trade;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.infrastructure.messaging.BroadcastService;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Controller
public class DashboardController {


    @Autowired
    private PositionManager positionManager;

    @Autowired
    private KillSwitchService killSwitchService;

    @Autowired
    private ProductToggleService productToggleService;

    @Autowired
    private BroadcastService broadcastService;

    @Autowired
    private StockFilterService stockFilterService;

    @Autowired
    private RiskManager riskManager;

    @Value("${trading.paper.mode:false}")
    private boolean paperMode;

    @Value("${trading.watchlist:NSE:RELIANCE}")
    private String watchlist;

    @Value("${trading.capital.total:1200000}")
    private double totalCapital;

    // ═══════════════════════════════════════════════════════════════════════
    //  MAIN DASHBOARD
    // ═══════════════════════════════════════════════════════════════════════

    @GetMapping("/")
    public String dashboard(Model model) {

        model.addAttribute("authenticated", true);
        model.addAttribute("paperMode", true);


        // 🔥 Positions - Using PositionManager directly
        int openPositions = positionManager.getOpenPositionCount();
        model.addAttribute("openPositions", openPositions);

        // Get unique symbols from all open positions
        Set<String> openInstruments = new HashSet<>();
        for (Position p : positionManager.getAllOpenPositions()) {
            openInstruments.add(p.getSymbol());
        }
        model.addAttribute("openInstruments", openInstruments);

        // 🔥 Dummy values (until wired properly)
        model.addAttribute("authenticated", true);
        model.addAttribute("paperMode", true);
        model.addAttribute("regime", "BULL");

        return "dashboard";
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  RESERVED CAPITAL DIAGNOSTICS
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Get current reserved capital snapshot (strategy + symbol)
     */
    @GetMapping("/api/risk/reserved-capital")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getReservedCapital() {
        Map<String, Object> response = new HashMap<>();
        response.put("totalReservedStrategy", riskManager.getTotalReservedCapital());
        response.put("totalReservedSymbol", riskManager.getTotalReservedCapitalBySymbol());
        response.put("reservedByStrategy", riskManager.getReservedCapitalPerStrategySnapshot());
        response.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(response);
    }

    /**
     * Emergency button: release ALL reserved capital (local reservations only).
     * WARNING: This does NOT cancel broker orders.
     */
    @PostMapping("/api/risk/release-all-reserved")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> releaseAllReserved() {
        riskManager.releaseAllReservedCapital();

        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "All reserved capital cleared (strategy + symbol)");
        response.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(response);
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  KILL SWITCH API
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Toggle kill switch and broadcast the new state to all connected clients
     */
    @PostMapping("/api/killswitch/toggle")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> toggleKillSwitch() {
        boolean newState = killSwitchService.toggle();

        // Broadcast the new state to all connected WebSocket clients
        broadcastService.broadcastKillSwitch(newState);

        Map<String, Object> response = new HashMap<>();
        response.put("active", newState);
        response.put("message", newState ? "Kill switch ACTIVATED - All trading stopped" : "Kill switch DEACTIVATED - Trading resumed");

        return ResponseEntity.ok(response);
    }

    /**
     * Get current kill switch status
     */
    @GetMapping("/api/killswitch/status")
    @ResponseBody
    public ResponseEntity<Map<String, Boolean>> getKillSwitchStatus() {
        Map<String, Boolean> response = new HashMap<>();
        response.put("active", killSwitchService.isActive());
        return ResponseEntity.ok(response);
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  PRODUCT TOGGLE API (MIS/CNC)
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Toggle MIS buy orders and broadcast the new state to all connected clients
     */
    @PostMapping("/api/product/mis/toggle")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> toggleMis() {
        boolean newState = productToggleService.toggleMis();

        // Broadcast the new state to all connected WebSocket clients
        broadcastService.broadcastProductToggle("MIS", newState);

        Map<String, Object> response = new HashMap<>();
        response.put("enabled", newState);
        response.put("message", newState ? "MIS buy orders ENABLED" : "MIS buy orders DISABLED");

        return ResponseEntity.ok(response);
    }

    /**
     * Toggle CNC buy orders and broadcast the new state to all connected WebSocket clients
     */
    @PostMapping("/api/product/cnc/toggle")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> toggleCnc() {
        boolean newState = productToggleService.toggleCnc();

        // Broadcast the new state to all connected WebSocket clients
        broadcastService.broadcastProductToggle("CNC", newState);

        Map<String, Object> response = new HashMap<>();
        response.put("enabled", newState);
        response.put("message", newState ? "CNC buy orders ENABLED" : "CNC buy orders DISABLED");

        return ResponseEntity.ok(response);
    }

    /**
     * Get current product toggle status (MIS and CNC)
     */
    @GetMapping("/api/product/status")
    @ResponseBody
    public ResponseEntity<Map<String, Boolean>> getProductStatus() {
        Map<String, Boolean> response = new HashMap<>();
        response.put("misEnabled", productToggleService.isMisEnabled());
        response.put("cncEnabled", productToggleService.isCncEnabled());
        return ResponseEntity.ok(response);
    }

    /**
     * Diagnostic endpoint: Get opening range statistics
     * Shows how many stocks have opening range data calculated
     */
    @GetMapping("/api/diagnostic/opening-range")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getOpeningRangeStats() {
        Map<String, Object> stats = new HashMap<>();

        int totalStocks = com.satyam.trading2.datamodel.Nifty500Stocks.instrumentTokenMap.size();
        int withOpeningRange = MarketContextBuilder.openingRangeMap.size();

        stats.put("totalStocks", totalStocks);
        stats.put("withOpeningRange", withOpeningRange);
        stats.put("missingOpeningRange", totalStocks - withOpeningRange);
        stats.put("coveragePercent", totalStocks > 0 ?
                  String.format("%.1f%%", (withOpeningRange * 100.0 / totalStocks)) : "0%");

        // Sample of stocks WITH opening range (first 10)
        List<String> samplesWithData = MarketContextBuilder.openingRangeMap.keySet()
                .stream()
                .limit(10)
                .collect(java.util.stream.Collectors.toList());
        stats.put("sampleStocksWithData", samplesWithData);

        // Sample of stocks WITHOUT opening range (first 10)
        List<String> samplesWithoutData = com.satyam.trading2.datamodel.Nifty500Stocks.instrumentTokenMap.keySet()
                .stream()
                .filter(symbol -> !MarketContextBuilder.openingRangeMap.containsKey(symbol))
                .limit(10)
                .collect(java.util.stream.Collectors.toList());
        stats.put("sampleStocksWithoutData", samplesWithoutData);

        return ResponseEntity.ok(stats);
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  STOCK FILTER MANAGEMENT
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Get current filter configuration
     */
    @GetMapping("/api/filters/config")
    @ResponseBody
    public ResponseEntity<FilterConfig> getFilterConfig() {
        return ResponseEntity.ok(stockFilterService.getFilterConfig());
    }

    /**
     * Update filter configuration
     * Request body should contain FilterConfig with conditions, combineOperator, enabled, applyTo
     */
    @PostMapping("/api/filters/config")
    @ResponseBody
    public ResponseEntity<Map<String, String>> updateFilterConfig(@RequestBody FilterConfig config) {
        stockFilterService.updateFilterConfig(config);

        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Filter configuration updated successfully");
        response.put("enabled", String.valueOf(config.isEnabled()));
        response.put("conditions", String.valueOf(config.getConditions().size()));

        return ResponseEntity.ok(response);
    }

    /**
     * Clear all filters (disable filtering)
     */
    @PostMapping("/api/filters/clear")
    @ResponseBody
    public ResponseEntity<Map<String, String>> clearFilters() {
        stockFilterService.clearFilters();

        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "All filters cleared");

        return ResponseEntity.ok(response);
    }

    /**
     * Get list of available filter fields
     * Returns all numeric and boolean fields from TradeEntryMetrics that can be used in filters
     */
    @GetMapping("/api/filters/fields")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getFilterFields() {
        Map<String, Object> response = new HashMap<>();

        // Numeric fields
        List<Map<String, String>> numericFields = new ArrayList<>();
        addField(numericFields, "gapPercent", "Gap %", "Percentage gap from previous close");
        addField(numericFields, "distanceFromOpenPrice", "Distance from Open %", "Distance from today's open price");
        addField(numericFields, "distanceFromVwap", "Distance from VWAP %", "Distance from VWAP");
        addField(numericFields, "volumeRatio", "Volume Ratio", "Current volume / Average volume");
        addField(numericFields, "stock5DayReturn", "Stock 5-Day Return %", "Stock's 5-day return");
        addField(numericFields, "nifty5DayReturn", "Nifty 5-Day Return %", "Nifty's 5-day return");
        addField(numericFields, "relativeStrength", "Relative Strength", "Stock return - Nifty return");
        addField(numericFields, "distanceFromDayLow", "Distance from Day Low %", "Distance from day's low");
        addField(numericFields, "atrPercent", "ATR %", "ATR as percentage of price");
        addField(numericFields, "ema20", "20 EMA", "20-period EMA value");
        addField(numericFields, "ema50", "50 EMA", "50-period EMA value");

        // Boolean fields
        List<Map<String, String>> booleanFields = new ArrayList<>();
        addField(booleanFields, "aboveEma20", "Above 20 EMA", "Price above 20 EMA");
        addField(booleanFields, "aboveEma50", "Above 50 EMA", "Price above 50 EMA");
        addField(booleanFields, "twoGreenCandles", "Two Green Candles", "Last 2 candles are green");
        addField(booleanFields, "buyAboveOpeningRangeHigh", "Above Opening Range High", "Price above opening range high");
        addField(booleanFields, "buyAboveYesterdayHigh", "Above Yesterday High", "Price above yesterday's high");

        response.put("numericFields", numericFields);
        response.put("booleanFields", booleanFields);
        response.put("operators", List.of(">", ">=", "<", "<=", "==", "!="));
        response.put("combineOperators", List.of("AND", "OR"));
        response.put("applyToOptions", List.of("GAINERS", "LOSERS", "BOTH"));

        return ResponseEntity.ok(response);
    }

    private void addField(List<Map<String, String>> list, String key, String label, String description) {
        Map<String, String> field = new HashMap<>();
        field.put("key", key);
        field.put("label", label);
        field.put("description", description);
        list.add(field);
    }

}
