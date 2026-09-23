package com.satyam.trading2.controller;


import com.satyam.trading2.config.KiteAuthService;
import com.satyam.trading2.config.KiteConfig;
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
    private BrokerToggleService brokerToggleService;

    @Autowired
    private DefaultBrokerService defaultBrokerService;

    @Autowired
    private BroadcastService broadcastService;

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

    // ═══════════════════════════════════════════════════════════════════════
    //  BROKER TOGGLE API (enable/disable trading per broker)
    // ═══════════════════════════════════════════════════════════════════════

    @PostMapping("/api/broker/toggle")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> toggleBroker(@RequestParam String broker) {
        com.satyam.trading2.broker.ExecutionRoute route;
        try {
            route = com.satyam.trading2.broker.ExecutionRoute.valueOf(broker.toUpperCase());
        } catch (IllegalArgumentException e) {
            Map<String, Object> err = new HashMap<>();
            err.put("success", false);
            err.put("message", "Unknown broker: " + broker);
            return ResponseEntity.badRequest().body(err);
        }
        boolean newState = brokerToggleService.toggle(route);
        broadcastService.broadcastBrokerToggle(route.name(), newState);
        Map<String, Object> response = new HashMap<>();
        response.put("broker", route.name());
        response.put("enabled", newState);
        response.put("message", newState
                ? route + " trading ENABLED"
                : route + " trading DISABLED — new buys will fall back to KITE");
        return ResponseEntity.ok(response);
    }

    @GetMapping("/api/broker/status")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getBrokerStatus() {
        Map<String, Object> response = new HashMap<>();
        response.put("brokers", brokerToggleService.getAllStates());
        return ResponseEntity.ok(response);
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  DEFAULT BROKER API (fallback route when no group filter matches)
    // ═══════════════════════════════════════════════════════════════════════

    @GetMapping("/api/broker/default")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getDefaultBroker() {
        Map<String, Object> response = new HashMap<>();
        response.put("broker", defaultBrokerService.getDefaultRoute().name());
        return ResponseEntity.ok(response);
    }

    @PostMapping("/api/broker/default")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> setDefaultBroker(@RequestParam String broker) {
        com.satyam.trading2.broker.ExecutionRoute route;
        try {
            route = defaultBrokerService.setDefaultRoute(broker);
        } catch (IllegalArgumentException e) {
            Map<String, Object> err = new HashMap<>();
            err.put("success", false);
            err.put("message", "Unknown broker: " + broker);
            return ResponseEntity.badRequest().body(err);
        }
        broadcastService.broadcastDefaultBroker(route.name());
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("broker", route.name());
        response.put("message", "Default broker set to " + route.name());
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

}
