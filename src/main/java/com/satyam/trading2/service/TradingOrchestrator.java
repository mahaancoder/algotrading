package com.satyam.trading2.service;

import com.satyam.trading2.config.KiteConfig;
import com.satyam.trading2.datamodel.Instrument;
import com.satyam.trading2.datamodel.Nifty500Stocks;
import com.satyam.trading2.datamodel.Order;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.risk.Exits;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.scheduler.InstrumentDataFetchScheduler;
import com.satyam.trading2.websocket.WebSocketService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.satyam.trading2.datamodel.Nifty500Stocks.tradingSymbols;

@Service
@RequiredArgsConstructor
public class TradingOrchestrator {

    private final KiteConfig kiteConfig;
    private final WebSocketService webSocketService;
    private final MarketContextBuilder marketContextBuilder;
    private final OrderServiceV2 orderServiceV2;
    private final TradeJournalService tradeJournalService;
    private final StockSelectionService stockSelectionService;
    private final ExecutionEngine executionEngine;
    private final PositionManager positionManager;
    private final RiskManager riskManager;
    private final InstrumentDataFetchScheduler instrumentDataFetchScheduler;

    private final ReconciliationService reconciliationService;
    private boolean started = false;

    public void start() {

        if (started) return;

        if (!kiteConfig.isAuthenticated()) {
            System.out.println("❌ Not authenticated");
            return;
        }
        marketContextBuilder.resetForNewDay();
        started = true;

        // ✅ CRITICAL FIX: Ensure instrument data is loaded before starting WebSocket
        // If app restarts after 09:03 AM, the scheduled job won't run until tomorrow
        // This ensures we always have instrument data regardless of restart time
        ensureInstrumentDataLoaded();

        // ✅ Load exited positions from today's completed SELL orders
        // Prevents re-entering symbols that were already exited today
        loadExitedSymbolsFromTodaysOrders();

        loadHoldingsAtStartup();
        reconciliationService.syncBrokerPositions(true);
        // ✅ FIX: Initialize stock selection immediately after loading previous close
        // Without this, selectedGainers/selectedLosers remain EMPTY for first 30 seconds
        // causing MomentumDipAccumulatorStrategy to never generate signals
        stockSelectionService.updateSelection();
        System.out.println("✅ Initial stock selection updated - ready for momentum signals");

        // Trade history is now loaded automatically via @PostConstruct in TradeJournalService


        webSocketService.startTicker(kiteConfig.getApiKey(), kiteConfig.getAccessToken());
    }


    public void loadHoldingsAtStartup() {
        System.out.println("Loading holdings at startup");
        List<Position> holdings = orderServiceV2.getHoldings();
        for (Position h : holdings) {
            positionManager.addPosition(h.getSymbol(), h.getStrategy(), h);
        }

        // Reconstruct missing DipStates for holdings
        reconstructMissingDipStates(holdings);
    }

    /**
     * Ensure instrument data is loaded before starting trading
     * If app restarts after 09:03 AM, scheduled jobs won't run until tomorrow
     * This method manually triggers the data load if needed
     */
    private void ensureInstrumentDataLoaded() {
        if (Nifty500Stocks.Nifty500tokenToInstrument.isEmpty()) {
            System.out.println("⚠️ Instrument data not loaded - triggering manual load...");

            // Step 1: Load instrument tokens, circuit limits, today's open (normally runs at 09:03 AM)
            instrumentDataFetchScheduler.fetchInstrumentsTokenCircuitLimitTodayOpen();
            System.out.println("✅ Loaded " + Nifty500Stocks.Nifty500tokenToInstrument.size() + " instruments");

            // Step 2: Load previous close data (normally runs at 09:10 AM)
            instrumentDataFetchScheduler.fetchPreviousCloseData();

            // Step 3: Opening range data (normally runs at 09:20 AM) - skip if before 09:20
            java.time.LocalTime now = java.time.LocalTime.now();
            java.time.LocalTime openingRangeTime = java.time.LocalTime.of(9, 20);
            if (now.isAfter(openingRangeTime)) {
                System.out.println("⚠️ After 09:20 AM - loading opening range data...");
                instrumentDataFetchScheduler.fetchOpeningRangeAndVwap();
            }
        } else {
            System.out.println("✅ Instrument data already loaded (" + Nifty500Stocks.Nifty500tokenToInstrument.size() + " instruments)");
        }
    }

    /**
     * Reconstruct DipStates for holdings that don't have them
     * This handles cases where DipState file is corrupted/deleted
     */
    private void reconstructMissingDipStates(List<Position> holdings) {
        System.out.println("🔧 Checking for missing DipStates...");
        int reconstructed = 0;

        for (Position holding : holdings) {
            String symbol = holding.getSymbol();
            String strategy = holding.getStrategy();
            com.satyam.trading2.datamodel.DipState existingState = executionEngine.getDipAccumulatorService().get(symbol, strategy);

            if (existingState == null) {
                // DipState missing for this holding - reconstruct it
                executionEngine.getDipAccumulatorService().reconstructFromHolding(holding, strategy);
                reconstructed++;
            }
        }

        if (reconstructed > 0) {
            System.out.println("✅ Reconstructed " + reconstructed + " missing DipStates from holdings");
        } else {
            System.out.println("✅ All holdings have DipStates");
        }
    }

    /**
     * Load symbols that were exited today from completed SELL orders
     * This prevents re-entering positions that were already closed today
     *
     * Fetches all COMPLETED SELL orders for MIS product (intraday) from today
     * and marks them as exited in the Exits map
     */
    private void loadExitedSymbolsFromTodaysOrders() {
        System.out.println("🔍 Loading exited symbols from today's completed SELL orders...");

        try {
            // Fetch all orders from today
            List<Order> allOrders = orderServiceV2.fetchOrders();

            // Filter for COMPLETED SELL orders with MIS product (intraday positions)
            List<Order> completedMisSells = allOrders.stream()
                    .filter(o -> "SELL".equalsIgnoreCase(o.getTransactionType()))
                    .filter(o -> "MIS".equals(o.getProduct())) // MIS = INTRADAY
                    .filter(o -> "COMPLETE".equalsIgnoreCase(o.getStatus()))
                    .collect(Collectors.toList());

            System.out.println("📊 Found " + completedMisSells.size() + " completed MIS SELL orders from today");

            if (completedMisSells.isEmpty()) {
                System.out.println("✅ No exits to load - fresh trading day");
                return;
            }

            // Mark each symbol as exited
            // Note: We don't know the exact strategy from the order, so we use a generic marker
            // The Exits.isSymbolExitedToday() method checks for ANY strategy exit
            Set<String> exitedSymbols = new HashSet<>();

            for (Order order : completedMisSells) {
                String symbol = order.getTradingSymbol();

                // Mark as exited with a generic "RESTART" strategy
                // This prevents re-entry regardless of which strategy wants to trade it
                Exits.markAsExitedToday(symbol, "Dip-Accumulator-Momentum");
                exitedSymbols.add(symbol);
            }

            System.out.println("🚫 Loaded " + exitedSymbols.size() + " unique exited symbols:");
            System.out.println("   " + String.join(", ",
                    exitedSymbols.stream()
                        .limit(10)
                        .collect(Collectors.toList())) +
                    (exitedSymbols.size() > 10 ? " (and " + (exitedSymbols.size() - 10) + " more)" : ""));
            System.out.println("✅ These symbols will not be re-entered today");

        } catch (Exception e) {
            System.err.println("❌ Failed to load exited symbols: " + e.getMessage());
            e.printStackTrace();
        }
    }

}