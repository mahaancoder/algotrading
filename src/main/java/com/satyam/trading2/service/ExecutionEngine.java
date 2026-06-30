package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.*;
import com.satyam.trading2.domain.service.HoldingService;
import com.satyam.trading2.domain.service.OrderExecutor;
import com.satyam.trading2.domain.service.PnLCalculator;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.risk.Exits;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.infrastructure.messaging.BroadcastService;
import com.satyam.trading2.infrastructure.messaging.SnapshotService;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.websocket.WebSocketBroadcaster;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.satyam.trading2.datamodel.Position.PositionType.HOLDING;
import static com.satyam.trading2.datamodel.TradeSignal.SignalType.EXIT_LONG;
import static com.satyam.trading2.helpers.LatestPriceHelper.getLatestPrice;


@Service
@RequiredArgsConstructor
@Data
public class ExecutionEngine {

    // ===== NEW: Clean Domain Services =====
    private final RiskManager riskManager;
    private final PositionManager positionManager;
    private final OrderExecutor orderExecutor;
    private final PendingOrderRepository pendingOrderRepository;
    private final DipAccumulatorService dipAccumulatorService;
    private final SignalAnalyticsService signalAnalyticsService;
    private final KillSwitchService killSwitchService;
    private final ProductToggleService productToggleService;

    // ===== GETTER: Allow other components to use the same locks for synchronization =====
    private final Map<String, Object> symbolStrategyLocks = new ConcurrentHashMap<>();
    public final Map<String, MarketContext> symbolMarketContext = new ConcurrentHashMap<>();

    private Object getSymbolStrategyLock(String symbol, String strategy) {
        String lockKey = symbol + "_" + strategy;
        return symbolStrategyLocks.computeIfAbsent(lockKey, k -> new Object());
    }

    private boolean isOutsideTradingHours() {
        LocalTime now = LocalTime.now();
        LocalTime marketOpen = LocalTime.of(9, 15);
        LocalTime marketClose = LocalTime.of(15, 30);
        return now.isBefore(marketOpen) || now.isAfter(marketClose);
    }

    /**
     * Check if BUY orders are allowed (9:15 AM - 3:00 PM)
     */
    private boolean isBuyAllowed() {
        LocalTime now = LocalTime.now();
        LocalTime buyStart = LocalTime.of(9, 5);
        LocalTime buyEnd = LocalTime.of(15, 0);
        return !now.isBefore(buyStart) && now.isBefore(buyEnd);
    }

    // ─────────────────────

    public void processSignal(TradeSignal signal, MarketContext ctx) throws Exception {
        String symbol = signal.getSymbol();
        String strategy = signal.getStrategyName();

        synchronized (getSymbolStrategyLock(symbol, strategy)) {
            processSignalInternal(signal, ctx);
        }
    }


    private void processSignalInternal(TradeSignal signal, MarketContext ctx) throws Exception {
        String symbol = signal.getSymbol();
        String strategy = signal.getStrategyName();
        int qty = calculateQty(signal);
        double capitalRequired = qty * signal.getEntryPrice();

        // ===== KILL SWITCH CHECK (highest priority - stops all trading) =====
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [KILL SWITCH] Buy rejected for " + symbol + " - Kill switch is ACTIVE");
            signalAnalyticsService.recordSignal(signal, false, "Kill switch active - all trading stopped", capitalRequired);
            return;
        }

        if (!isBuyAllowed()) {
            LocalTime now = LocalTime.now();
            System.out.println("⏰ [DEBUG] Buy rejected - Current time: " + now + " (expected 09:05 - 15:00)");
            signalAnalyticsService.recordSignal(signal, false, "Outside buy hours (9:15 AM - 3:00 PM)", capitalRequired);
            return;
        }

        if (Exits.isExitedToday(symbol, strategy)) {
            signalAnalyticsService.recordSignal(signal, false, "Symbol+Strategy exited today", capitalRequired);
            return;
        }

        Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);
        Position position = positionManager.getPosition(symbol, strategy);

        if (!isAccumulationAllowed(position, signal.getEntryPrice(), symbol, strategy)) {
            signalAnalyticsService.recordSignal(signal, false, "Accumulation not allowed (price condition)", capitalRequired);
            return;
        }

        executeBuy(signal, ctx);
    }

    private void executeBuy(TradeSignal signal, MarketContext ctx) throws IOException {

        // ===== OPTIMIZATION: Pre-compute all values before any blocking operations =====
        String symbol = signal.getSymbol();
        String strategy = signal.getStrategyName();
        double entry = signal.getEntryPrice();
        int qty = calculateQty(signal);
        double actualCapital = qty * entry;
        DipState state = dipAccumulatorService.get(symbol, strategy);

        // ===== STEP 1: Determine MIS or CNC (pre-computed, no lookups in order placement) =====
        Position position = positionManager.getPosition(symbol, strategy);
        boolean isHolding = (position != null && position.getPositionType() == HOLDING);

        // ===== STEP 1.2: Get Entry Type from Signal (set by strategy) =====
        EntryType entryType = signal.getEntryType() != null ? signal.getEntryType() : EntryType.UNKNOWN;

        // ===== STEP 1.5: Circuit Limit Check =====
        if (!com.satyam.trading2.helpers.CircuitLimitChecker.isTargetPriceWithinCircuitLimits(symbol, entry, isHolding)) {
            System.out.println("❌ [executeBuy] REJECTED " + symbol + " - target price would hit upper circuit limit. Entry: " + entry);
            signalAnalyticsService.recordSignal(signal, false, "Circuit limit breach - target would hit upper circuit", actualCapital);
            return;
        }

        // ===== STEP 1.6: Product Toggle Check (MIS/CNC disabled check) =====
        if (isHolding && !productToggleService.isCncEnabled()) {
            System.out.println("🛑 [executeBuy] REJECTED " + symbol + " - CNC buy orders are DISABLED");
            signalAnalyticsService.recordSignal(signal, false, "CNC buy orders disabled - toggle to enable", actualCapital);
            return;
        }
        if (!isHolding && !productToggleService.isMisEnabled()) {
            System.out.println("🛑 [executeBuy] REJECTED " + symbol + " - MIS buy orders are DISABLED");
            signalAnalyticsService.recordSignal(signal, false, "MIS buy orders disabled - toggle to enable", actualCapital);
            return;
        }

        // ===== STEP 2: Risk Check (fast, in-memory) =====
        System.out.println("🔍 [executeBuy] Checking signal safety for " + symbol + " | Capital: ₹" +
                         String.format("%.0f", actualCapital) + " | Product: " + (isHolding ? "CNC" : "MIS") +
                         " | Strategy: " + strategy);

        RiskManager.RiskCheckResult riskCheck = riskManager.checkSignalSafety(symbol, strategy, actualCapital, entry, state, isHolding);
        if(!riskCheck.isSafe()) {
            System.out.println("❌ [executeBuy] Risk check FAILED for " + symbol + ": " + riskCheck.getRejectionReason());
            signalAnalyticsService.recordSignal(signal, false, riskCheck.getRejectionReason(), actualCapital);
            return;
        }
        System.out.println("✅ [executeBuy] Risk check PASSED for " + symbol);

        // ===== STEP 2.5: RESERVE CAPITAL PER STRATEGY (prevents race conditions) =====
        if (!riskManager.reserveCapital(strategy, actualCapital)) {
            System.out.println("❌ [executeBuy] Capital reservation FAILED for " + symbol + " - strategy budget would be exceeded. Aborting.");
            signalAnalyticsService.recordSignal(signal, false, "Insufficient strategy capital - budget exceeded", actualCapital);
            return;
        }

        // ===== STEP 2.6: RESERVE CAPITAL PER SYMBOL (prevents exceeding 50K per symbol) =====
        if (!riskManager.reserveCapitalForSymbol(symbol, actualCapital)) {
            System.out.println("❌ [executeBuy] Symbol capital reservation FAILED for " + symbol + " - would exceed 50K limit per symbol. Aborting.");
            riskManager.releaseCapital(strategy, actualCapital); // Release strategy reservation
            signalAnalyticsService.recordSignal(signal, false, "Symbol capital limit - exceeds 50K per symbol", actualCapital);
            return;
        }

        try {
            // ===== STEP 3: PLACE ENTRY ORDER (This is the critical path - minimize time here) =====
            OrderResponse res = orderExecutor.placeBuyOrder(symbol, qty, entry, isHolding, strategy);

            // ===== STEP 4: CREATE PENDING ORDER =====
            String orderId = res.getEntryOrderId();
            // Null check - if order placement failed, don't proceed
            if (orderId == null) {
                System.out.println("❌ [executeBuy] Order placement FAILED for " + symbol + " - orderId is null. Aborting.");
                riskManager.releaseCapital(strategy, actualCapital); // Release strategy reservation
                riskManager.releaseCapitalForSymbol(symbol, actualCapital); // Release symbol reservation
                signalAnalyticsService.recordSignal(signal, false, "Order placement failed - orderId is null", actualCapital);
                return;
            }

            PendingOrder po = pendingOrderRepository.create(orderId, symbol, TradeSide.BUY, strategy, qty, entry, isHolding);
            po.setEntryType(entryType);  // Set entry type for analytics
            pendingOrderRepository.save(po);  // ✅ Save to repository so webhook can find it

            symbolMarketContext.put(symbol, ctx);

            // ===== RECORD SUCCESSFUL SIGNAL EXECUTION =====
            signalAnalyticsService.recordSignal(signal, true, null, actualCapital);


            // ==== STEP 5: CREATE/UPDATE DIP STATE =====
            if (state == null) {
                state = new DipState();
                state.setSymbol(symbol);
                state.setOwnerStrategy(strategy);
                state.setLevelFilled(1);
            } else {
                state.setLevelFilled(state.getLevelFilled() + 1);
            }
            state.addBuy(actualCapital, qty);
            state.setLastPrice(entry);
            state.setLastObservedPrice(entry);
            state.setLastBuyTime(System.currentTimeMillis());
            dipAccumulatorService.save(state);



            // NOTE: Capital remains "reserved" until webhook confirms position creation
            // The webhook handler will call riskManager.confirmCapitalUsage()

        } catch (Exception e) {
            // On any failure, release the reserved capital (both strategy and symbol)
            System.out.println("❌ [executeBuy] Exception during order placement for " + symbol + ": " + e.getMessage());
            riskManager.releaseCapital(strategy, actualCapital);
            riskManager.releaseCapitalForSymbol(symbol, actualCapital);
            signalAnalyticsService.recordSignal(signal, false, "Exception during order placement: " + e.getMessage(), actualCapital);
            e.printStackTrace();
            throw e;
        }
    }


    private static int calculateQty(TradeSignal signal) {
        return Math.max(1, (int) (25000 / signal.getEntryPrice()));
    }

    public void executeSell(TradeSignal signal, Position position) throws IOException {

        String symbol = signal.getSymbol();
        String strategy = signal.getStrategyName();
        int qty = position.getTotalQuantity();

        // ===== KILL SWITCH CHECK (highest priority - stops all trading) =====
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [KILL SWITCH] Sell rejected for " + symbol + " - Kill switch is ACTIVE");
            return;
        }

        if(isOutsideTradingHours()) return;
        // Validation
        if (position == null || !position.isOpen() || qty <= 0 || position.isExitProcessed()) {
            System.out.println("Position not valid for sell: isOpen=" + position.isOpen() + ", qty=" + qty);
            return;
        }

        // ===== EXECUTE SELL (using OrderExecutor) =====
        boolean isHolding = position.getPositionType() == HOLDING;
        orderExecutor.cancelOrder(position.getTargetOrderId());
        String orderId = orderExecutor.placeSellOrder(symbol, qty, signal.getEntryPrice(), isHolding,strategy);

        // Null check - if order placement failed, don't proceed
        if (orderId == null) {
            System.out.println("❌ [executeSell] Order placement FAILED for " + symbol + " - orderId is null. Aborting.");
            return;
        }

        PendingOrder po = pendingOrderRepository.create(orderId, symbol, TradeSide.SELL, strategy, qty , signal.getEntryPrice(), isHolding);
        pendingOrderRepository.save(po);  // ✅ Save to repository so webhook can find it

//        ALL 3 below steps we are doing at onsellcompleted event too here we are doing only for safety sake
        position.setExitProcessed(true);
        // ===== MARK AS EXITED (using RiskManager) - PER STRATEGY =====
        Exits.markAsExitedToday(symbol, strategy);
        // ===== CLEANUP STATE =====
        dipAccumulatorService.remove(symbol, strategy);
    }

    private boolean isAccumulationAllowed(Position existingPosition, double newPrice, String symbol, String strategy) {
        // No existing position? This is a new entry - always allowed
        if (existingPosition == null || !existingPosition.isOpen()) {
            return true;
        }

        // This strategy already has an open position - check accumulation rules
        double previousEntry = existingPosition.getAveragePrice();

        // Holdings can accumulate anytime (no 1% drop requirement)
        if (existingPosition.getPositionType() == HOLDING) {
            return true;
        }

        // Active positions require 1% price drop for accumulation
        boolean priceDroppedEnough = newPrice < previousEntry && newPrice <= previousEntry * 0.99;

        if (!priceDroppedEnough) {
            return false;
        }

        return true;
    }






}