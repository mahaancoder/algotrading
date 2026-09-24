package com.satyam.trading2.execution;

import com.satyam.trading2.broker.Broker;
import com.satyam.trading2.broker.BrokerRouter;
import com.satyam.trading2.broker.ExecutionRoute;
import com.satyam.trading2.broker.model.BrokerOrderResponse;
import com.satyam.trading2.broker.model.OrderRequest;
import com.satyam.trading2.broker.model.OrderType;
import com.satyam.trading2.broker.model.ProductType;
import com.satyam.trading2.broker.model.TransactionType;
import com.satyam.trading2.datamodel.DipState;
import com.satyam.trading2.datamodel.EntryType;
import com.satyam.trading2.datamodel.MarketContext;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.risk.Exits;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.service.DefaultBrokerService;
import com.satyam.trading2.service.DipAccumulatorService;
import com.satyam.trading2.service.KillSwitchService;
import com.satyam.trading2.service.ProductToggleService;
import com.satyam.trading2.service.SignalAnalyticsService;
import com.satyam.trading2.service.BrokerToggleService;
import com.satyam.trading2.strategy.FilterEngine;
import com.satyam.trading2.strategy.FilterResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.satyam.trading2.datamodel.Position.PositionType.HOLDING;

/**
 * OrderExecutionService — the main execution entry point.
 *
 * Strategies call orderExecutionService.execute(signal) and remain completely
 * broker-independent. Everything after that is handled by:
 *   Filter → Router → (stickiness override) → Sanitizer → Broker → PendingOrder
 *
 * This replaces ExecutionEngine as the strategy's entry point. It absorbs:
 *  - Pre-flight gates (kill switch, trading hours, exited-today, accumulation)
 *  - Filter + routing (new)
 *  - Position-stickiness override (Decision #2: re-buying the same stock
 *    must go to the existing broker so correct averaging happens)
 *  - Order placement via Broker.placeOrder(OrderRequest)
 *  - PendingOrder creation
 *
 * The symbol+strategy lock map is shared with OrderCompletionProcessor and
 * ReconciliationService to prevent race conditions, same as ExecutionEngine did.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderExecutionService {

    private final FilterEngine filterEngine;
    private final BrokerRouter brokerRouter;
    private final RiskManager riskManager;
    private final PositionManager positionManager;
    private final PendingOrderRepository pendingOrderRepository;
    private final DipAccumulatorService dipAccumulatorService;
    private final SignalAnalyticsService signalAnalyticsService;
    private final KillSwitchService killSwitchService;
    private final ProductToggleService productToggleService;
    private final BrokerToggleService brokerToggleService;
    private final DefaultBrokerService defaultBrokerService;

    // Shared lock map — same pattern as ExecutionEngine, used to serialize
    // signal processing, order completion, and reconciliation per symbol+strategy.
    private final Map<String, Object> symbolStrategyLocks = new ConcurrentHashMap<>();
    public final Map<String, MarketContext> symbolMarketContext = new ConcurrentHashMap<>();

    public Map<String, Object> getSymbolStrategyLocks() {
        return symbolStrategyLocks;
    }

    private Object getSymbolStrategyLock(String symbol, String strategy) {
        return symbolStrategyLocks.computeIfAbsent(symbol + "_" + strategy, k -> new Object());
    }

    // ─────────────────────────────────────────────────────────────────────
    // BUY execution — the main entry point for strategies
    // ─────────────────────────────────────────────────────────────────────

    public void execute(TradeSignal signal, MarketContext ctx) throws Exception {
        String symbol = signal.getSymbol();
        String strategy = signal.getStrategyName();

        synchronized (getSymbolStrategyLock(symbol, strategy)) {
            executeBuy(signal, ctx);
        }
    }

    private void executeBuy(TradeSignal signal, MarketContext ctx) throws IOException {
        String symbol = signal.getSymbol();
        String strategy = signal.getStrategyName();
        double entry = signal.getEntryPrice();
        int qty = calculateQty(signal);
        double actualCapital = qty * entry;
        DipState state = dipAccumulatorService.get(symbol, strategy);

        // ===== GATE 1: KILL SWITCH =====
        if (killSwitchService.isActive()) {
            signalAnalyticsService.recordSignal(signal, false, "Kill switch active - all trading stopped", actualCapital);
            return;
        }

        // ===== GATE 2: TRADING HOURS =====
        if (!isBuyAllowed()) {
            signalAnalyticsService.recordSignal(signal, false, "Outside buy hours (9:15 AM - 3:00 PM)", actualCapital);
            return;
        }

        // ===== GATE 3: EXITED TODAY (price-aware per Decision #1) =====
        if (Exits.isExitedToday(symbol, strategy)) {
            // Re-entry allowed only when new buy price is lower than earlier buy price
            if (!Exits.isReEntryAllowed(symbol, strategy, entry)) {
                signalAnalyticsService.recordSignal(signal, false, "Symbol+Strategy exited today - re-entry price not lower", actualCapital);
                return;
            }
        }

        // ===== GATE 4: ACCUMULATION CHECK =====
        // Check across ALL brokers — if a position exists anywhere for this
        // symbol+strategy, accumulation rules apply. The stickiness override
        // below will route to that broker.
        Position existingPosition = findExistingPositionAcrossBrokers(symbol, strategy);
        if (!isAccumulationAllowed(existingPosition, entry)) {
            signalAnalyticsService.recordSignal(signal, false, "Accumulation not allowed (price condition)", actualCapital);
            return;
        }

        // ===== STEP 1: Determine MIS or CNC =====
        boolean isHolding = (existingPosition != null && existingPosition.getPositionType() == HOLDING);

        // ===== STEP 1.5: Circuit Limit Check =====
        if (!com.satyam.trading2.helpers.CircuitLimitChecker.isTargetPriceWithinCircuitLimits(symbol, entry, isHolding)) {
            signalAnalyticsService.recordSignal(signal, false, "Circuit limit breach - target would hit upper circuit", actualCapital);
            return;
        }

        // ===== STEP 1.6: Product Toggle Check =====
        if (isHolding && !productToggleService.isCncEnabled()) {
            signalAnalyticsService.recordSignal(signal, false, "CNC buy orders disabled - toggle to enable", actualCapital);
            return;
        }
        if (!isHolding && !productToggleService.isMisEnabled()) {
            signalAnalyticsService.recordSignal(signal, false, "MIS buy orders disabled - toggle to enable", actualCapital);
            return;
        }

        // ===== STEP 1.7: NO-TRADE VETO FILTER =====
        // If the signal matches any no-trade rule, block the trade entirely
        // (regardless of which broker group it would route to).
        String noTradeReason = filterEngine.getNoTradeRejectionReason(signal);
        if (noTradeReason != null) {
//            System.out.println("🚫 [NoTradeFilter] " + symbol + " rejected - " + noTradeReason);
            signalAnalyticsService.recordSignal(signal, false, noTradeReason, actualCapital);
            return;
        }

        // ===== STEP 2: DETERMINE BROKER FIRST (needed for broker-aware risk check) =====
        ExecutionRoute route = determineBrokerRoute(signal, symbol, strategy);
        if (route == null) {
            // Broker toggle rejection — existing position at a disabled broker, can't average
            signalAnalyticsService.recordSignal(signal, false, "Broker disabled — cannot average existing position", actualCapital);
            return;
        }
        Broker broker = brokerRouter.getBrokerWithFallback(route);
        String brokerName = broker.getBrokerName();

        // Visibility into every routing decision: which broker a BUY was sent to
        // and why (entryType drives the Group1/Group2 filter). Helps diagnose
        // why orders do/don't reach INDSTOCKS/SHOONYA vs the KITE fallback.
//        log.info("🧭 [Routing]  {} entryType={} → route={}",
//                 symbol, signal.getEntryType(), route);

        // ===== STEP 2.5: RISK CHECK (broker-aware — checks per-broker + global capital) =====
        RiskManager.RiskCheckResult riskCheck = riskManager.checkSignalSafety(symbol, strategy, actualCapital, entry, state, isHolding, brokerName);
        if (!riskCheck.isSafe()) {
            signalAnalyticsService.recordSignal(signal, false, riskCheck.getRejectionReason(), actualCapital);
            System.out.println("very risky");
            return;
        }

        // ===== STEP 2.6: RESERVE CAPITAL (strategy level — broker-aware) =====
        if (!riskManager.reserveCapital(strategy, actualCapital, brokerName)) {
            signalAnalyticsService.recordSignal(signal, false, "Insufficient " + brokerName + " broker capital - budget exceeded", actualCapital);
            System.out.println("less funds1");
            return;
        }

        // ===== STEP 2.7: RESERVE CAPITAL (symbol level) =====
        if (!riskManager.reserveCapitalForSymbol(symbol, actualCapital)) {
            riskManager.releaseCapital(strategy, actualCapital, brokerName);
            signalAnalyticsService.recordSignal(signal, false, "Symbol capital limit - exceeds 50K per symbol", actualCapital);
            System.out.println("less funds2");
            return;
        }

        try {
            // ===== STEP 4: BUILD ORDER REQUEST =====
            EntryType entryType = signal.getEntryType() != null ? signal.getEntryType() : EntryType.UNKNOWN;
            OrderRequest orderRequest = OrderRequest.builder()
                    .symbol(symbol)
                    .exchange("NSE")
                    .transactionType(TransactionType.BUY)
                    .orderType(OrderType.LIMIT)
                    .quantity(qty)
                    .price(entry)
                    .productType(isHolding ? ProductType.DELIVERY : ProductType.INTRADAY)
                    .strategyId(strategy)
                    .signalId(null) // TODO: add signalId to TradeSignal
                    .build();

            // ===== STEP 5: PLACE ORDER VIA BROKER =====
            BrokerOrderResponse response = broker.placeOrder(orderRequest);

            if (response == null || !response.isAccepted() || response.getBrokerOrderId() == null) {
                riskManager.releaseCapital(strategy, actualCapital, brokerName);
                riskManager.releaseCapitalForSymbol(symbol, actualCapital);
                String reason = response != null ? response.getRejectionReason() : "Broker returned null response";
                // Surface EVERY broker rejection in the log. Previously this only
                // recorded an analytics signal, so a rejected INDSTOCKS/SHOONYA order
                // (e.g. "No security_id mapped") produced no log line at all — the
                // "orders silently not reaching the broker" symptom.
                log.warn("❌ [OrderExecution] {} {} order REJECTED by {} → {}",
                        strategy, symbol, brokerName, reason);
                signalAnalyticsService.recordSignal(signal, false, "Order placement failed - " + reason, actualCapital);
                return;
            }

            String brokerOrderId = response.getBrokerOrderId();

            // ===== STEP 6: CREATE PENDING ORDER (broker-aware) =====
            PendingOrder po = pendingOrderRepository.create(
                    brokerOrderId, symbol, com.satyam.trading2.datamodel.TradeSide.BUY, strategy,
                    qty, entry, isHolding, brokerName, route, null);
            po.setEntryType(entryType);
            pendingOrderRepository.save(po);

            // Confirm exactly what lookup key was stored, so we can compare it against
            // the order_id the broker's WS feed later reports on completion. A mismatch
            // here vs the WS order_id is what leaves completions "unmatched" (no sell).
            System.out.println("[OrderExecution] PendingOrder stored → key=" + brokerName + "::" + brokerOrderId
                    + " symbol=" + symbol + " side=BUY qty=" + qty);

            symbolMarketContext.put(symbol, ctx);
            signalAnalyticsService.recordSignal(signal, true, null, actualCapital);

            // ===== STEP 7: CREATE/UPDATE DIP STATE =====
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

        } catch (Exception e) {
            riskManager.releaseCapital(strategy, actualCapital, brokerName);
            riskManager.releaseCapitalForSymbol(symbol, actualCapital);
            signalAnalyticsService.recordSignal(signal, false, "Exception during order placement: " + e.getMessage(), actualCapital);
            throw e;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Broker route determination — filter + stickiness override
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Determine which broker to route to.
     *
     * 1. Run FilterEngine to get the PREFERRED route (Group1→INDSTOCKS, etc).
     * 2. STICKINESS OVERRIDE (Decision #2): if a Position already exists for
     *    this symbol+strategy at ANY broker, route to THAT broker instead.
     *    This ensures correct averaging — you can't average a position split
     *    across two brokers.
     * 3. BROKER TOGGLE: if the selected broker is disabled, fall back to KITE
     *    for new positions. For stickiness (existing position at a disabled
     *    broker), return null to signal rejection — can't average at a
     *    disabled broker, and shouldn't split across brokers.
     */
    private ExecutionRoute determineBrokerRoute(TradeSignal signal, String symbol, String strategy) {
        FilterResult filterResult = filterEngine.evaluate(signal);
        ExecutionRoute preferredRoute = filterResult.getExecutionRoute();

        // DEFAULT BROKER OVERRIDE: when the signal matched NEITHER group filter,
        // the route defaults to KITE. Allow the operator to pick a different
        // default broker (INDSTOCKS/SHOONYA) from the dashboard for the day.
        // Group1/Group2 matches are untouched — this only replaces the KITE
        // fallback, and only if the chosen default is enabled + available.
        if (preferredRoute == ExecutionRoute.KITE) {
            ExecutionRoute configuredDefault = defaultBrokerService.getDefaultRoute();
            if (configuredDefault != ExecutionRoute.KITE
                    && brokerRouter.isAvailable(configuredDefault)
                    && brokerToggleService.isEnabled(configuredDefault)) {
                log.debug("[OrderExecutionService] {} matched no group — using dashboard default broker {}",
                        symbol, configuredDefault);
                preferredRoute = configuredDefault;
            }
        }

        // Stickiness: check if a position already exists at any broker
        Position existing = findExistingPositionAcrossBrokers(symbol, strategy);
        if (existing != null && existing.getBroker() != null && existing.getExecutionRoute() != null) {
            ExecutionRoute existingRoute = existing.getExecutionRoute();
            if (existingRoute != preferredRoute) {
                log.debug("[OrderExecutionService] Stickiness override: {} existing at {} → routing to {} instead of {}",
                        symbol, existing.getBroker(), existingRoute, preferredRoute);
            }
            // If the existing broker is disabled, we can't average there.
            // The caller will reject the buy — the position can still be exited via SELL.
            if (!brokerToggleService.isEnabled(existingRoute)) {
                log.info("🛑 [BrokerToggle] {} disabled — cannot average existing position at {} for {}",
                        existingRoute, symbol, strategy);
                return null; // signals rejection to caller
            }
            return existingRoute;
        }

        // New position: if preferred broker is disabled, fall back to KITE
        if (!brokerToggleService.isEnabled(preferredRoute)) {
            log.info("🔄 [BrokerToggle] {} disabled — falling back to KITE for new position in {}",
                    preferredRoute, symbol);
            return ExecutionRoute.KITE;
        }

        return preferredRoute;
    }

    /**
     * Find an existing open position for symbol+strategy across ALL brokers.
     * Returns the first match (there should only be one per the stickiness rule).
     */
    private Position findExistingPositionAcrossBrokers(String symbol, String strategy) {
        Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);
        for (Position p : strategyPositions.values()) {
            if (p != null && p.isOpen() && !p.isExitProcessed() && strategy.equals(p.getStrategy())) {
                return p;
            }
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────────
    // SELL execution (for schedulers / manual exits)
    // ─────────────────────────────────────────────────────────────────────

    public void executeSell(TradeSignal signal, Position position) throws IOException {
        String symbol = signal.getSymbol();
        String strategy = signal.getStrategyName();
        int qty = position.getTotalQuantity();

        if (killSwitchService.isActive()) {
            log.info("🛑 [KILL SWITCH] Sell rejected for {}", symbol);
            return;
        }

        if (isOutsideTradingHours()) return;

        if (position == null || !position.isOpen() || qty <= 0 || position.isExitProcessed()) {
            log.info("Position not valid for sell: {}", symbol);
            return;
        }

        boolean isHolding = position.getPositionType() == HOLDING;

        // Route to the broker that owns this position
        ExecutionRoute route = position.getExecutionRoute() != null
                ? position.getExecutionRoute() : ExecutionRoute.KITE;
        Broker broker = brokerRouter.getBrokerWithFallback(route);

        // Cancel existing target order at that broker
        if (position.getTargetOrderId() != null) {
            broker.cancelOrder(position.getTargetOrderId());
        }

        // Place sell order
        OrderRequest sellRequest = OrderRequest.builder()
                .symbol(symbol)
                .exchange("NSE")
                .transactionType(TransactionType.SELL)
                .orderType(OrderType.LIMIT)
                .quantity(qty)
                .price(signal.getEntryPrice())
                .productType(isHolding ? ProductType.DELIVERY : ProductType.INTRADAY)
                .strategyId(strategy)
                .build();

        BrokerOrderResponse response = broker.placeOrder(sellRequest);
        if (response == null || !response.isAccepted() || response.getBrokerOrderId() == null) {
            log.error("❌ [executeSell] Order placement FAILED for {}", symbol);
            return;
        }

        String brokerOrderId = response.getBrokerOrderId();
        String brokerName = broker.getBrokerName();

        PendingOrder po = pendingOrderRepository.create(
                brokerOrderId, symbol, com.satyam.trading2.datamodel.TradeSide.SELL, strategy,
                qty, signal.getEntryPrice(), isHolding, brokerName, route, null);
        pendingOrderRepository.save(po);

        // Safety: mark exit state (also done in onSellCompleted)
        position.setExitProcessed(true);
        Exits.markAsExitedToday(symbol, strategy, position.getAveragePrice());
        dipAccumulatorService.remove(symbol, strategy);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────

    private boolean isOutsideTradingHours() {
        LocalTime now = LocalTime.now();
        return now.isBefore(LocalTime.of(9, 15)) || now.isAfter(LocalTime.of(15, 10));
    }

    private boolean isBuyAllowed() {
        LocalTime now = LocalTime.now();
        return !now.isBefore(LocalTime.of(9, 10)) && now.isBefore(LocalTime.of(15, 0));
    }

    private static int calculateQty(TradeSignal signal) {
        LocalTime now = LocalTime.now();
        if (now.isBefore(LocalTime.of(9, 16))) {
            return Math.max(1, (int) (50000 / signal.getEntryPrice()));
        }
        return Math.max(1, (int) (25000 / signal.getEntryPrice()));
    }

    private boolean isAccumulationAllowed(Position existingPosition, double newPrice) {
        if (existingPosition == null || !existingPosition.isOpen()) {
            return true;
        }
        double previousEntry = existingPosition.getAveragePrice();
        if (existingPosition.getPositionType() == HOLDING) {
            return true;
        }
        return newPrice < previousEntry && newPrice <= previousEntry * 0.99;
    }
}
