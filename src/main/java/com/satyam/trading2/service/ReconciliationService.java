package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.*;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.order.OrderCompletionProcessor;
import com.satyam.trading2.order.BrokerOrderRouter;
import com.satyam.trading2.order.SellLifecycleLockRegistry;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.repository.HoldingMetadataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReconciliationService {

    private final OrderServiceV2 orderServiceV2;
    private final PendingOrderRepository pendingOrderRepository;
    private final BrokerOrderRouter brokerOrderRouter;
    private final PositionManager positionRepository;
    private final OrderCompletionProcessor completionProcessor;
    private final RiskManager riskManager;
    private final DipAccumulatorService dipAccumulatorService;
    private final MarketContextBuilder marketContextBuilder;
    private final HoldingMetadataRepository holdingMetadataRepository;

    // A single transient broker snapshot must never erase a live local position.
    // Counts are reset as soon as the broker reports the position again.
    private final Map<String, Integer> consecutiveMissingSnapshots = new ConcurrentHashMap<>();
    private static final int MISSING_SNAPSHOTS_BEFORE_REMOVAL = 2;

    // ─────────────────────────────────────────────────────────────────────
    // Kite is the ONLY positions/holdings source (getPositions()/getHoldings()
    // hit the Kite API). Reconciliation must therefore only touch KITE positions
    // and must NEVER remove Shoonya/INDstocks positions just because they're
    // absent from the Kite response. These helpers key/scope everything by broker.
    // ─────────────────────────────────────────────────────────────────────
    private static String brokerOf(Position p) {
        return p.getBroker() != null ? p.getBroker() : "KITE";
    }

    private static boolean isKite(Position p) {
        return "KITE".equals(brokerOf(p));
    }

    private static String posKey(Position p) {
        return p.getSymbol() + "_" + p.getStrategy() + "_" + brokerOf(p);
    }

    public void reconcile() {
        // Check if current time is within trading window (9:20 AM to 3:00 PM)
        LocalTime now = LocalTime.now();
        LocalTime startTime = LocalTime.of(9, 20);
        LocalTime endTime = LocalTime.of(15, 0);

        if (now.isBefore(startTime) || now.isAfter(endTime)) {
            return;
        }

        log.info("🔄 Starting reconciliation cycle");
        try {
            // Step 1: Sync broker positions with our PositionManager
            if (!syncBrokerPositions(false)) {
                log.warn("[Reconciliation] Skipping target-order reconciliation because the broker position snapshot was not safely applied.");
                return;
            }

            // Step 3: Ensure every open position has a target SELL order
            ensureTargetOrdersForOpenPositions();

            log.info("✅ Reconciliation cycle complete");
        } catch (Exception e) {
            log.error("Error in reconciliation: {}", e.getMessage(), e);
        }
    }

    /**
     * Applies one serialized, validated Kite position snapshot.
     *
     * Missing positions require two consecutive authoritative snapshots and
     * must have no live or unresolved SELL order before local state is removed.
     * This protects the in-memory position book from transient broker/API
     * inconsistencies and delayed order lifecycle events.
     *
     * @return true only when both positions and holdings snapshots were valid
     */
    public synchronized boolean syncBrokerPositions(boolean setTargetOrderId) {
        try {
            List<Position> brokerPositions = orderServiceV2.getPositions();
            if (brokerPositions == null) {
                log.error("[Reconciliation] Kite positions snapshot unavailable; retaining all local positions.");
                return false;
            }

            List<Position> brokerHoldings = orderServiceV2.getHoldings();
            if (brokerHoldings == null) {
                log.error("[Reconciliation] Kite holdings snapshot unavailable; retaining all local positions.");
                return false;
            }

            // Fetch one strict order-book snapshot for the whole reconciliation cycle.
            // It is needed to settle manually cancelled target orders and to avoid
            // releasing a reservation based on a position snapshot alone.
            List<Order> orderSnapshot = orderServiceV2.fetchOrdersSnapshot();
            if (orderSnapshot == null) {
                log.error("[Reconciliation] Kite order-book snapshot unavailable; retaining local order references and skipping automation.");
                return false;
            }

            List<Position> allBrokerPositions = new ArrayList<>();
            allBrokerPositions.addAll(brokerPositions.stream()
                    .filter(p -> p != null && p.getPositionType() == Position.PositionType.INTRADAY)
                    .collect(Collectors.toList()));
            allBrokerPositions.addAll(brokerHoldings);

            // Include tombstones: broker-confirmed inventory must be able to restore
            // rows hidden from ordinary open-position views after a local completion.
            List<Position> ourPositions = positionRepository.getAllPositions().stream()
                    .filter(java.util.Objects::nonNull)
                    .filter(ReconciliationService::isKite)
                    .collect(Collectors.toList());

            for (Position brokerPos : allBrokerPositions) {
                List<Position> localMatches = ourPositions.stream()
                        .filter(p -> p.getSymbol().equalsIgnoreCase(brokerPos.getSymbol()))
                        .filter(p -> p.getPositionType() == brokerPos.getPositionType())
                        .collect(Collectors.toList());

                if (localMatches.size() == 1) {
                    Position local = localMatches.get(0);
                    String key = missingKey(local);
                    consecutiveMissingSnapshots.remove(key);
                    synchronized (SellLifecycleLockRegistry.lockFor(local)) {
                        if (local.isExitProcessed()) {
                            // Re-open only because the validated broker snapshot positively
                            // confirms remaining long inventory. Keep recorded SELL IDs.
                            local.setOpen(true);
                            local.setExitProcessed(false);
                            log.warn("[Reconciliation] Restoring tombstoned {} {} with broker-confirmed qty={}.",
                                    local.getSymbol(), local.getPositionType(), brokerPos.getTotalQuantity());
                        }
                        if (local.getTotalQuantity() != brokerPos.getTotalQuantity()
                                || Math.abs(local.getAveragePrice() - brokerPos.getAveragePrice()) > 0.01) {
                            log.warn("[Reconciliation] Updating aggregate Kite inventory for {} {}: local qty={} avg={}, broker qty={} avg={}",
                                    local.getSymbol(), local.getPositionType(), local.getTotalQuantity(),
                                    local.getAveragePrice(), brokerPos.getTotalQuantity(), brokerPos.getAveragePrice());
                            local.setTotalQuantity(brokerPos.getTotalQuantity());
                            local.setAveragePrice(brokerPos.getAveragePrice());
                            positionRepository.updatePosition(local);
                        }
                    }
                } else if (localMatches.isEmpty()) {
                    positionRepository.addPosition(brokerPos);
                    consecutiveMissingSnapshots.remove(missingKey(brokerPos));
                    log.warn("[Reconciliation] Restored broker-reported Kite position {} {} qty={} avg={} with broker fallback strategy={}",
                            brokerPos.getSymbol(), brokerPos.getPositionType(), brokerPos.getTotalQuantity(),
                            brokerPos.getAveragePrice(), brokerPos.getStrategy());
                } else {
                    // Kite exposes one aggregate net quantity per symbol/product, while
                    // this application can hold several strategy allocations for that
                    // same broker position. Attribute broker-confirmed remaining shares
                    // FIFO: oldest strategy allocation consumes the remaining inventory
                    // first. This is deterministic and prevents stale local quantities
                    // from authorizing a SELL after a manual market exit.
                    List<Position> fifo = localMatches.stream()
                            .sorted(java.util.Comparator
                                    .comparingLong((Position p) -> p.getCreatedAt() > 0
                                            ? p.getCreatedAt() : p.getUpdatedAt())
                                    .thenComparing(p -> p.getStrategy() == null ? "" : p.getStrategy()))
                            .collect(Collectors.toList());
                    int remainingBrokerQty = Math.max(0, brokerPos.getTotalQuantity());
                    for (Position local : fifo) {
                        consecutiveMissingSnapshots.remove(missingKey(local));
                        synchronized (SellLifecycleLockRegistry.lockFor(local)) {
                            int allocatedQty = Math.min(Math.max(0, local.getTotalQuantity()), remainingBrokerQty);
                            remainingBrokerQty -= allocatedQty;
                            if (local.getTotalQuantity() != allocatedQty) {
                                log.warn("[Reconciliation][FIFO] {} {} strategy={} localQty={} -> brokerAllocatedQty={} (aggregate Kite qty={})",
                                        local.getSymbol(), local.getPositionType(), local.getStrategy(),
                                        local.getTotalQuantity(), allocatedQty, brokerPos.getTotalQuantity());
                                local.setTotalQuantity(allocatedQty);
                            }
                            if (allocatedQty == 0) {
                                local.setOpen(false);
                                local.setExitProcessed(true);
                                local.setTargetPlaced(false);
                                local.setTargetOrderId(null);
                                local.setSecondaryTargetPlaced(false);
                                local.setSecondaryTargetOrderId(null);
                                local.setSecondaryTargetQty(0);
                            } else {
                                local.setOpen(true);
                                local.setExitProcessed(false);
                            }
                            positionRepository.updatePosition(local);
                        }
                    }
                    if (remainingBrokerQty > 0) {
                        // Broker reports more inventory than the sum of local strategy
                        // allocations. Preserve it as an explicit recovery allocation;
                        // never silently inflate an existing strategy's position.
                        Position recovered = new Position(
                                brokerPos.getSymbol(), remainingBrokerQty, brokerPos.getAveragePrice(),
                                0, true, "MANUAL-RECONCILIATION", null, null, null,
                                brokerPos.getPositionType());
                        recovered.setBroker("KITE");
                        recovered.setExecutionRoute(com.satyam.trading2.broker.ExecutionRoute.KITE);
                        positionRepository.addPosition(recovered);
                        log.error("[Reconciliation][FIFO] Kite reports {} unallocated shares for {}; restored under MANUAL-RECONCILIATION for explicit review.",
                                remainingBrokerQty, brokerPos.getSymbol());
                    }
                }
            }

            // A missing symbol/product is not sufficient evidence to delete a
            // position. First obtain a strict order-book snapshot; if it is
            // unavailable, retain every missing local position.
            List<Position> missingLocals = ourPositions.stream()
                    .filter(local -> allBrokerPositions.stream().noneMatch(p ->
                            p.getSymbol().equalsIgnoreCase(local.getSymbol())
                                    && p.getPositionType() == local.getPositionType()))
                    .collect(Collectors.toList());

            // orderSnapshot was fetched and validated before any local mutation.

            List<PendingOrder> unresolvedOrders = pendingOrderRepository.findOrdersRequiringReconciliation();
            for (Position local : missingLocals) {
                synchronized (SellLifecycleLockRegistry.lockFor(local)) {
                if (local.isExitProcessed()) continue;
                String key = missingKey(local);
                int missingCount = consecutiveMissingSnapshots.merge(key, 1, Integer::sum);
                if (missingCount < MISSING_SNAPSHOTS_BEFORE_REMOVAL) {
                    log.warn("[Reconciliation] Retaining {} {} qty={} after missing broker snapshot {}/{}.",
                            local.getSymbol(), local.getPositionType(), local.getTotalQuantity(),
                            missingCount, MISSING_SNAPSHOTS_BEFORE_REMOVAL);
                    continue;
                }

                boolean activeBrokerSell = orderSnapshot.stream()
                        .filter(java.util.Objects::nonNull)
                        .filter(o -> local.getSymbol().equalsIgnoreCase(o.getTradingSymbol()))
                        .filter(o -> "SELL".equalsIgnoreCase(o.getTransactionType()))
                        .filter(o -> (local.getPositionType() == Position.PositionType.INTRADAY
                                ? "MIS" : "CNC").equalsIgnoreCase(o.getProduct()))
                        .anyMatch(o -> isLiveOrderStatus(o.getStatus()));

                boolean unresolvedSell = unresolvedOrders.stream()
                        .filter(java.util.Objects::nonNull)
                        .filter(po -> po.getSymbol() != null && local.getSymbol().equalsIgnoreCase(po.getSymbol()))
                        .filter(po -> "KITE".equalsIgnoreCase(po.getBroker() == null ? "KITE" : po.getBroker()))
                        .filter(po -> po.getSide() == com.satyam.trading2.datamodel.TradeSide.SELL)
                        .filter(po -> po.getPositionType() == local.getPositionType())
                        .findAny().isPresent();

                if (activeBrokerSell || unresolvedSell) {
                    consecutiveMissingSnapshots.remove(key);
                    log.warn("[Reconciliation] Retaining missing {} {} because SELL lifecycle is still active/unresolved (brokerActiveSell={}, pendingUnresolved={}).",
                            local.getSymbol(), local.getPositionType(), activeBrokerSell, unresolvedSell);
                    continue;
                }

                log.warn("[Reconciliation] Two consecutive valid Kite snapshots and no active/unresolved SELL confirm no remaining {} inventory for {}; removing local strategy position {}.",
                        local.getPositionType(), local.getSymbol(), local.getStrategy());
                positionRepository.removePosition(local.getSymbol(), local.getStrategy(), brokerOf(local));
                if (local.getPositionType() == Position.PositionType.HOLDING) {
                    holdingMetadataRepository.remove(local.getSymbol(), local.getStrategy());
                }
                consecutiveMissingSnapshots.remove(key);
                }
            }

            // Broker position quantities have now been applied. Only now can terminal
            // SELL states be committed and their stale target references released.
            reconcileTerminalKiteSellOrders(orderSnapshot);
            return true;
        } catch (Exception e) {
            log.error("[Reconciliation] Error syncing broker positions; retaining local positions where possible: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Reconcile app-tracked Kite SELL order states against the same validated
     * order snapshot used for position reconciliation. For terminal orders,
     * broker net positions have already been applied, so the reservation can
     * safely be released even when the order was manually cancelled after a
     * partial fill. Unknown/missing order states remain untouched.
     */
    private void reconcileTerminalKiteSellOrders(List<Order> orderSnapshot) throws IOException {
        Map<String, Order> byId = orderSnapshot.stream()
                .filter(java.util.Objects::nonNull)
                .filter(o -> o.getOrderId() != null)
                .collect(Collectors.toMap(Order::getOrderId, o -> o, (first, ignored) -> first));

        for (PendingOrder pending : pendingOrderRepository.findAll()) {
            if (pending == null || pending.getSide() != TradeSide.SELL
                    || !"KITE".equalsIgnoreCase(pending.getBroker() == null ? "KITE" : pending.getBroker())) {
                continue;
            }
            String orderId = pending.getBrokerOrderId() != null
                    ? pending.getBrokerOrderId() : pending.getOrderId();
            Order brokerOrder = orderId == null ? null : byId.get(orderId);
            if (brokerOrder == null || brokerOrder.getStatus() == null
                    || brokerOrder.getFilledQuantity() == null) {
                continue;
            }
            String status = brokerOrder.getStatus().trim().toUpperCase(java.util.Locale.ROOT);
            boolean complete = "COMPLETE".equals(status) || "FILLED".equals(status);
            boolean cancelled = "CANCELLED".equals(status) || "CANCELED".equals(status)
                    || "EXPIRED".equals(status);
            boolean rejected = "REJECTED".equals(status);
            if (!complete && !cancelled && !rejected) {
                continue;
            }

            pending.setFilledQty(Math.max(0, brokerOrder.getFilledQuantity()));
            if (brokerOrder.getAveragePrice() != null && brokerOrder.getAveragePrice() > 0) {
                pending.setAvgPrice(brokerOrder.getAveragePrice());
            }
            pending.setState(complete ? PendingOrderState.COMPLETE
                    : (rejected ? PendingOrderState.REJECTED : PendingOrderState.CANCELLED));
            // Position inventory was reconciled from Kite before this marker is
            // persisted; this prevents replaying a terminal partial fill on restart.
            pending.setCompletionProcessed(true);
            pending.setUpdatedTime(System.currentTimeMillis());
            pendingOrderRepository.save(pending);
        }

        for (Position position : positionRepository.getAllPositions()) {
            if (position == null || !isKite(position)) continue;
            synchronized (SellLifecycleLockRegistry.lockFor(position)) {
                boolean changed = false;
                String primary = position.getTargetOrderId();
                String secondary = position.getSecondaryTargetOrderId();
                if (isConfirmedTerminalSell(primary, position, byId)) {
                    position.setTargetOrderId(null);
                    position.setTargetPlaced(false);
                    changed = true;
                }
                if (isConfirmedTerminalSell(secondary, position, byId)) {
                    position.setSecondaryTargetOrderId(null);
                    position.setSecondaryTargetPlaced(false);
                    position.setSecondaryTargetQty(0);
                    changed = true;
                }
                if (changed) positionRepository.updatePosition(position);
            }
        }
    }

    private boolean isConfirmedTerminalSell(String orderId, Position position, Map<String, Order> byId) {
        if (orderId == null) return false;
        Order order = byId.get(orderId);
        if (order == null || order.getStatus() == null || order.getFilledQuantity() == null) return false;
        if (!position.getSymbol().equalsIgnoreCase(order.getTradingSymbol())
                || !"SELL".equalsIgnoreCase(order.getTransactionType())
                || !(position.getPositionType() == Position.PositionType.HOLDING
                    ? "CNC" : "MIS").equalsIgnoreCase(order.getProduct())) return false;
        String status = order.getStatus().trim().toUpperCase(java.util.Locale.ROOT);
        return "COMPLETE".equals(status) || "FILLED".equals(status)
                || "CANCELLED".equals(status) || "CANCELED".equals(status)
                || "REJECTED".equals(status) || "EXPIRED".equals(status);
    }

    private static String missingKey(Position position) {
        return position.getSymbol().toUpperCase() + "|" + position.getPositionType() + "|" + brokerOf(position);
    }

    private static boolean isLiveOrderStatus(String status) {
        if (status == null || status.trim().isEmpty()) return true;
        String normalized = status.trim().toUpperCase();
        // Fail closed: only an explicitly terminal broker status is safe evidence
        // that an order no longer reserves inventory.
        return !("COMPLETE".equals(normalized)
                || "FILLED".equals(normalized)
                || "REJECTED".equals(normalized)
                || "CANCELLED".equals(normalized)
                || "CANCELED".equals(normalized)
                || "EXPIRED".equals(normalized));
    }

    /**
     * Ensure every OPEN position has a corresponding target SELL order
     * This is the core reconciliation logic: Open Position → must have SELL order
     * Logic: Match by SYMBOL + QUANTITY, not by targetOrderId
     */
    private void ensureTargetOrdersForOpenPositions() {
        log.info("🔍 Checking that all open positions have target SELL orders...");

        List<Position> kiteOpenPositions = positionRepository.getAllOpenPositions().stream()
                .filter(java.util.Objects::nonNull)
                .filter(ReconciliationService::isKite)
                .filter(p -> !p.isExitProcessed())
                .collect(Collectors.toList());

        List<Order> openSellOrders = getOpenSellOrders();
        if (openSellOrders == null) {
            log.error("🛑 Kite order book unavailable; skipping target reconciliation to avoid duplicate SELLs.");
            return;
        }

        for (Position position : kiteOpenPositions) {
            synchronized (SellLifecycleLockRegistry.lockFor(position)) {
                try {
                    if (!position.isOpen() || position.isExitProcessed() || position.getTotalQuantity() <= 0) {
                        continue;
                    }

                    String symbol = position.getSymbol();
                    boolean isHolding = position.getPositionType() == Position.PositionType.HOLDING;
                    String expectedProduct = isHolding ? "CNC" : "MIS";
                    List<Order> candidates = openSellOrders.stream()
                            .filter(java.util.Objects::nonNull)
                            .filter(o -> symbol.equalsIgnoreCase(o.getTradingSymbol()))
                            .filter(o -> expectedProduct.equalsIgnoreCase(o.getProduct()))
                            .collect(Collectors.toList());

                    // Prefer the exact order IDs already recorded on this position.
                    // A primary + secondary target can both be legitimate; any other
                    // same-symbol SELL is untracked and makes automatic replacement unsafe.
                    Order existingSellOrder = null;
                    String recordedTargetId = position.getTargetOrderId();
                    final String recordedPrimaryIdForScan = recordedTargetId;
                    final String recordedSecondaryId = position.getSecondaryTargetOrderId();

                    if (recordedTargetId != null || recordedSecondaryId != null) {
                        boolean untrackedActiveSell = candidates.stream()
                                .filter(java.util.Objects::nonNull)
                                .map(Order::getOrderId)
                                .filter(java.util.Objects::nonNull)
                                .anyMatch(id -> !id.equals(recordedPrimaryIdForScan)
                                        && !id.equals(recordedSecondaryId));
                        if (untrackedActiveSell) {
                            log.error("🛑 {} has active SELLs beyond its recorded primary/secondary IDs; "
                                            + "not adopting, cancelling, or replacing any order automatically.",
                                    symbol);
                            continue;
                        }
                    }

                    // A recorded secondary order is not a substitute for a missing
                    // primary target. Keep the secondary untouched and require explicit
                    // reconciliation rather than accidentally reclassifying its role.
                    if (recordedTargetId == null && recordedSecondaryId != null
                            && candidates.stream().anyMatch(o -> recordedSecondaryId.equals(o.getOrderId()))) {
                        log.warn("🛑 {} has only its recorded secondary SELL active; leaving it intact "
                                + "and skipping automatic primary-target adoption.", symbol);
                        continue;
                    }
                    if (recordedTargetId != null) {
                        final String targetIdToFind = recordedTargetId;
                        existingSellOrder = candidates.stream()
                                .filter(o -> targetIdToFind.equals(o.getOrderId()))
                                .findFirst()
                                .orElse(null);

                        if (existingSellOrder == null) {
                            // A missing active order may have completed or partially
                            // filled before cancellation. Only clear it if the broker
                            // proves terminal zero-fill state.
                            if (!brokerOrderRouter.cancelAndConfirmSell(position, recordedTargetId, 0L)) {
                                log.error("🛑 {} has unresolved target SELL {}; leaving it tracked and skipping reconciliation",
                                        symbol, recordedTargetId);
                                continue;
                            }
                            position.setTargetOrderId(null);
                            position.setTargetPlaced(false);
                            positionRepository.updatePosition(position);
                            recordedTargetId = null;
                        }
                    }

                    long sameSymbolPositionCount = kiteOpenPositions.stream()
                            .filter(p -> symbol.equalsIgnoreCase(p.getSymbol()))
                            .filter(p -> p.getPositionType() == position.getPositionType())
                            .count();

                    if (existingSellOrder == null) {
                        if (candidates.isEmpty() && sameSymbolPositionCount == 1) {
                            log.info("Position {} ({}) has no active SELL; creating a target",
                                    symbol, position.getPositionType());
                            createMissingTargetOrder(position);
                            continue;
                        }
                        if (candidates.isEmpty()) {
                            log.error("🛑 Multiple local {} positions exist for {} with no unambiguous target mapping; "
                                            + "not creating overlapping SELL orders. Reconcile strategy allocations first.",
                                    position.getPositionType(), symbol);
                            continue;
                        }
                        if (candidates.size() == 1 && sameSymbolPositionCount == 1) {
                            // Safe adoption only when both sides are unambiguous.
                            existingSellOrder = candidates.get(0);
                        } else {
                            log.error("🛑 Ambiguous active SELLs for {} {}: {} broker orders for {} local positions. "
                                            + "Not adopting or cancelling any order; reconcile manually.",
                                    symbol, position.getPositionType(), candidates.size(), sameSymbolPositionCount);
                            continue;
                        }
                    }

                    int requiredQty = position.getTotalQuantity();
                    double existingPrice = existingSellOrder.getPrice() != null
                            ? existingSellOrder.getPrice() : 0.0;
                    double expectedTarget = com.satyam.trading2.helpers.TargetMultiplierHelper
                            .calculateTargetPrice(position.getAveragePrice(), isHolding);
                    boolean qtyMatches = existingSellOrder.getQuantity() == requiredQty;
                    boolean priceMatches = Math.abs(existingPrice - expectedTarget) <= (expectedTarget * 0.001);

                    if (qtyMatches && priceMatches) {
                        String brokerOrderId = existingSellOrder.getOrderId();
                        if (!brokerOrderId.equals(position.getTargetOrderId())) {
                            position.setTargetOrderId(brokerOrderId);
                            position.setTargetPlaced(true);
                            positionRepository.updatePosition(position);
                        }
                    } else {
                        updateTargetOrderQuantity(position, existingSellOrder);
                    }
                } catch (Exception e) {
                    log.error("❌ Error reconciling target for {}: {}", position.getSymbol(), e.getMessage(), e);
                }
            }
        }
    }

    private List<Order> getOpenSellOrders() {
        List<Order> allOrders = orderServiceV2.fetchOrders();
        if (allOrders == null) return null;
        return allOrders.stream()
                .filter(java.util.Objects::nonNull)
                .filter(o -> "SELL".equalsIgnoreCase(o.getTransactionType()))
                .filter(o -> "OPEN".equalsIgnoreCase(o.getStatus())
                        || "TRIGGER PENDING".equalsIgnoreCase(o.getStatus())
                        || "PARTIALLY FILLED".equalsIgnoreCase(o.getStatus()))
                .collect(Collectors.toList());
    }

    /**
     * Update quantity of an existing target SELL order
     */
    private void updateTargetOrderQuantity(Position position, Order existingSellOrder) {
        String symbol = position.getSymbol();
        String strategy = position.getStrategy();

        // ===== FIX: Synchronize on symbol+strategy lock to prevent race with webhook handler =====
        synchronized (SellLifecycleLockRegistry.lockFor(position)) {
            try {
                // ===== FIX: Double-check exit status after acquiring lock =====
                if (position.isExitProcessed()) {
                    System.out.println("⏭️ [ReconciliationService] Skipping " + symbol + " - already exit processed");
                    return;
                }

                boolean isHolding = position.getPositionType() == Position.PositionType.HOLDING;
                double averagePrice = position.getAveragePrice();
                // ✅ Use TargetMultiplierHelper for consistent target pricing
                double target = com.satyam.trading2.helpers.TargetMultiplierHelper.calculateTargetPrice(averagePrice, isHolding);

                // Cancel old order and place new one with updated quantity
                String newTargetOrderId = brokerOrderRouter.placeTargetSell(
                        position,
                        position.getSymbol(),
                        target,
                        position.getTotalQuantity(),
                        existingSellOrder.getOrderId(),
                        isHolding,
                        position.getStrategy()
                );

                if (newTargetOrderId != null) {
                    position.setTargetOrderId(newTargetOrderId);
                    position.setTargetPlaced(true);
                    positionRepository.updatePosition(position);

                    // ✅ FIX: Create PendingOrder for target so webhook can find it
                    PendingOrder targetPO = brokerOrderRouter.createSellPending(
                            position,
                            newTargetOrderId,
                            position.getSymbol(),
                            position.getStrategy(),
                            position.getTotalQuantity(),
                            target,
                            isHolding
                    );
                    pendingOrderRepository.save(targetPO);

                    System.out.println("✅ Updated target order for {} from orderId={} qty={} to orderId={} qty={}"+
                            position.getSymbol() + existingSellOrder.getOrderId() + existingSellOrder.getQuantity()+
                            newTargetOrderId+ position.getTotalQuantity());
                } else {
                    System.out.println("❌ Failed to update target order for {}"+ position.getSymbol());
                }

            } catch (Exception e) {
                log.error("❌ Exception updating target order for {}: {}", position.getSymbol(), e.getMessage(), e);
            }
        } // End synchronized block
    }

    /**
     * Create a missing target order for a position
     */
    private void createMissingTargetOrder(Position position) {
        String symbol = position.getSymbol();
        String strategy = position.getStrategy();

        // ===== FIX: Synchronize on symbol+strategy lock to prevent race with webhook handler =====
        synchronized (SellLifecycleLockRegistry.lockFor(position)) {
            try {
                // ===== FIX: Double-check exit status after acquiring lock =====
                if (position.isExitProcessed()) {
                    System.out.println("⏭️ [ReconciliationService] Skipping " + symbol + " - already exit processed");
                    return;
                }

                // ===== FIX: Double-check if target already exists after acquiring lock =====
                if (position.isTargetPlaced() && position.getTargetOrderId() != null) {
                    System.out.println("⏭️ [ReconciliationService] Skipping " + symbol + " - target already placed (orderId: " + position.getTargetOrderId() + ")");
                    return;
                }

                boolean isHolding = position.getPositionType() == Position.PositionType.HOLDING;
                double averagePrice = position.getAveragePrice();
                // ✅ Use TargetMultiplierHelper for consistent target pricing
                double target = com.satyam.trading2.helpers.TargetMultiplierHelper.calculateTargetPrice(averagePrice, isHolding);

                // ===== CIRCUIT LIMIT CHECK =====
                if (!com.satyam.trading2.helpers.CircuitLimitChecker.isSpecificTargetWithinCircuitLimits(position.getSymbol(), target)) {
                    System.out.println("❌ [ReconciliationService] SKIPPED creating target for " + position.getSymbol() +
                                     " - target would hit upper circuit limit. Target: ₹" + String.format("%.2f", target));
                    return;
                }

                String newTargetOrderId = brokerOrderRouter.placeTargetSell(
                        position,
                        position.getSymbol(),
                        target,
                        position.getTotalQuantity(),
                        null,
                        isHolding,
                        position.getStrategy()
                );

                if (newTargetOrderId != null) {
                    position.setTargetOrderId(newTargetOrderId);
                    position.setTargetPlaced(true);
                    positionRepository.updatePosition(position);

                    // ✅ FIX: Create PendingOrder for target so webhook can find it
                    PendingOrder targetPO = brokerOrderRouter.createSellPending(
                            position,
                            newTargetOrderId,
                            position.getSymbol(),
                            position.getStrategy(),
                            position.getTotalQuantity(),
                            target,
                            isHolding
                    );
                    pendingOrderRepository.save(targetPO);

                    System.out.println("✅ Created missing target order for" + position.getSymbol()+": orderId={}," +newTargetOrderId+ "target="+target+" qty={}"+
                            position.getTotalQuantity());
                } else {
                    log.error("❌ Failed to create target order for {}", position.getSymbol());
                }

            } catch (Exception e) {
                log.error("❌ Exception creating target order for {}: {}", position.getSymbol(), e.getMessage(), e);
            }
        } // End synchronized block
    }

}