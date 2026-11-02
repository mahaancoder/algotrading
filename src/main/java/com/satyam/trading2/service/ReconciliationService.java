package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.*;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.order.OrderCompletionProcessor;
import com.satyam.trading2.order.BrokerOrderRouter;
import com.satyam.trading2.order.SellLifecycleLockRegistry;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.risk.RiskManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    @Scheduled(fixedDelay = 900000)
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
            syncBrokerPositions(false);

            // Step 3: Ensure every open position has a target SELL order
            ensureTargetOrdersForOpenPositions();

            log.info("✅ Reconciliation cycle complete");
        } catch (Exception e) {
            log.error("Error in reconciliation: {}", e.getMessage(), e);
        }
    }

    public void syncBrokerPositions(boolean setTargetOrderId) {
        try {
            // Fetch actual positions from broker (includes both MIS and CNC day positions)
            List<Position> brokerPositions = orderServiceV2.getPositions();

            // Also fetch holdings (CNC long-term holdings - already merged by broker)
            List<Position> brokerHoldings = orderServiceV2.getHoldings();

            // ===== FIX: Don't merge CNC day positions with holdings - holdings are already merged! =====
            // Filter out CNC positions from brokerPositions to avoid double-counting
            // When you buy CNC, broker returns it in BOTH /positions (day) AND /holdings (merged)
            List<Position> brokerPositionsMISOnly = brokerPositions.stream()
                    .filter(p -> p.getPositionType() == Position.PositionType.INTRADAY) // Only MIS
                    .collect(java.util.stream.Collectors.toList());

//            System.out.println("🔍 [syncBrokerPositions] Broker positions (MIS only): " + brokerPositionsMISOnly.size() +
//                             ", Broker holdings (CNC): " + brokerHoldings.size());

            // Merge MIS positions + Holdings (no overlap now!)
            List<Position> allBrokerPositions = new ArrayList<>();
            allBrokerPositions.addAll(brokerPositionsMISOnly);
            allBrokerPositions.addAll(brokerHoldings);

            // Get our current open positions
            List<Position> ourPositions = positionRepository.getAllOpenPositions();

            // Create maps for efficient lookup by symbol + strategy
            // After filtering out CNC day positions, there should be NO duplicates
            // (MIS positions are separate from Holdings by definition)
            Map<String, Position> brokerMap = allBrokerPositions.stream()
                    .collect(Collectors.toMap(
                            ReconciliationService::posKey,
                            p -> p,
                            (p1, p2) -> {
                                // This should NOT happen after filtering - log if it does!
//                                log.warn("⚠️ [syncBrokerPositions] Unexpected duplicate broker position: {} ({}) - Type: {}, Qty: {}, Avg: {}",
//                                        p1.getSymbol(), p1.getStrategy(), p1.getPositionType(),
//                                        p1.getTotalQuantity(), p1.getAveragePrice());
//                                log.warn("   Duplicate 2: Type: {}, Qty: {}, Avg: {}",
//                                        p2.getPositionType(), p2.getTotalQuantity(), p2.getAveragePrice());
                                // Keep first one
                                return p1;
                            }
                    ));

            Map<String, Position> ourMap = ourPositions.stream()
                    .collect(Collectors.toMap(
                            ReconciliationService::posKey,
                            p -> p,
                            (p1, p2) -> {
                                log.warn("⚠️ [syncBrokerPositions] Duplicate local position: {} ({})",
                                        p1.getSymbol(), p1.getStrategy());
                                return p1;
                            }
                    ));

            // Step 1: Add positions from broker that we don't have, or update mismatched ones.
            // allBrokerPositions come from the Kite API, so they are KITE-owned; keying by
            // broker ensures we never collide with a same-symbol Shoonya/INDstocks position.
            for (Position brokerPos : allBrokerPositions) {
                String symbol = brokerPos.getSymbol();
                String strategy = brokerPos.getStrategy();
                String key = posKey(brokerPos);
                Position ourPos = ourMap.get(key);

                if (ourPos == null) {
                    // Broker has position but we don't - add it
//                    System.out.println("⚠️ Broker has position " + symbol + " (" + strategy + ", " +
//                                     brokerPos.getPositionType() + ") but we don't. Adding to PositionManager.");
                    positionRepository.addPosition(brokerPos);
                } else {
                    // Position exists in both - verify quantities match
                    if (ourPos.getTotalQuantity() != brokerPos.getTotalQuantity() ||
                        Math.abs(ourPos.getAveragePrice() - brokerPos.getAveragePrice()) > 0.01) {

                        System.out.println("⚠️ Position " + symbol + " (" + strategy + ", " + brokerPos.getPositionType() +
                                ") mismatch - Our qty=" + ourPos.getTotalQuantity() +
                                ", avgPrice=" + String.format("%.2f", ourPos.getAveragePrice()) +
                                " vs Broker qty=" + brokerPos.getTotalQuantity() +
                                ", avgPrice=" + String.format("%.2f", brokerPos.getAveragePrice()) + ". Updating.");

                        ourPos.setTotalQuantity(brokerPos.getTotalQuantity());
                        ourPos.setAveragePrice(brokerPos.getAveragePrice());
                        positionRepository.updatePosition(ourPos);
                    }
                }
            }

            // Step 2: Remove positions we have but broker doesn't.
            // ===== MULTI-BROKER SAFETY: only KITE positions are reconciled against the
            // Kite API. Shoonya/INDstocks positions are NEVER removed here — they are
            // absent from the Kite response by definition, not because they were exited.
            for (Position ourPos : ourPositions) {
                if (!isKite(ourPos)) {
                    continue; // never remove non-KITE positions during Kite reconciliation
                }
                String symbol = ourPos.getSymbol();
                String strategy = ourPos.getStrategy();
                String key = posKey(ourPos);
                Position brokerPos = brokerMap.get(key);

                if (brokerPos == null) {
                    // We have position but broker doesn't - this position was likely exited
//                    System.out.println("⚠️ We have position " + symbol + " (" + strategy + ", " +
//                                     ourPos.getPositionType() + ") but broker doesn't. Removing from PositionManager.");
                    positionRepository.removePosition(symbol, ourPos.getStrategy(), brokerOf(ourPos));
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Error syncing broker positions: {}"+ e.getMessage()+ e);
        }
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