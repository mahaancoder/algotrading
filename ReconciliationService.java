package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.*;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.order.OrderCompletionProcessor;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.risk.RiskManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
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
    private final PositionManager positionRepository;
    private final OrderCompletionProcessor completionProcessor;
    private final RiskManager riskManager;
    private final DipAccumulatorService dipAccumulatorService;
    private final MarketContextBuilder marketContextBuilder;
    private final ExecutionEngine executionEngine;

    // ===== FIX: Use ExecutionEngine's locks to prevent race conditions =====
    private Object getSymbolStrategyLock(String symbol, String strategy) {
        return executionEngine.getSymbolStrategyLocks().computeIfAbsent(
            symbol + "_" + strategy,
            k -> new Object()
        );
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

            System.out.println("🔍 [syncBrokerPositions] Broker positions (MIS only): " + brokerPositionsMISOnly.size() +
                             ", Broker holdings (CNC): " + brokerHoldings.size());

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
                            p -> p.getSymbol() + "_" + p.getStrategy(),
                            p -> p,
                            (p1, p2) -> {
                                // This should NOT happen after filtering - log if it does!
                                log.warn("⚠️ [syncBrokerPositions] Unexpected duplicate broker position: {} ({}) - Type: {}, Qty: {}, Avg: {}",
                                        p1.getSymbol(), p1.getStrategy(), p1.getPositionType(),
                                        p1.getTotalQuantity(), p1.getAveragePrice());
                                log.warn("   Duplicate 2: Type: {}, Qty: {}, Avg: {}",
                                        p2.getPositionType(), p2.getTotalQuantity(), p2.getAveragePrice());
                                // Keep first one
                                return p1;
                            }
                    ));

            Map<String, Position> ourMap = ourPositions.stream()
                    .collect(Collectors.toMap(
                            p -> p.getSymbol() + "_" + p.getStrategy(),
                            p -> p,
                            (p1, p2) -> {
                                log.warn("⚠️ [syncBrokerPositions] Duplicate local position: {} ({})",
                                        p1.getSymbol(), p1.getStrategy());
                                return p1;
                            }
                    ));

            // Step 1: Add positions from broker that we don't have, or update mismatched ones
            for (Position brokerPos : allBrokerPositions) {
                String symbol = brokerPos.getSymbol();
                String strategy = brokerPos.getStrategy();
                String key = symbol + "_" + strategy;
                Position ourPos = ourMap.get(key);

                if (ourPos == null) {
                    // Broker has position but we don't - add it
                    System.out.println("⚠️ Broker has position " + symbol + " (" + strategy + ", " +
                                     brokerPos.getPositionType() + ") but we don't. Adding to PositionManager.");
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

            // Step 2: Remove positions we have but broker doesn't
            for (Position ourPos : ourPositions) {
                String symbol = ourPos.getSymbol();
                String strategy = ourPos.getStrategy();
                String key = symbol + "_" + strategy;
                Position brokerPos = brokerMap.get(key);

                if (brokerPos == null) {
                    // We have position but broker doesn't - this position was likely exited
                    System.out.println("⚠️ We have position " + symbol + " (" + strategy + ", " +
                                     ourPos.getPositionType() + ") but broker doesn't. Removing from PositionManager.");
                    positionRepository.removePosition(symbol, ourPos.getStrategy());
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

        // Get all open positions from our PositionManager
        List<Position> openPositions = positionRepository.getAllOpenPositions();

        Map<String, Order> sellOrdersBySymbol = getStringOrderMap();

        for (Position position : openPositions) {
            try {
                String symbol = position.getSymbol();
                int requiredQty = position.getTotalQuantity();

                // ===== FIX: Skip positions that are already exit processed =====
                if (position.isExitProcessed()) {
                    continue;
                }

                // Find SELL order for this symbol (if any)
                // ✅ FIX: Use consistent key format with underscore
                String lookupKey = symbol + "_" + position.getPositionType().name();
                Order existingSellOrder = sellOrdersBySymbol.get(lookupKey);

                if (existingSellOrder == null) {
                    // retry once more to avoid race conditions
                    Thread.sleep(2000);
                    Map<String, Order> sellOrdersBySymbolRetry = getStringOrderMap();
                    Order existingSellOrderRetry = sellOrdersBySymbolRetry.get(lookupKey);
                    // No SELL order exists for this symbol → create one
                    if (existingSellOrderRetry == null) {
                        System.out.println("⚠️ Position " + symbol + " (" + position.getPositionType().name() + ") has NO target SELL order. Creating...");
                        createMissingTargetOrder(position);
                    } else {
                        System.out.println("✅ Found SELL order on retry for " + symbol + " - orderId: " + existingSellOrderRetry.getOrderId());
                    }
                }else {
                    // SELL order exists → check if quantity and price match
                    int existingQty = existingSellOrder.getQuantity();
                    double existingPrice = existingSellOrder.getPrice() != null ? existingSellOrder.getPrice() : 0.0;

                    // Calculate expected target price based on current average price
                    boolean isHolding = position.getPositionType() == Position.PositionType.HOLDING;
                    double averagePrice = position.getAveragePrice();
                    // ✅ Use TargetMultiplierHelper for consistent target pricing
                    double expectedTarget = com.satyam.trading2.helpers.TargetMultiplierHelper.calculateTargetPrice(averagePrice, isHolding);

                    // Check if both quantity AND price match (within a small tolerance)
                    boolean qtyMatches = (existingQty == requiredQty);
                    // ✅ Use 0.1% tolerance for price matching (both up and down)
                    boolean priceMatches = Math.abs(existingPrice - expectedTarget) <= (expectedTarget * 0.001); // 0.1% tolerance

                    if (qtyMatches && priceMatches) {
                        // Both quantity and price match → all good
                        // Update our position's targetOrderId if it's different
                        String brokerOrderId = existingSellOrder.getOrderId();
                        if (!brokerOrderId.equals(position.getTargetOrderId())) {
                            System.out.println("🔄 Updating targetOrderId for"+symbol+" from "+position.getTargetOrderId()+" to "+brokerOrderId);
                            position.setTargetOrderId(brokerOrderId);
                            position.setTargetPlaced(true);
                            positionRepository.updatePosition(position);
                        }
                    } else {
                        // Quantity or price mismatch → update the existing order
                        if (!qtyMatches) {
                            System.out.println("⚠️ Position "+symbol+" has SELL order but qty mismatch: existing="+existingQty+", required="+requiredQty+". Updating...");
                        }
                        if (!priceMatches) {
                            System.out.println("⚠️ Position "+symbol+" has SELL order but price mismatch: existing="+String.format("%.2f", existingPrice)+", expected="+String.format("%.2f", expectedTarget)+" (avgPrice="+String.format("%.2f", averagePrice)+"). Updating...");
                        }
                        updateTargetOrderQuantity(position, existingSellOrder);
                    }
                }

            } catch (Exception e) {
                log.error("❌ Error checking position {}: {}", position.getSymbol(), e.getMessage(), e);
            }
        }
    }

    @NotNull
    private Map<String, Order> getStringOrderMap() {
        List<Order> allOrders = orderServiceV2.fetchOrders();
        // Get all PENDING/OPEN SELL orders from broker
        List<Order> openSellOrders = allOrders.stream()
                .filter(o -> o != null)
                .filter(o -> "SELL".equalsIgnoreCase(o.getTransactionType()))
                .filter(o -> "OPEN".equalsIgnoreCase(o.getStatus()) ||
                             "TRIGGER PENDING".equalsIgnoreCase(o.getStatus()))
                .collect(Collectors.toList());

        log.info("📋 Found {} open SELL orders from broker", openSellOrders.size());

        // Create map by symbol for efficient lookup
        // ✅ FIX: Use product type from order to determine INTRADAY vs HOLDING
        Map<String, Order> sellOrdersBySymbol = openSellOrders.stream()
                .collect(Collectors.toMap(
                    o -> {
                        String product = o.getProduct();
                        String posType = "MIS".equals(product) ? "INTRADAY" : "HOLDING";
                        String key = o.getTradingSymbol() + "_" + posType;
                        log.debug("  Map key: {} -> OrderId: {}", key, o.getOrderId());
                        return key;
                    },
                    o -> o,
                    (o1, o2) -> {
                        log.warn("⚠️ Duplicate SELL order for same symbol+type - keeping orderId: {}, discarding: {}",
                                o1.getOrderId(), o2.getOrderId());
                        return o1;
                    }
                ));

        log.info("📋 Created lookup map with {} unique keys", sellOrdersBySymbol.size());
        return sellOrdersBySymbol;
    }

    /**
     * Update quantity of an existing target SELL order
     */
    private void updateTargetOrderQuantity(Position position, Order existingSellOrder) {
        String symbol = position.getSymbol();
        String strategy = position.getStrategy();

        // ===== FIX: Synchronize on symbol+strategy lock to prevent race with webhook handler =====
        synchronized (getSymbolStrategyLock(symbol, strategy)) {
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
                String newTargetOrderId = orderServiceV2.updateTargetOrder(
                        position.getSymbol(),
                        target,
                        position.getTotalQuantity(),
                        existingSellOrder.getOrderId(), // Cancel this old order
                        isHolding,
                        position.getStrategy()
                );

                if (newTargetOrderId != null) {
                    position.setTargetOrderId(newTargetOrderId);
                    position.setTargetPlaced(true);
                    positionRepository.updatePosition(position);

                    // ✅ FIX: Create PendingOrder for target so webhook can find it
                    PendingOrder targetPO = pendingOrderRepository.create(
                            newTargetOrderId,
                            position.getSymbol(),
                            TradeSide.SELL,
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
        synchronized (getSymbolStrategyLock(symbol, strategy)) {
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

                String newTargetOrderId = orderServiceV2.updateTargetOrder(
                        position.getSymbol(),
                        target,
                        position.getTotalQuantity(),
                        null, // No old order to cancel
                        isHolding,
                        position.getStrategy()
                );

                if (newTargetOrderId != null) {
                    position.setTargetOrderId(newTargetOrderId);
                    position.setTargetPlaced(true);
                    positionRepository.updatePosition(position);

                    // ✅ FIX: Create PendingOrder for target so webhook can find it
                    PendingOrder targetPO = pendingOrderRepository.create(
                            newTargetOrderId,
                            position.getSymbol(),
                            TradeSide.SELL,
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