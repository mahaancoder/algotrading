package com.satyam.trading2.order;

import com.satyam.trading2.application.dto.PositionDto;
import com.satyam.trading2.datamodel.*;
import com.satyam.trading2.domain.service.PnLCalculator;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.helpers.LatestPriceHelper;
import com.satyam.trading2.repository.HoldingMetadataRepository;
import com.satyam.trading2.risk.Exits;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.infrastructure.messaging.BroadcastService;
import com.satyam.trading2.service.DipAccumulatorService;
import com.satyam.trading2.service.ExecutionEngine;
import com.satyam.trading2.service.OrderServiceV2;
import com.satyam.trading2.service.TradeJournalService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.satyam.trading2.datamodel.Position.PositionType.HOLDING;

@Component
@RequiredArgsConstructor
public class OrderCompletionProcessor {

    private final PositionManager positionManager;
    private final PendingOrderRepository pendingOrderRepository;
    private final OrderServiceV2 orderServiceV2;
    private final DipAccumulatorService dipAccumulatorService;
    private final ExecutionEngine executionEngine;
    private final BroadcastService broadcastService;
    private final TradeJournalService tradeJournalService;
    private final PnLCalculator pnlCalculator;
    private final RiskManager riskManager;
    private final LatestPriceHelper latestPriceHelper;
    private final com.satyam.trading2.service.EntryTypeAnalyticsService entryTypeAnalytics;
    private final HoldingMetadataRepository holdingMetadataRepository;
    private final com.satyam.trading2.service.MarketContextBuilder marketContextBuilder;
    private final com.satyam.trading2.service.TradeMetricsCollector tradeMetricsCollector;
    private final com.satyam.trading2.broker.BrokerRouter brokerRouter;
    private final BrokerOrderRouter brokerOrderRouter;


    // Async executor for non-blocking broadcasts
    private final ExecutorService broadcastExecutor = Executors.newFixedThreadPool(
            2,
            r -> {
                Thread t = new Thread(r);
                t.setName("broadcast-worker");
                t.setDaemon(true); // Don't block JVM shutdown
                return t;
            }
    );

    // Use the same symbol lock as BUY execution, SELL lifecycle, and schedulers.
    private Object getSymbolStrategyLock(String symbol, String strategy) {
        return SellLifecycleLockRegistry.lockFor(
                symbol, strategy, "KITE", Position.PositionType.INTRADAY);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Broker-aware sell/target placement.
    //
    // The buy for a Shoonya/INDstocks position is routed to that broker, so the
    // corresponding target/exit sell must go to the SAME broker — not Kite.
    // OrderServiceV2 is Kite-only, so KITE positions keep using it (preserving
    // its update-target dedup logic); non-KITE positions route through
    // BrokerRouter so the sell lands at the owning broker AND its fill event
    // matches the PendingOrder keyed by (broker, brokerOrderId).
    // ─────────────────────────────────────────────────────────────────────

    private com.satyam.trading2.broker.ExecutionRoute routeOf(Position position) {
        if (position.getExecutionRoute() != null) return position.getExecutionRoute();
        if (position.getBroker() != null) {
            try {
                return com.satyam.trading2.broker.ExecutionRoute.valueOf(
                        position.getBroker().trim().toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException ignored) {
                // Legacy/unknown broker labels use the backward-compatible default.
            }
        }
        return com.satyam.trading2.broker.ExecutionRoute.KITE;
    }

    /** Cancel an order at the broker that owns the position. */
    private void cancelViaBroker(Position position, String orderId) {
        brokerOrderRouter.cancelViaBroker(position, orderId);
    }

    /**
     * Place/replace a TARGET sell. KITE keeps updateTargetOrder (which cancels the
     * prior target and dedups holdings); non-KITE cancels the prior target then
     * places a fresh LIMIT SELL at the owning broker.
     * @return broker order id, or null on failure.
     */
    private String placeTargetSell(Position position, String symbol, double price, int qty,
                                   String oldTargetOrderId, boolean isHolding, String strategy) {
        return brokerOrderRouter.placeTargetSell(
                position, symbol, price, qty, oldTargetOrderId, isHolding, strategy);
    }

    /**
     * Place a plain LIMIT SELL (no cancel-and-replace). Used for the CNC quick-exit
     * order and the post-fill re-placed holding sell. KITE uses OrderServiceV2;
     * non-KITE routes to the owning broker.
     * @return broker order id, or null on failure.
     */
    private String placeLimitSell(Position position, String symbol, double price, int qty,
                                  boolean isHolding, String strategy) {
        // All completion-triggered exits use the same broker-symbol lock and
        // quantity-reservation guard as scheduler/manual SELL paths.
        return brokerOrderRouter.placeLimitSell(position, symbol, price, qty, isHolding, strategy);
    }

    /** Create a SELL PendingOrder tagged with the position's broker + route so the
     *  fill event is matched by (broker, brokerOrderId). KITE positions record KITE. */
    private PendingOrder createSellPending(Position position, String orderId, String symbol,
                                           String strategy, int qty, double price, boolean isHolding) {
        com.satyam.trading2.broker.ExecutionRoute route = routeOf(position);
        String broker = position.getBroker() != null ? position.getBroker() : route.name();
        return pendingOrderRepository.create(orderId, symbol, TradeSide.SELL, strategy,
                qty, price, isHolding, broker, route, null);
    }

    public void onBuyCompleted(PendingOrder pendingOrder) throws IOException {
        try {
            String orderId = pendingOrder.getOrderId();
//            System.out.println("✅ [OrderCompletionProcessor] onBuyCompleted " + pendingOrder.getSymbol()
//                    + " " + pendingOrder.getStrategy() + " broker=" + pendingOrder.getBroker()
//                    + " route=" + pendingOrder.getExecutionRoute() + " qty=" + pendingOrder.getFilledQty()
//                    + " @ ₹" + String.format("%.2f", pendingOrder.getAvgPrice()));
            if(!pendingOrder.isCompleted()) {
                System.out.println("⚠️ [OrderCompletionProcessor] Order not completed, exiting onBuyCompleted: " + orderId);
                return;
            }

//        1. FIND POSITION
            // For holdings, find by symbol only (can only have 1 holding per symbol)
            // For intraday, find by symbol + strategy (can have multiple strategies for same symbol)
            Position position;
            if (pendingOrder.getPositionType() == Position.PositionType.HOLDING) {
                // Broker-aware: same symbol can be held at multiple brokers as
                // separate rows. Accumulate into this broker's holding only.
                String lookupBroker = pendingOrder.getBroker() != null ? pendingOrder.getBroker() : "KITE";
                position = positionManager.findHoldingBySymbolAndBroker(pendingOrder.getSymbol(), lookupBroker);
                System.out.println("🔍 [OrderCompletionProcessor] Looking for HOLDING: " + pendingOrder.getSymbol() +
                                 " broker=" + lookupBroker + " - Found: " + (position != null));
            } else {
                // Broker-aware lookup: a Shoonya/INDSTOCKS buy must find (and accumulate into)
                // its own position, not the KITE-keyed one. The 2-arg overload defaults to KITE.
                String lookupBroker = pendingOrder.getBroker() != null ? pendingOrder.getBroker() : "KITE";
                position = positionManager.findBySymbolAndStrategy(
                        pendingOrder.getSymbol(), pendingOrder.getStrategy(), lookupBroker);
            }

            if(position == null) {
                System.out.println("Position not found, creating new position");
                // Create with broker dimension from the pending order
                String broker = pendingOrder.getBroker() != null ? pendingOrder.getBroker() : "KITE";
                com.satyam.trading2.broker.ExecutionRoute route = pendingOrder.getExecutionRoute() != null
                        ? pendingOrder.getExecutionRoute() : com.satyam.trading2.broker.ExecutionRoute.KITE;
                position = positionManager.createPosition(pendingOrder.getSymbol(), pendingOrder.getStrategy(),
                        pendingOrder.getPositionType(), broker, route);
                // Set entry type for new positions from pending order
                if (pendingOrder.getEntryType() != null) {
                    position.setEntryType(pendingOrder.getEntryType());
                }
            }

// Serialize BUY-fill position updates with the same lock used by all SELL lifecycle paths.
            synchronized (SellLifecycleLockRegistry.lockFor(position)) {
            //        2. IDEMPOTENCY
            if(position.getBuyOrderIds().contains(orderId)) {
                System.out.println("⚠️ Order already processed (idempotency check), exiting");
                return;
            }

//      3. UPDATE POSITION
            int oldQty = position.getTotalQuantity();
            double oldAvg = position.getAveragePrice();

            int newQty = oldQty + pendingOrder.getFilledQty();
            double newAvg = ((oldAvg * oldQty) + (pendingOrder.getAvgPrice() * pendingOrder.getFilledQty())) / newQty;

            position.setTotalQuantity(newQty);
            position.setAveragePrice(newAvg);
            position.getBuyOrderIds().add(orderId);

            // Log accumulation for debugging
            if (oldQty > 0) {
                System.out.println("📈 [OrderCompletionProcessor] ACCUMULATION for " + pendingOrder.getSymbol() +
                                 ": Old: " + oldQty + " @ ₹" + String.format("%.2f", oldAvg) +
                                 " + New: " + pendingOrder.getFilledQty() + " @ ₹" + String.format("%.2f", pendingOrder.getAvgPrice()) +
                                 " = Total: " + newQty + " @ ₹" + String.format("%.2f", newAvg) +
                                 " (Old Target OrderId: " + position.getTargetOrderId() + ")");
            }

            // Calculate actual capital used for this fill
            double actualCapital = pendingOrder.getAvgPrice() * pendingOrder.getFilledQty();

            // ===== ANALYTICS: Record completed buy order =====
            if (position.getEntryType() != null) {
                entryTypeAnalytics.recordBuyOrder(position.getEntryType());
            }

//      4. TARGET ORDER MANAGEMENT
            boolean isHolding = position.getPositionType() == Position.PositionType.HOLDING;

            if (isHolding) {
                // ===== CNC / HOLDING: Dual sell-order strategy =====
                //
                // Sell Order A ("holding target"):
                //   qty  = total holdings (existing + new buy)
                //   price = blended avg × 1.0003  (0.03% above blended avg)
                //
                // Sell Order B ("quick exit"):
                //   qty  = new buy qty only
                //   price = new buy avg × 1.01    (1% above new buy — same as MIS target)
                //
                // When either fills → cancel the other.

                double blendedAvg = newAvg;  // already computed as weighted avg of old + new
                double holdingTargetPrice = blendedAvg * 1.0003;
                double quickExitPrice = pendingOrder.getAvgPrice() * 1.01;
                int newBuyQty = pendingOrder.getFilledQty();

//                System.out.println("🎯 [OrderCompletionProcessor] CNC dual sell for " + pendingOrder.getSymbol() +
//                                 " | Blended avg: ₹" + String.format("%.2f", blendedAvg) +
//                                 " | Holding target: ₹" + String.format("%.2f", holdingTargetPrice) + " (qty=" + newQty + ")" +
//                                 " | Quick exit: ₹" + String.format("%.2f", quickExitPrice) + " (qty=" + newBuyQty + ")");

                // --- Cancel any existing sell orders for this holding (both primary + secondary) ---
                String oldTargetOrderId = position.getTargetOrderId();
                String oldSecondaryOrderId = position.getSecondaryTargetOrderId();
                if (oldTargetOrderId != null) {
                    try { cancelViaBroker(position, oldTargetOrderId); } catch (Exception ignored) {}
                }
                if (oldSecondaryOrderId != null) {
                    try { cancelViaBroker(position, oldSecondaryOrderId); } catch (Exception ignored) {}
                }

                // --- Sell Order A: holding target (total qty, 0.03% above blended avg) ---
                // If a legacy secondary SELL exists, do not place a new full-quantity
                // primary SELL until that order is reconciled. A cancel request alone is
                // not proof that the secondary order cannot still execute.
                if (oldSecondaryOrderId != null) {
                    System.err.println("🛑 [OrderCompletionProcessor] Not placing a replacement holding SELL for "
                            + pendingOrder.getSymbol() + " while legacy secondary SELL "
                            + oldSecondaryOrderId + " is unresolved.");
                    position.setTargetPlaced(false);
                } else if (com.satyam.trading2.helpers.CircuitLimitChecker.isSpecificTargetWithinCircuitLimits(pendingOrder.getSymbol(), holdingTargetPrice)) {
                    String holdingSellOrderId = placeTargetSell(
                            position, pendingOrder.getSymbol(), holdingTargetPrice, newQty, oldTargetOrderId, true, position.getStrategy());
                    if (holdingSellOrderId != null) {
                        position.setTargetOrderId(holdingSellOrderId);
                        position.setTargetPlaced(true);
                        PendingOrder targetPO = createSellPending(
                                position, holdingSellOrderId, pendingOrder.getSymbol(),
                                pendingOrder.getStrategy(), newQty, holdingTargetPrice, true);
                        pendingOrderRepository.save(targetPO);
                        System.out.println("✅ [OrderCompletionProcessor] Holding target sell placed: " + holdingSellOrderId +
                                         " (qty=" + newQty + ", price=₹" + String.format("%.2f", holdingTargetPrice) + ")");
                    } else {
                        System.err.println("❌ [OrderCompletionProcessor] Holding target sell FAILED for " + pendingOrder.getSymbol());
                        position.setTargetPlaced(false);
                        position.setTargetOrderId(null);
                    }
                } else {
                    System.out.println("🚫 [OrderCompletionProcessor] Holding target price hits circuit limit — skipping sell A");
                    position.setTargetPlaced(false);
                    position.setTargetOrderId(null);
                }

                // SAFETY: Do not place a second, overlapping SELL order for the new
                // buy quantity. The primary holding target already covers the full
                // holding quantity (new + existing shares); a concurrent quick-exit
                // SELL can execute before the primary cancellation is confirmed and
                // sell more shares than are owned.
                // Do not forget a legacy secondary order merely because cancellation
                // was requested. Keep its ID attached until broker reconciliation confirms
                // it is terminal and its fills have been accounted for. The trailing
                // scheduler fails closed while this ID remains associated with the holding.
                if (oldSecondaryOrderId == null) {
                    position.setSecondaryTargetQty(0);
                    position.setSecondaryTargetPlaced(false);
                } else {
                    position.setSecondaryTargetOrderId(oldSecondaryOrderId);
                    position.setSecondaryTargetPlaced(true);
                    System.err.println("🛑 [OrderCompletionProcessor] Legacy secondary SELL "
                            + oldSecondaryOrderId + " remains unresolved for " + pendingOrder.getSymbol()
                            + "; manual/broker reconciliation is required before another SELL is placed.");
                }
                System.out.println("🛡️ [OrderCompletionProcessor] Holding uses one SELL order only for "
                        + pendingOrder.getSymbol() + " (qty=" + newQty + ") for new orders.");

            } else {
                // ===== INTRADAY / MIS: Single target sell order (existing logic) =====
                double target = com.satyam.trading2.helpers.TargetMultiplierHelper.calculateTargetPrice(newAvg, false);
                String timeWindow = com.satyam.trading2.helpers.TargetMultiplierHelper.getCurrentTimeWindow();
//                System.out.println("🎯 [OrderCompletionProcessor] Target for " + pendingOrder.getSymbol()
//                        + " route=" + routeOf(position) + " - Window: " + timeWindow + ", Target: ₹" + String.format("%.2f", target));

                if (!com.satyam.trading2.helpers.CircuitLimitChecker.isSpecificTargetWithinCircuitLimits(pendingOrder.getSymbol(), target)) {
//                    System.out.println("🚫 [OrderCompletionProcessor] SKIPPED placing target for " + pendingOrder.getSymbol()
//                            + " - target would hit upper circuit limit. Target: ₹" + String.format("%.2f", target));
                    position.setTargetPlaced(false);
                    position.setTargetOrderId(null);
                } else {
                    String oldTargetOrderId = position.getTargetOrderId();
                    if (oldTargetOrderId != null) {
//                        System.out.println("🔄 [OrderCompletionProcessor] Updating existing target order " + oldTargetOrderId
//                                + " for " + pendingOrder.getSymbol() + " with new qty=" + newQty + " and target=₹" + String.format("%.2f", target));
                    } else {
//                        System.out.println("🆕 [OrderCompletionProcessor] Creating new target order for " + pendingOrder.getSymbol()
//                                + " with qty=" + newQty + " and target=₹" + String.format("%.2f", target));
                    }

                    String newTargetOrderId = placeTargetSell(position, pendingOrder.getSymbol(), target, newQty, oldTargetOrderId, false, position.getStrategy());
                    if(newTargetOrderId != null) {
                        position.setTargetOrderId(newTargetOrderId);
                        position.setTargetPlaced(true);
                        PendingOrder targetPO = createSellPending(
                                position, newTargetOrderId, pendingOrder.getSymbol(),
                                pendingOrder.getStrategy(), newQty, target, false);
                        pendingOrderRepository.save(targetPO);
//                        System.out.println("✅ [OrderCompletionProcessor] Successfully placed target order " + newTargetOrderId
//                                + " for " + pendingOrder.getSymbol() + " (qty=" + newQty + ", target=₹" + String.format("%.2f", target) + ")");
                    } else {
//                        System.out.println("❌❌❌ CRITICAL: Target order placement FAILED for " + pendingOrder.getSymbol()
//                                + " | Qty: " + newQty + " | Target: ₹" + String.format("%.2f", target));
                        position.setTargetPlaced(false);
                        position.setTargetOrderId(null);
                    }
                }
            }

//      5. SAVE
            positionManager.updatePosition(position);

//      5.5. CONFIRM CAPITAL USAGE (move from "reserved" to "used")
            // This releases the reservation and the position is now counted via PositionManager.
            // Use the broker-aware overload (Decision #3: per-broker capital tracking).
            String poBroker = pendingOrder.getBroker() != null ? pendingOrder.getBroker() : "KITE";
            riskManager.confirmCapitalUsage(pendingOrder.getStrategy(), actualCapital, poBroker);
            // Also release the per-symbol capital reservation
            riskManager.releaseCapitalForSymbol(pendingOrder.getSymbol(), actualCapital);

//      6. MARK ORDER PROCESSED
            pendingOrder.markCompletionProcessed();
            pendingOrder.markCompleted(newAvg, pendingOrder.getFilledQty());
            pendingOrderRepository.save(pendingOrder);

//      7. Broadcasting
            String symbol= pendingOrder.getSymbol();
            if (isHolding) {
                broadcastHoldings(symbol);
            } else {
                broadcastOpenPositions(symbol);
            }

            // Save event to journal
            TradeEvent buyEvent = new TradeEvent();
            buyEvent.setSymbol(symbol);
            buyEvent.setStrategy(pendingOrder.getStrategy());
            buyEvent.setType("BUY");
            buyEvent.setPrice(pendingOrder.getAvgPrice());
            buyEvent.setQty(pendingOrder.getFilledQty());
            buyEvent.setPnl(0);
            buyEvent.setTime(System.currentTimeMillis());
            buyEvent.setEntryType(position.getEntryType());
            buyEvent.setBroker(position.getBroker() != null ? position.getBroker() : "KITE");
            tradeJournalService.saveEvent(buyEvent);

            // ===== COLLECT TRADE METRICS =====
            String metricsBroker = pendingOrder.getBroker() != null ? pendingOrder.getBroker() :
                    (position.getBroker() != null ? position.getBroker() : "KITE");
            tradeMetricsCollector.collectMetrics(
                symbol,
                pendingOrder.getStrategy(),
                pendingOrder.getAvgPrice(),
                pendingOrder.getFilledQty(),
                pendingOrder.getOrderId(),
                position.getEntryType(),
                isHolding,
                executionEngine.symbolMarketContext.get(symbol),
                metricsBroker
            );

            // Broadcast using type-safe DTO
            String entryType = position.getEntryType() != null ? position.getEntryType().name() : null;
            broadcastService.broadcastBuy(symbol, pendingOrder.getStrategy(), pendingOrder.getAvgPrice(), pendingOrder.getFilledQty(), entryType);

            }

        } catch (Exception e) {
            System.out.println("❌❌❌ EXCEPTION in onBuyCompleted for " + pendingOrder.getSymbol()
                    + " " + pendingOrder.getStrategy() + " (orderId=" + pendingOrder.getOrderId()
                    + "): " + e.getMessage());
            e.printStackTrace();
            throw e; // Re-throw so caller knows it failed
        }
    }

    public void onSellCompleted(PendingOrder pendingOrder) throws IOException {
        try {
            String sellOrderId = pendingOrder.getOrderId();
            if(pendingOrder.isCompletionProcessed()) {
                System.out.println("⚠️ Sell order already processed (idempotency), exiting");
                return;
            }

            String symbol = pendingOrder.getSymbol();
            String strategy = pendingOrder.getStrategy();

            // ===== Find position by primary OR secondary target order ID =====
            Position position = positionManager.findByTargetOrderId(sellOrderId);
            boolean isSecondarySell = false;
            if (position == null) {
                position = positionManager.findBySecondaryTargetOrderId(sellOrderId);
                isSecondarySell = (position != null);
            }
            if(position==null) {
                System.out.println("⚠️ Position not found for sell order ID: " + sellOrderId + ", exiting");
                return;
            }

            // Serialize the entire fill/cancel/position-update lifecycle against all schedulers.
            synchronized (SellLifecycleLockRegistry.lockFor(position)) {
            // ===== LEGACY DUAL SELL: confirm the other order is cancelled with zero fills =====
            boolean otherSellCancelled = true;
            String unresolvedOtherSellId = null;
            if (position.getTargetOrderId() != null && position.getSecondaryTargetOrderId() != null) {
                String otherOrderId = isSecondarySell
                        ? position.getTargetOrderId()
                        : position.getSecondaryTargetOrderId();
                otherSellCancelled = brokerOrderRouter.cancelAndConfirmSell(position, otherOrderId, 2500L);
                if (!otherSellCancelled) {
                    unresolvedOtherSellId = otherOrderId;
                    System.err.println("🚨 [OrderCompletionProcessor] SELL fill for " + symbol
                            + " raced with cancellation of other SELL " + otherOrderId
                            + ". No replacement SELL will be placed; position requires immediate broker reconciliation.");
                } else if (isSecondarySell) {
                    position.setTargetOrderId(null);
                    position.setTargetPlaced(false);
                } else {
                    position.setSecondaryTargetOrderId(null);
                    position.setSecondaryTargetQty(0);
                    position.setSecondaryTargetPlaced(false);
                }
            }

            // Declare variables outside synchronized block so they're accessible later
            int soldQty = pendingOrder.getFilledQty();
            double sellPrice = pendingOrder.getAvgPrice();
            double buyPrice = position.getAveragePrice();
            double pnl = (sellPrice - buyPrice) * soldQty;

            // ===== FIX: Synchronize on symbol+strategy lock to prevent race with scheduler =====
            synchronized (SellLifecycleLockRegistry.lockFor(position)) {
                // Double-check exit status after acquiring lock
                if(position.isExitProcessed()){
                    System.out.println("⚠️ Position already exited (after lock acquisition), exiting");
                    return;
                }
            position.setRealizedPnl(pnl);
            position.setUpdatedAt(System.currentTimeMillis());
            boolean fullExit = position.getTotalQuantity() == soldQty;
            if (fullExit) {
                position.setOpen(false);
                position.setExitProcessed(true);
                if (otherSellCancelled) {
                    positionManager.updatePosition(position);
                    // Broker-aware removal: use the position's broker dimension.
                    String posBroker = position.getBroker() != null ? position.getBroker() : "KITE";
                    positionManager.removePosition(symbol, strategy, posBroker);
                } else {
                    // Keep a tombstone with the unresolved order IDs so a later broker
                    // event/reconciliation can still find it. Never place a replacement.
                    if (unresolvedOtherSellId != null && isSecondarySell) {
                        position.setTargetOrderId(unresolvedOtherSellId);
                    } else if (unresolvedOtherSellId != null) {
                        position.setSecondaryTargetOrderId(unresolvedOtherSellId);
                        position.setSecondaryTargetPlaced(true);
                    }
                    positionManager.updatePosition(position);
                }
            } else {
                // Partial exit: keep the remaining long position active. If the other
                // SELL cannot be confirmed cancelled, retain its ID and do not replace it.
                position.setTotalQuantity(position.getTotalQuantity() - soldQty);
                position.setExitProcessed(false);
                positionManager.updatePosition(position);
            }

                dipAccumulatorService.remove(symbol, strategy);
                pendingOrder.markCompletionProcessed();
                // A terminal order can have a partial cumulative fill. Its executed
                // shares have now been applied to Position, but the broker order itself
                // remains CANCELLED/REJECTED rather than COMPLETE.
                if (pendingOrder.getFilledQty() < pendingOrder.getRequestQty()) {
                    pendingOrder.markCancelled();
                }
                pendingOrderRepository.save(pendingOrder);
                String tradeBroker = position.getBroker() != null ? position.getBroker() : "KITE";
                pnlCalculator.recordTrade(pnl, strategy, tradeBroker);
                // Decision #1: price-aware exit — record buy price so re-entry
                // is allowed only at a lower price (within or across brokers).
                Exits.markAsExitedToday(symbol, strategy, buyPrice);

                // ===== DUAL SELL: If quick-exit (secondary) filled, re-place a sell
                // for the remaining holdings (since we cancelled the holding target).
                // The remaining qty = total - secondaryTargetQty.
                if (isSecondarySell && position.isOpen() && otherSellCancelled) {
                    int remainingQty = position.getTotalQuantity(); // already reduced by partial exit
                    if (remainingQty > 0) {
                        double remainingAvg = position.getAveragePrice();
                        double newHoldingTarget = remainingAvg * 1.0003; // 0.03% above remaining avg
                        System.out.println("🔄 [OrderCompletionProcessor] Re-placing holding sell for " + symbol +
                                         " (remaining qty=" + remainingQty + ", target=₹" + String.format("%.2f", newHoldingTarget) + ")");
                        String newSellOrderId = placeLimitSell(
                                position, symbol, newHoldingTarget, remainingQty, true, strategy);
                        if (newSellOrderId != null) {
                            position.setTargetOrderId(newSellOrderId);
                            position.setTargetPlaced(true);
                            position.setSecondaryTargetOrderId(null);
                            position.setSecondaryTargetPlaced(false);
                            positionManager.updatePosition(position);
                            PendingOrder newTargetPO = createSellPending(
                                    position, newSellOrderId, symbol, strategy,
                                    remainingQty, newHoldingTarget, true);
                            pendingOrderRepository.save(newTargetPO);
                            System.out.println("✅ [OrderCompletionProcessor] Re-placed holding sell: " + newSellOrderId);
                        }
                    }
                } else {
                    // Holding target (primary) filled or single sell — clear secondary
                    position.setSecondaryTargetOrderId(null);
                    position.setSecondaryTargetPlaced(false);
                    positionManager.updatePosition(position);
                }
            } // End synchronized block - critical section complete

            long timestamp = System.currentTimeMillis();

            // ===== ANALYTICS: Calculate holding duration if position was converted =====
            Long holdingDuration = null;
            if (position.isConvertedToHolding() && position.getConversionTime() != null) {
                // Duration in days from conversion to exit
                holdingDuration = (timestamp - position.getConversionTime()) / (1000L * 60 * 60 * 24);
            }

            TradeRecord record = new TradeRecord(
                symbol,
                strategy,
                buyPrice,
                sellPrice,
                soldQty,
                pnl,
                timestamp,
                position.getEntryType() != null ? position.getEntryType() : EntryType.UNKNOWN,
                position.isConvertedToHolding(),
                holdingDuration,
                position.getBroker() != null ? position.getBroker() : "KITE"
            );
            tradeJournalService.saveTrade(record);

            // ===== ANALYTICS: Record closed trade =====
            entryTypeAnalytics.recordClosedTrade(record);

            // ===== UPDATE TRADE METRICS WITH EXIT INFO =====
            tradeMetricsCollector.updateExitMetrics(
                symbol,
                strategy,
                sellPrice,
                pnl,
                    pendingOrder.getPositionType() == HOLDING,
                holdingDuration != null ? holdingDuration.intValue() : null
            );

            String entryType = position.getEntryType() != null ? position.getEntryType().name() : null;
            broadcastService.broadcastSell(symbol, strategy, buyPrice, sellPrice, soldQty, pnl, "morning/evening checks", entryType);
            broadcastService.broadcastStrategyPnL(strategy, pnlCalculator.getStrategyPnL(strategy));
            // Broadcast per-broker P&L so the dashboard Broker P&L section updates live
            for (Map.Entry<String, Double> bpnl : pnlCalculator.getAllBrokerPnLs().entrySet()) {
                broadcastService.broadcastBrokerPnL(bpnl.getKey(), bpnl.getValue());
            }
            broadcastPnL();
            broadcastTodaysPnL();
            broadcastRisk();
            broadcastEntryTypeAnalytics();

            // If it was a holding and fully exited, broadcast removal
            if (position.getPositionType() == Position.PositionType.HOLDING && position.getTotalQuantity() == soldQty) {
                broadcastService.broadcastHoldingRemoved(symbol, strategy);
                System.out.println("📦 Broadcasted HOLDING_REMOVED for " + symbol + " - " + strategy);
            }

            TradeEvent sellEvent = new TradeEvent();
            sellEvent.setSymbol(symbol);
            sellEvent.setStrategy(strategy);
            sellEvent.setType("SELL");
            sellEvent.setPrice(sellPrice);
            sellEvent.setQty(soldQty);
            sellEvent.setPnl(pnl);
            sellEvent.setTime(System.currentTimeMillis());
            sellEvent.setBroker(position.getBroker() != null ? position.getBroker() : "KITE");
            tradeJournalService.saveEvent(sellEvent);

            }

        } catch (Exception e) {
            System.err.println("❌❌❌ EXCEPTION in onSellCompleted ❌❌❌");
            System.err.println("Order ID: " + pendingOrder.getOrderId());
            System.err.println("Symbol: " + pendingOrder.getSymbol());
            System.err.println("Strategy: " + pendingOrder.getStrategy());
            System.err.println("Exception Type: " + e.getClass().getName());
            System.err.println("Exception Message: " + e.getMessage());
            System.err.println("Stack Trace:");
            e.printStackTrace();
            System.err.println("❌❌❌ END EXCEPTION ❌❌❌");
            throw e; // Re-throw so caller knows it failed
        }

    }

    public void broadcastHoldings(String symbol) {
        Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);
        if (strategyPositions.isEmpty()) return;

        for (Position p : strategyPositions.values()) {
            if (p.getPositionType() != HOLDING || !p.isOpen()) continue;
            double last = latestPriceHelper.getLatestPrice(p.getSymbol());
            PositionDto dto = PositionDto.fromHolding(p, last);
            broadcastService.broadcastPosition(dto);
        }
    }

    public void broadcastOpenPositions(String symbol) {
        Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);
        if (strategyPositions.isEmpty()) return;

        // Broadcast positions for the updated symbol
        for (Position p : strategyPositions.values()) {
            if (!p.isOpen() || p.getPositionType() != Position.PositionType.INTRADAY) continue;
            double last = latestPriceHelper.getLatestPrice(p.getSymbol());

            // Use type-safe DTO
            PositionDto dto = PositionDto.fromIntradayPosition(p, last);
            broadcastService.broadcastPosition(dto);
        }
    }

    private void broadcastRisk() {
        CompletableFuture.runAsync(() -> {
            try {
                Map<String, Object> riskData = new HashMap<>();
                riskData.put("current", 0);
                riskData.put("max", 50000);
                riskData.put("pnl", pnlCalculator.getTotalPnL());
                riskData.put("drawdown", pnlCalculator.getMaxDrawdown());
                riskData.put("positions", positionManager.getOpenPositionCount());
                broadcastService.broadcastRisk(riskData);
            } catch (Exception e) {
                System.out.println("Failed to broadcast risk: " + e.getMessage());
                e.printStackTrace();
            }
        }, broadcastExecutor);
    }

    private void broadcastPnL() {
        CompletableFuture.runAsync(() -> {
            try {
                // Total realized P&L is already maintained by PnLCalculator
                // No need to add calculateGain() as it would double-count
                double totalRealizedPnL = pnlCalculator.getTotalPnL();
                broadcastService.broadcastTotalPnL(totalRealizedPnL);
            } catch (Exception e) {
                System.out.println("Failed to broadcast P&L: " + e.getMessage());
                e.printStackTrace();
            }
        }, broadcastExecutor);
    }

    private void broadcastTodaysPnL() {
        CompletableFuture.runAsync(() -> {
            try {
                broadcastService.broadcastTodaysPnL(pnlCalculator.getTodaysPnL());
            } catch (Exception e) {
                System.out.println("Failed to broadcast today's P&L: " + e.getMessage());
                e.printStackTrace();
            }
        }, broadcastExecutor);
    }

    private void broadcastEntryTypeAnalytics() {
        CompletableFuture.runAsync(() -> {
            try {
                com.satyam.trading2.application.dto.EntryTypeAnalyticsDto analyticsDto =
                    com.satyam.trading2.application.dto.EntryTypeAnalyticsDto.from(
                        entryTypeAnalytics.getAnalyticsSummary()
                    );
                broadcastService.broadcastEntryTypeAnalytics(analyticsDto);
            } catch (Exception e) {
                System.out.println("Failed to broadcast entry type analytics: " + e.getMessage());
                e.printStackTrace();
            }
        }, broadcastExecutor);
    }
}