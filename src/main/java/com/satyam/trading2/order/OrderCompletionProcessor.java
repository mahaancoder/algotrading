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

    // ===== FIX: Helper method to get symbol+strategy lock from ExecutionEngine =====
    private Object getSymbolStrategyLock(String symbol, String strategy) {
        return executionEngine.getSymbolStrategyLocks().computeIfAbsent(
            symbol + "_" + strategy,
            k -> new Object()
        );
    }

    public void onBuyCompleted(PendingOrder pendingOrder) throws IOException {
        try {
            String orderId = pendingOrder.getOrderId();
            if(!pendingOrder.isCompleted()) {
                System.out.println("⚠️ Order not completed, exiting onBuyCompleted");
                return;
            }

//        1. FIND POSITION
            // For holdings, find by symbol only (can only have 1 holding per symbol)
            // For intraday, find by symbol + strategy (can have multiple strategies for same symbol)
            Position position;
            if (pendingOrder.getPositionType() == Position.PositionType.HOLDING) {
                position = positionManager.findHoldingBySymbol(pendingOrder.getSymbol());
                System.out.println("🔍 [OrderCompletionProcessor] Looking for HOLDING: " + pendingOrder.getSymbol() +
                                 " - Found: " + (position != null));
            } else {
                position = positionManager.findBySymbolAndStrategy(pendingOrder.getSymbol(), pendingOrder.getStrategy());
            }

            if(position == null) {
                System.out.println("Position not found, creating new position");
                position = positionManager.createPosition(pendingOrder.getSymbol(), pendingOrder.getStrategy(), pendingOrder.getPositionType());
                // Set entry type for new positions from pending order
                if (pendingOrder.getEntryType() != null) {
                    position.setEntryType(pendingOrder.getEntryType());
                }
            }

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
            // ✅ Use time-based multiplier for consistent target pricing
            double target = com.satyam.trading2.helpers.TargetMultiplierHelper.calculateTargetPrice(newAvg, isHolding);

            if (!isHolding) {
                String timeWindow = com.satyam.trading2.helpers.TargetMultiplierHelper.getCurrentTimeWindow();
                System.out.println("🎯 [OrderCompletionProcessor] Target for " + pendingOrder.getSymbol() +
                                 " - Window: " + timeWindow + ", Target: ₹" + String.format("%.2f", target));
            }

            // ===== CIRCUIT LIMIT CHECK =====
            if (!com.satyam.trading2.helpers.CircuitLimitChecker.isSpecificTargetWithinCircuitLimits(pendingOrder.getSymbol(), target)) {
                System.out.println("🚫 [OrderCompletionProcessor] SKIPPED placing target for " + pendingOrder.getSymbol() +
                                 " - target would hit upper circuit limit. Target: ₹" + String.format("%.2f", target));
                // Mark that target was intentionally not placed due to circuit limit
                position.setTargetPlaced(false);
                position.setTargetOrderId(null);
            } else {
                String oldTargetOrderId = position.getTargetOrderId();

                // Log what we're about to do
                if (oldTargetOrderId != null) {
                    System.out.println("🔄 [OrderCompletionProcessor] Updating existing target order " + oldTargetOrderId +
                                     " for " + pendingOrder.getSymbol() +
                                     " with new qty=" + newQty + " and target=₹" + String.format("%.2f", target));
                } else {
                    System.out.println("🆕 [OrderCompletionProcessor] Creating new target order for " + pendingOrder.getSymbol() +
                                     " with qty=" + newQty + " and target=₹" + String.format("%.2f", target));
                }

                String newTargetOrderId = orderServiceV2.updateTargetOrder(pendingOrder.getSymbol(), target, newQty, oldTargetOrderId, isHolding, position.getStrategy());
                if(newTargetOrderId != null) {
                    position.setTargetOrderId(newTargetOrderId);
                    position.setTargetPlaced(true);

                    // ✅ FIX: Create PendingOrder for target so webhook can find it
                    PendingOrder targetPO = pendingOrderRepository.create(
                            newTargetOrderId,
                            pendingOrder.getSymbol(),
                            TradeSide.SELL,
                            pendingOrder.getStrategy(),
                            newQty,
                            target,
                            isHolding
                    );
                    pendingOrderRepository.save(targetPO);
                    System.out.println("✅ [OrderCompletionProcessor] Successfully placed target order " + newTargetOrderId +
                                     " for " + pendingOrder.getSymbol() + " (qty=" + newQty + ", target=₹" + String.format("%.2f", target) + ")");
                } else {
                    // ❌ CRITICAL: Target order placement FAILED!
                    System.err.println("❌❌❌ CRITICAL: Target order placement FAILED for " + pendingOrder.getSymbol());
                    System.err.println("   Symbol: " + pendingOrder.getSymbol());
                    System.err.println("   Strategy: " + pendingOrder.getStrategy());
                    System.err.println("   Quantity: " + newQty);
                    System.err.println("   Target Price: ₹" + String.format("%.2f", target));
                    System.err.println("   Position Average: ₹" + String.format("%.2f", newAvg));
                    System.err.println("   IS HOLDING: " + isHolding);
                    System.err.println("⚠️ Position will be left WITHOUT a target order!");
                    System.err.println("❌❌❌ Manual intervention may be required!");

                    // Mark that target placement failed
                    position.setTargetPlaced(false);
                    position.setTargetOrderId(null);
                }
            }

//      5. SAVE
            positionManager.updatePosition(position);

//      5.5. CONFIRM CAPITAL USAGE (move from "reserved" to "used")
            // This releases the reservation and the position is now counted via PositionManager
            riskManager.confirmCapitalUsage(pendingOrder.getStrategy(), actualCapital);
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
            tradeJournalService.saveEvent(buyEvent);

            // ===== COLLECT TRADE METRICS =====
            tradeMetricsCollector.collectMetrics(
                symbol,
                pendingOrder.getStrategy(),
                pendingOrder.getAvgPrice(),
                pendingOrder.getFilledQty(),
                pendingOrder.getOrderId(),
                position.getEntryType(),
                isHolding,
                executionEngine.symbolMarketContext.get(symbol)
            );

            // Broadcast using type-safe DTO
            String entryType = position.getEntryType() != null ? position.getEntryType().name() : null;
            broadcastService.broadcastBuy(symbol, pendingOrder.getStrategy(), pendingOrder.getAvgPrice(), pendingOrder.getFilledQty(), entryType);

        } catch (Exception e) {
            System.err.println("❌❌❌ EXCEPTION in onBuyCompleted ❌❌❌");
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

    public void onSellCompleted(PendingOrder pendingOrder) throws IOException {
        try {
            String sellOrderId = pendingOrder.getOrderId();
            if(pendingOrder.isCompletionProcessed()) {
                System.out.println("⚠️ Sell order already processed (idempotency), exiting");
                return;
            }

            String symbol = pendingOrder.getSymbol();
            String strategy = pendingOrder.getStrategy();

            Position position = positionManager.findByTargetOrderId(sellOrderId);
            if(position==null) {
                System.out.println("⚠️ Position not found for target order ID: " + sellOrderId + ", exiting");
                return;
            }

            // Declare variables outside synchronized block so they're accessible later
            int soldQty = pendingOrder.getFilledQty();
            double sellPrice = pendingOrder.getAvgPrice();
            double buyPrice = position.getAveragePrice();
            double pnl = (sellPrice - buyPrice) * soldQty;

            // ===== FIX: Synchronize on symbol+strategy lock to prevent race with scheduler =====
            synchronized (getSymbolStrategyLock(symbol, strategy)) {
                // Double-check exit status after acquiring lock
                if(position.isExitProcessed()){
                    System.out.println("⚠️ Position already exited (after lock acquisition), exiting");
                    return;
                }
            position.setRealizedPnl(pnl);

            position.setExitProcessed(true);
            position.setUpdatedAt(System.currentTimeMillis());
            if (position.getTotalQuantity() == soldQty) {
                // Full exit
                position.setOpen(false);
                positionManager.updatePosition(position);
                positionManager.removePosition(symbol, strategy);
            } else {
                // Partial exit
                position.setTotalQuantity(position.getTotalQuantity() - soldQty);
                positionManager.updatePosition(position);
            }

                dipAccumulatorService.remove(symbol, strategy);
                pendingOrder.markCompletionProcessed();
                pendingOrderRepository.save(pendingOrder);
                pnlCalculator.recordTrade(pnl, strategy);
                Exits.markAsExitedToday(symbol, strategy);
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
                holdingDuration
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
            tradeJournalService.saveEvent(sellEvent);

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