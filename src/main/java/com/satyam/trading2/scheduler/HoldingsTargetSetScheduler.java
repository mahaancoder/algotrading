package com.satyam.trading2.scheduler;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.domain.service.OrderExecutor;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.service.DipAccumulatorService;
import com.satyam.trading2.service.ExecutionEngine;
import com.satyam.trading2.service.OrderServiceV2;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class HoldingsTargetSetScheduler {

    private final PositionManager positionManager;
    private final OrderServiceV2 orderServiceV2;
    private final ExecutionEngine executionEngine;
    private final OrderExecutor orderExecutor;
    private final PendingOrderRepository pendingOrderRepository;
    private final DipAccumulatorService dipAccumulatorService;
    private final com.satyam.trading2.service.KillSwitchService killSwitchService;


    /**
     * First run at 9:05 AM with 1% target
     */
    @Scheduled(cron = "0 05 9 * * MON-FRI")
    public void morningHoldingCheck_905() throws Exception {
        System.out.println("🕐 [9:05 AM] Running Holdings Target Set with 1.0% target");
        morningHoldingCheckWithTarget(1.01); // 1% target
    }

    /**
     * Second run at 9:15 AM with 0.3% target
     */
    @Scheduled(cron = "0 15 9 * * MON-FRI")
    public void morningHoldingCheck_915() throws Exception {
        System.out.println("🕐 [9:15 AM] Running Holdings Target Set with 0.3% target");
        morningHoldingCheckWithTarget(1.003); // 0.3% target
    }

    /**
     * Common method to check holdings and set targets with specified multiplier
     */
    private void morningHoldingCheckWithTarget(double targetMultiplier) throws Exception {

        // 🛑 KILL SWITCH CHECK
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [HoldingsTargetSetScheduler] Skipped - Kill switch is ACTIVE");
            return;
        }

        // Fetch fresh holdings from broker API
        List<Position> brokerHoldings = orderServiceV2.getHoldings();

        if (brokerHoldings.isEmpty()) return;

        // Process each holding from the broker
        for (Position brokerHolding : brokerHoldings) {
            String symbol = brokerHolding.getSymbol();

            // Reconcile with existing in-memory position to get strategy and other metadata
            Position existingPosition = findExistingHolding(symbol);

            // Use existing position if found, otherwise use broker holding with default strategy
            Position holding = existingPosition != null ? existingPosition : brokerHolding;

            // Update quantity and average price from broker (source of truth)
            holding.setTotalQuantity(brokerHolding.getTotalQuantity());
            holding.setAveragePrice(brokerHolding.getAveragePrice());

            // ===== PRESERVE METADATA: If using broker holding, preserve entryType that was enriched in OrderServiceV2 =====
            if (existingPosition == null && brokerHolding.getEntryType() != null) {
                // brokerHolding was already enriched with metadata in OrderServiceV2.getHoldings()
                holding.setEntryType(brokerHolding.getEntryType());
                holding.setConvertedToHolding(brokerHolding.isConvertedToHolding());
                holding.setConversionTime(brokerHolding.getConversionTime());
            }

            // Update in position manager
            positionManager.addPosition(holding);

            // Place target order with specified multiplier
            placeTargetForHolding(holding, targetMultiplier);

        }

    }

    /**
     * Find existing holding in position manager by symbol
     * Searches across all strategies for the symbol
     */
    private Position findExistingHolding(String symbol) {
        Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);

        // Return the first holding found for this symbol (across any strategy)
        for (Position position : strategyPositions.values()) {
            if (position.getPositionType() == Position.PositionType.HOLDING) {
                return position;
            }
        }

        return null;
    }

    /**
     * Place target order for a holding with default 0.3% target (public method for external use)
     */
    public void placeTargetForHolding(Position holding) {
        placeTargetForHolding(holding, 1.003); // Default 0.3% target
    }

    /**
     * Place target order for a holding with custom target multiplier
     */
    private void placeTargetForHolding(Position holding, double targetMultiplier) {
        try {
            // If target already exists, cancel it first to place new one
            if (holding.getTargetOrderId() != null) {
                System.out.println("🔄 [HoldingsTargetSetScheduler] Canceling existing target for " + holding.getSymbol() +
                                 " to update with new target (multiplier: " + targetMultiplier + ")");
                try {
                    orderServiceV2.cancelOrder(holding.getTargetOrderId());
                    holding.setTargetOrderId(null);
                    holding.setTargetPlaced(false);
                } catch (Exception e) {
                    System.out.println("⚠️ [HoldingsTargetSetScheduler] Failed to cancel existing target: " + e.getMessage());
                    // Continue anyway - broker might have already cancelled it
                }
            }

            double target = holding.getAveragePrice() * targetMultiplier;
            double targetPercent = (targetMultiplier - 1.0) * 100;

            System.out.println("📊 [HoldingsTargetSetScheduler] Setting target for " + holding.getSymbol() +
                             " @ ₹" + String.format("%.2f", target) +
                             " (" + String.format("%.1f", targetPercent) + "% above avg price ₹" +
                             String.format("%.2f", holding.getAveragePrice()) + ")");

            // ===== CIRCUIT LIMIT CHECK =====
            if (!com.satyam.trading2.helpers.CircuitLimitChecker.isSpecificTargetWithinCircuitLimits(holding.getSymbol(), target)) {
                System.out.println("❌ [HoldingsTargetSetScheduler] SKIPPED placing target for " + holding.getSymbol() +
                                 " - target would hit upper circuit limit. Target: ₹" + String.format("%.2f", target));
                return;
            }
            Thread.sleep(100);
            String orderId = orderExecutor.placeTargetOrder(
                    holding.getSymbol(),
                    target,
                    holding.getTotalQuantity(),
                    null,
                    true // isHolding = true
                    ,holding.getStrategy()
            );

            // ✅ FIX: Add target order to PendingOrderRepository so webhooks can be processed
            if (orderId != null) {
                PendingOrder po = pendingOrderRepository.create(
                        orderId,
                        holding.getSymbol(),
                        TradeSide.SELL,
                        holding.getStrategy(),
                        holding.getTotalQuantity(),
                        target,
                        true // isHolding = true
                );
                pendingOrderRepository.save(po);

                // Update the holding object
                holding.setTargetOrderId(orderId);
                holding.setTargetPlaced(true);

                // ✅ CRITICAL: Update the Position in PositionManager
                positionManager.updatePosition(holding);

                dipAccumulatorService.remove(holding.getSymbol(), holding.getStrategy());

                System.out.println("✅ [HoldingsTargetSetScheduler] Target placed successfully for " + holding.getSymbol() +
                                 " with orderId: " + orderId);
            }
        } catch (Exception e) {
            System.out.println("❌ [HoldingsTargetSetScheduler] Exception placing target for " + holding.getSymbol() +
                             ": " + e.getMessage());
        }
    }

}
