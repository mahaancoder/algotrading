package com.satyam.trading2.scheduler;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.domain.service.OrderExecutor;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.order.BrokerOrderRouter;
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
    private final BrokerOrderRouter brokerOrderRouter;


    /**
     * First run at 9:05 AM with 1% target
     */
    @Scheduled(cron = "0 05 9 * * MON-FRI")
    public void morningHoldingCheck_905() throws Exception {
        System.out.println("🕐 [9:05 AM] Running Holdings Target Set with 1.0% target");
        morningHoldingCheckWithTarget(1.003); // 0.3% target
    }

    /**
     * Second run at 9:15 AM with 0.3% target
     */

 /*   @Scheduled(cron = "0 15 9 * * MON-FRI")
    public void morningHoldingCheck_915() throws Exception {
        System.out.println("🕐 [9:15 AM] Running Holdings Target Set with 0.3% target");
        morningHoldingCheckWithTarget(1.003); // 0.3% target
    }
*/
    /**
     * Common method to check holdings and set targets with specified multiplier
     */
    private void morningHoldingCheckWithTarget(double targetMultiplier) throws Exception {

        // 🛑 KILL SWITCH CHECK
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [HoldingsTargetSetScheduler] Skipped - Kill switch is ACTIVE");
            return;
        }

        // Fetch fresh holdings from the KITE broker API (Kite is the only holdings
        // source; Shoonya/INDstocks holdings are tracked in PositionManager and are
        // handled in the non-KITE loop below).
        List<Position> brokerHoldings = orderServiceV2.getHoldings();

        // Process each KITE holding from the broker
        for (Position brokerHolding : brokerHoldings) {
            String symbol = brokerHolding.getSymbol();

            // Reconcile with existing in-memory KITE position to get strategy and metadata
            Position existingPosition = findExistingHolding(symbol, brokerHolding.getBroker());

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

        // ===== Non-KITE holdings (Shoonya/INDstocks) =====
        // Kite getHoldings() only returns Kite holdings, so process broker holdings
        // tracked in PositionManager separately, routing targets to their owning broker.
        for (Position holding : positionManager.getAllHoldings()) {
            if (brokerOrderRouter.isKite(holding)) {
                continue; // KITE holdings already handled from the broker API above
            }
            placeTargetForHolding(holding, targetMultiplier);
        }

    }

    /**
     * Find existing holding in position manager by symbol at the given broker.
     * The same symbol can be held at multiple brokers as independent rows, so a
     * broker-aware lookup is required to avoid merging into another broker's holding.
     */
    private Position findExistingHolding(String symbol, String broker) {
        return positionManager.findHoldingBySymbolAndBroker(symbol, broker);
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
            // If primary target already exists, cancel it first to place new one
            if (holding.getTargetOrderId() != null) {
                System.out.println("🔄 [HoldingsTargetSetScheduler] Canceling existing target for " + holding.getSymbol() +
                                 " to update with new target (multiplier: " + targetMultiplier + ")");
                try {
                    brokerOrderRouter.cancelViaBroker(holding, holding.getTargetOrderId());
                    holding.setTargetOrderId(null);
                    holding.setTargetPlaced(false);
                } catch (Exception e) {
                    System.out.println("⚠️ [HoldingsTargetSetScheduler] Failed to cancel existing target: " + e.getMessage());
                }
            }
            // Also cancel any existing secondary (quick-exit) sell order
            if (holding.getSecondaryTargetOrderId() != null) {
                System.out.println("🔄 [HoldingsTargetSetScheduler] Canceling existing quick-exit sell for " + holding.getSymbol());
                try {
                    brokerOrderRouter.cancelViaBroker(holding, holding.getSecondaryTargetOrderId());
                    holding.setSecondaryTargetOrderId(null);
                    holding.setSecondaryTargetPlaced(false);
                } catch (Exception e) {
                    System.out.println("⚠️ [HoldingsTargetSetScheduler] Failed to cancel quick-exit sell: " + e.getMessage());
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
            // Route the target sell to the broker that owns this holding. KITE keeps
            // updateTargetOrder (dedup); non-KITE places a fresh LIMIT SELL at that broker.
            String orderId = brokerOrderRouter.placeTargetSell(
                    holding,
                    holding.getSymbol(),
                    target,
                    holding.getTotalQuantity(),
                    null,
                    true, // isHolding = true
                    holding.getStrategy()
            );

            // ✅ FIX: Add target order to PendingOrderRepository so webhooks can be processed
            if (orderId != null) {
                // Broker-tagged PendingOrder so the fill event is matched to the
                // owning broker (KITE/SHOONYA/INDSTOCKS)
                PendingOrder po = brokerOrderRouter.createSellPending(
                        holding,
                        orderId,
                        holding.getSymbol(),
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
