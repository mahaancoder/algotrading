package com.satyam.trading2.scheduler;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.infrastructure.messaging.BroadcastService;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.order.BrokerOrderRouter;
import com.satyam.trading2.order.SellLifecycleLockRegistry;
import com.satyam.trading2.service.ReconciliationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

import static com.satyam.trading2.datamodel.Position.PositionType.HOLDING;
import static com.satyam.trading2.datamodel.Position.PositionType.INTRADAY;

@Service
@RequiredArgsConstructor
public class HourlyTargetUpdateScheduler {

    private final PositionManager positionManager;
    private final BroadcastService broadcastService;
    private final PendingOrderRepository pendingOrderRepository;
    private final ReconciliationService reconciliationService;
    private final BrokerOrderRouter brokerOrderRouter;
    private final com.satyam.trading2.service.KillSwitchService killSwitchService;

    /**
     * Update target orders at 10:15 AM with 0.7% multiplier
     */
    @Scheduled(cron = "0 15 10 * * MON-FRI")
    public void updateTargetsAt1015AM() {
        updateTargetOrders("10:15 AM");
    }

    /**
     * Update target orders at 10:30 AM with 0.6% multiplier
     */
    @Scheduled(cron = "0 30 10 * * MON-FRI")
    public void updateTargetsAt1030AM() {
        updateTargetOrders("10:30 AM");
    }

    /**
     * Update target orders at 11:00 AM with 0.5% multiplier
     */
    @Scheduled(cron = "0 0 11 * * MON-FRI")
    public void updateTargetsAt11AM() {
        updateTargetOrders("11:00 AM");
    }

    /**
     * Update target orders at 12:00 PM with 0.4% multiplier
     */
    @Scheduled(cron = "0 0 12 * * MON-FRI")
    public void updateTargetsAt12PM() {
        updateTargetOrders("12:00 PM");
    }

    /**
     * Update target orders at 1:00 PM with 0.3% multiplier
     */
    @Scheduled(cron = "0 0 13 * * MON-FRI")
    public void updateTargetsAt1PM() {
        updateTargetOrders("1:00 PM");
    }

    /**
     * Update target orders at 2:00 PM with 0.2% multiplier
     */
    @Scheduled(cron = "0 0 14 * * MON-FRI")
    public void updateTargetsAt2PM() {
        updateTargetOrders("2:00 PM");
    }

    /**
     * Update target orders at 3:00 PM with 0.15% multiplier
     */
    @Scheduled(cron = "0 0 15 * * MON-FRI")
    public void updateTargetsAt3PM() {
        updateTargetOrders("3:00 PM");
    }

    /**
     * Simplified approach:
     * 1. Sync broker positions (MIS only)
     * 2. Fetch all open MIS SELL orders
     * 3. Cancel ALL open MIS SELL orders
     * 4. Create fresh target orders for all MIS positions using time-based multiplier
     * 5. Update position manager and pending orders
     */
    private void updateTargetOrders(String timeLabel) {
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [HourlyTargetUpdateScheduler] Skipped - Kill switch is ACTIVE");
            return;
        }

        double multiplier = com.satyam.trading2.helpers.TargetMultiplierHelper.getCurrentIntradayMultiplier();
        String timeWindow = com.satyam.trading2.helpers.TargetMultiplierHelper.getCurrentTimeWindow();
        System.out.println("🔔 " + timeLabel + " - Updating target orders - " + timeWindow);

        try {
            reconciliationService.syncBrokerPositions(false);
            List<Position> intradayPositions = positionManager.getAllOpenPositions().stream()
                    .filter(p -> p.getPositionType() == INTRADAY)
                    .filter(p -> !p.isExitProcessed())
                    .collect(Collectors.toList());

            int createdCount = 0;
            int failedCount = 0;
            int skippedCount = 0;

            for (Position position : intradayPositions) {
                synchronized (SellLifecycleLockRegistry.lockFor(position)) {
                    try {
                        if (killSwitchService.isActive()) break;
                        if (!position.isOpen() || position.isExitProcessed() || position.getTotalQuantity() <= 0) {
                            skippedCount++;
                            continue;
                        }

                        String symbol = position.getSymbol();
                        int qty = position.getTotalQuantity();
                        double newTarget = position.getAveragePrice() * (1.0 + (0.01 * multiplier));
                        String oldOrderId = position.getTargetOrderId();

                        // Each position is updated independently. Never cancel every MIS
                        // SELL in the account: that can race fills and cancel another
                        // strategy's legitimate order.
                        String newOrderId = brokerOrderRouter.placeTargetSell(
                                position, symbol, newTarget, qty, oldOrderId, false, position.getStrategy());

                        if (newOrderId == null) {
                            failedCount++;
                            System.err.println("🛑 [HourlyTargetUpdateScheduler] Target replacement blocked for "
                                    + symbol + " (old order=" + oldOrderId + "). Reconciliation required.");
                            continue;
                        }

                        PendingOrder targetPO = brokerOrderRouter.createSellPending(
                                position, newOrderId, symbol, position.getStrategy(), qty, newTarget, false);
                        try {
                            pendingOrderRepository.save(targetPO);
                        } catch (Exception persistenceError) {
                            // The broker has accepted this SELL. Keep the ID on the position
                            // and stop further replacements so reconciliation can recover it.
                            position.setTargetOrderId(newOrderId);
                            position.setTarget(newTarget);
                            position.setTargetPlaced(true);
                            positionManager.updatePosition(position);
                            throw persistenceError;
                        }

                        position.setTargetOrderId(newOrderId);
                        position.setTarget(newTarget);
                        position.setTargetPlaced(true);
                        positionManager.updatePosition(position);
                        createdCount++;
                        Thread.sleep(100L);
                    } catch (Exception e) {
                        failedCount++;
                        System.err.println("❌ [HourlyTargetUpdateScheduler] Error updating "
                                + position.getSymbol() + ": " + e.getMessage());
                    }
                }
            }

            String summary = String.format(
                    "%s - Complete: Updated %d targets, %d failed, %d skipped",
                    timeLabel, createdCount, failedCount, skippedCount);
            System.out.println("📊 " + summary);
            broadcastService.broadcastLog(summary);
        } catch (Exception e) {
            System.err.println("❌ Error in updateTargetOrdersWithMultiplier: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
