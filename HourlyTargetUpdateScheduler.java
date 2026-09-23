package com.satyam.trading2.scheduler;

import com.satyam.trading2.datamodel.Order;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.infrastructure.messaging.BroadcastService;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.service.OrderServiceV2;
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
    private final OrderServiceV2 orderServiceV2;
    private final BroadcastService broadcastService;
    private final PendingOrderRepository pendingOrderRepository;
    private final ReconciliationService reconciliationService;
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
        // 🛑 KILL SWITCH CHECK
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [HourlyTargetUpdateScheduler] Skipped - Kill switch is ACTIVE");
            return;
        }

        // ✅ Get current multiplier from helper (time-based)
        double multiplier = com.satyam.trading2.helpers.TargetMultiplierHelper.getCurrentIntradayMultiplier();
        String timeWindow = com.satyam.trading2.helpers.TargetMultiplierHelper.getCurrentTimeWindow();

        System.out.println("🔔 " + timeLabel + " - Updating target orders - " + timeWindow);

        try {
            // ===== STEP 1: Sync broker positions (MIS only) =====
            System.out.println("📊 Step 1: Syncing broker positions...");
            reconciliationService.syncBrokerPositions(false);

            // Get all INTRADAY positions from position manager
            List<Position> intradayPositions = positionManager.getAllOpenPositions().stream()
                    .filter(p -> p.getPositionType() == INTRADAY)
                    .filter(p -> !p.isExitProcessed())
                    .collect(Collectors.toList());

            System.out.println("📊 Found " + intradayPositions.size() + " INTRADAY positions");

            if (intradayPositions.isEmpty()) {
                System.out.println("⏭️ No INTRADAY positions to update - skipping");
                return;
            }

            // ===== STEP 2: Fetch all open MIS SELL orders =====
            System.out.println("📊 Step 2: Fetching all open SELL orders...");
            List<Order> allOrders = orderServiceV2.fetchOrders();
            List<Order> misSellOrders = allOrders.stream()
                    .filter(o -> "SELL".equalsIgnoreCase(o.getTransactionType()))
                    .filter(o -> "MIS".equals(o.getProduct())) // MIS = INTRADAY
                    .filter(o -> "OPEN".equalsIgnoreCase(o.getStatus()) ||
                               "TRIGGER PENDING".equalsIgnoreCase(o.getStatus()))
                    .collect(Collectors.toList());

            System.out.println("📊 Found " + misSellOrders.size() + " open MIS SELL orders");

            // ===== STEP 3: Cancel ALL open MIS SELL orders =====
            System.out.println("📊 Step 3: Cancelling all open MIS SELL orders...");
            int cancelledCount = 0;
            int cancelFailedCount = 0;

            for (Order order : misSellOrders) {
                try {
                    orderServiceV2.cancelOrder(order.getOrderId());
                    System.out.println("✅ Cancelled order " + order.getOrderId() + " for " + order.getTradingSymbol());
                    cancelledCount++;
                    Thread.sleep(100); // Small delay to avoid rate limiting
                } catch (Exception e) {
                    System.err.println("⚠️ Failed to cancel order " + order.getOrderId() + ": " + e.getMessage());
                    cancelFailedCount++;
                }
            }

            System.out.println("📊 Cancelled: " + cancelledCount + ", Failed: " + cancelFailedCount);

            // Wait a moment for cancellations to process
            if (cancelledCount > 0) {
                System.out.println("⏳ Waiting 2 seconds for cancellations to process...");
                Thread.sleep(2000);
            }

            // ===== STEP 4: Create fresh target orders for all MIS positions =====
            System.out.println("📊 Step 4: Creating fresh target orders for all INTRADAY positions...");
            int createdCount = 0;
            int failedCount = 0;

            for (Position position : intradayPositions) {
                try {
                    String symbol = position.getSymbol();
                    double avgPrice = position.getAveragePrice();
                    int qty = position.getTotalQuantity();
                    double newTarget = avgPrice * (1.0 + (0.01 * multiplier));

                    // Place new target order
                    String newOrderId = orderServiceV2.placeOrder(
                            symbol,
                            "SELL",
                            "LIMIT",
                            qty,
                            newTarget,
                            0, // No trigger price for LIMIT orders
                            "MIS",
                            position.getStrategy()
                    );

                    if (newOrderId != null) {
                        // Update position
                        position.setTargetOrderId(newOrderId);
                        position.setTarget(newTarget);
                        position.setTargetPlaced(true);
                        positionManager.updatePosition(position);

                        // Create PendingOrder
                        PendingOrder targetPO = pendingOrderRepository.create(
                                newOrderId,
                                symbol,
                                TradeSide.SELL,
                                position.getStrategy(),
                                qty,
                                newTarget,
                                false // isHolding = false
                        );
                        pendingOrderRepository.save(targetPO);

                        createdCount++;
                        System.out.println("✅ " + symbol + ": Created target @ ₹" + String.format("%.2f", newTarget) +
                                         " (orderId: " + newOrderId + ")");
                    } else {
                        failedCount++;
                        System.err.println("❌ Failed to create target for " + symbol);
                    }

                    Thread.sleep(100); // Small delay to avoid rate limiting

                } catch (Exception e) {
                    failedCount++;
                    System.err.println("❌ Error creating target for " + position.getSymbol() + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }

            // ===== SUMMARY =====
            String summary = String.format(
                "%s - Complete: Cancelled %d old orders, Created %d new targets, %d failed",
                timeLabel, cancelledCount, createdCount, failedCount
            );
            System.out.println("📊 " + summary);
            broadcastService.broadcastLog(summary);

        } catch (Exception e) {
            System.err.println("❌ Error in updateTargetOrdersWithMultiplier: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
