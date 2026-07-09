package com.satyam.trading2.scheduler;

import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.helpers.LatestPriceHelper;
import com.satyam.trading2.service.OrderServiceV2;
import com.satyam.trading2.service.ReconciliationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

import static com.satyam.trading2.datamodel.Position.PositionType.INTRADAY;

/**
 * Scheduler that runs every 15 minutes after 11:00 AM
 * Updates sell orders for profitable INTRADAY positions (profit >= 0.3%) to current LTP
 */
@Service
@RequiredArgsConstructor
public class TrailingStopScheduler {

    private final PositionManager positionManager;
    private final OrderServiceV2 orderServiceV2;
    private final ReconciliationService reconciliationService;
    private final com.satyam.trading2.service.KillSwitchService killSwitchService;

    /**
     * Runs every 15 minutes: 0, 15, 30, 45 minutes past each hour
     * Only executes after 10:00 AM on weekdays (MON-FRI)
     */
    @Scheduled(cron = "0 0/5 * * * MON-FRI")
    public void updateTrailingStops() {
        // 🛑 KILL SWITCH CHECK
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [TrailingStopScheduler] Skipped - Kill switch is ACTIVE");
            return;
        }

        // Only run after 10:00 AM
        LocalTime now = LocalTime.now();
        if (now.isBefore(LocalTime.of(9, 35))) {
            return;
        }

        System.out.println("🔄 [TrailingStop] Running at " + now + " - Checking profitable positions...");

        reconciliationService.syncBrokerPositions(false);
        try {
            // Get all open INTRADAY positions
            List<Position> intradayPositions = positionManager.getAllOpenPositions().stream()
                    .filter(p -> p.getPositionType() == INTRADAY)
                    .filter(p -> !p.isExitProcessed())
                    .collect(Collectors.toList());

            if (intradayPositions.isEmpty()) {
                System.out.println("⏭️ [TrailingStop] No INTRADAY positions found - skipping");
                return;
            }

            System.out.println("📊 [TrailingStop] Found " + intradayPositions.size() + " INTRADAY positions");

            int updatedCount = 0;
            int skippedCount = 0;
            int failedCount = 0;

            for (Position position : intradayPositions) {
                try {
                    String symbol = position.getSymbol();
                    double avgPrice = position.getAveragePrice();
                    int qty = position.getTotalQuantity();
                    
                    // Get current LTP from LatestPriceHelper
                    double ltp = LatestPriceHelper.getLatestPrice(symbol);
                    
                    if (ltp <= 0) {
                        System.out.println("⚠️ [TrailingStop] " + symbol + ": LTP not available, skipping");
                        skippedCount++;
                        continue;
                    }

                    // Calculate profit percentage
                    double profitPercent = ((ltp - avgPrice) / avgPrice) * 100.0;

                    // Only update if profit >= 0.3%
                    if (profitPercent < 0.3) {
                        System.out.println("⏭️ [TrailingStop] " + symbol + ": Profit " + 
                                         String.format("%.2f", profitPercent) + "% < 0.3%, skipping");
                        skippedCount++;
                        continue;
                    }

                    // Update target order to current LTP
                    String oldOrderId = position.getTargetOrderId();
                    String newOrderId = orderServiceV2.updateTargetOrder(
                            symbol,
                            ltp, // New target = current LTP
                            qty,
                            oldOrderId,
                            false, // isHolding = false (INTRADAY)
                            position.getStrategy()
                    );

                    if (newOrderId != null) {
                        // Update position with new target order ID
                        position.setTargetOrderId(newOrderId);
                        position.setTarget(ltp);
                        positionManager.updatePosition(position);

                        updatedCount++;
                        System.out.println("✅ [TrailingStop] " + symbol + 
                                         ": Updated target to LTP ₹" + String.format("%.2f", ltp) +
                                         " (Profit: " + String.format("%.2f", profitPercent) + "%, " +
                                         "Entry: ₹" + String.format("%.2f", avgPrice) + ")");
                    } else {
                        failedCount++;
                        System.err.println("❌ [TrailingStop] " + symbol + ": Failed to update target order");
                    }

                    // Small delay to avoid rate limiting
                    Thread.sleep(100);

                } catch (Exception e) {
                    failedCount++;
                    System.err.println("❌ [TrailingStop] Error processing " + position.getSymbol() + 
                                     ": " + e.getMessage());
                }
            }

            // Summary
            String summary = String.format(
                "[TrailingStop] Complete - Updated: %d, Skipped: %d, Failed: %d",
                updatedCount, skippedCount, failedCount
            );
            System.out.println("📊 " + summary);

        } catch (Exception e) {
            System.err.println("❌ [TrailingStop] Error in updateTrailingStops: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

