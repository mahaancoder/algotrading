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

@Service
@RequiredArgsConstructor
public class TrailingStopScheduler {

    private final PositionManager positionManager;
    private final OrderServiceV2 orderServiceV2;
    private final ReconciliationService reconciliationService;
    private final com.satyam.trading2.service.KillSwitchService killSwitchService;

    @Scheduled(cron = "0/20 * * * * MON-FRI")
    public void updateTrailingStops() {
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [TrailingStopScheduler] Skipped - Kill switch is ACTIVE");
            return;
        }

        LocalTime now = LocalTime.now();
        if (now.isBefore(LocalTime.of(9, 25))) {
            return;
        }

        // Stop running after 15:00
        if (now.isAfter(LocalTime.of(15, 0))) {
            return;
        }

        double threshold;
        if (!now.isBefore(LocalTime.of(14, 0))) { // >= 14:00
            threshold = 0.05;
        } else if (!now.isBefore(LocalTime.of(13, 0))) { // >= 13:00
            threshold = 0.1;
        } else if (!now.isBefore(LocalTime.of(12, 0))) { // >= 12:00
            threshold = 0.15;
        } else {
            threshold = 0.3;
        }

        System.out.println("🔄 [TrailingStop] Running at " + now + " - threshold: " + threshold + "%");

        reconciliationService.syncBrokerPositions(false);
        try {
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

                    double ltp = LatestPriceHelper.getLatestPrice(symbol);

                    if (ltp <= 0) {
                        System.out.println("⚠️ [TrailingStop] " + symbol + ": LTP not available, skipping");
                        skippedCount++;
                        continue;
                    }

                    double profitPercent = ((ltp - avgPrice) / avgPrice) * 100.0;

                    if (profitPercent < threshold) {
                        System.out.println("⏭️ [TrailingStop] " + symbol + ": Profit " +
                                String.format("%.2f", profitPercent) + "% < " + threshold + "%, skipping");
                        skippedCount++;
                        continue;
                    }

                    String oldOrderId = position.getTargetOrderId();
                    String newOrderId = orderServiceV2.updateTargetOrder(
                            symbol,
                            ltp,
                            qty,
                            oldOrderId,
                            false,
                            position.getStrategy()
                    );

                    if (newOrderId != null) {
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

                    Thread.sleep(100);

                } catch (Exception e) {
                    failedCount++;
                    System.err.println("❌ [TrailingStop] Error processing " + position.getSymbol() +
                            ": " + e.getMessage());
                }
            }

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
