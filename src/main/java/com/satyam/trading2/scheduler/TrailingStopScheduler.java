package com.satyam.trading2.scheduler;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.helpers.LatestPriceHelper;
import com.satyam.trading2.order.BrokerOrderRouter;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.service.OrderServiceV2;
import com.satyam.trading2.service.ReconciliationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

import static com.satyam.trading2.datamodel.Position.PositionType.HOLDING;
import static com.satyam.trading2.datamodel.Position.PositionType.INTRADAY;

@Service
@RequiredArgsConstructor
public class TrailingStopScheduler {

    private final PositionManager positionManager;
    private final OrderServiceV2 orderServiceV2;
    private final ReconciliationService reconciliationService;
    private final com.satyam.trading2.service.KillSwitchService killSwitchService;
    private final PendingOrderRepository pendingOrderRepository;
    private final BrokerOrderRouter brokerOrderRouter;

    @Scheduled(cron = "0/10 * * * * MON-FRI")
    public void updateTrailingStops() {
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [TrailingStopScheduler] Skipped - Kill switch is ACTIVE");
            return;
        }

        LocalTime now = LocalTime.now();
        if (now.isBefore(LocalTime.of(9, 17))) {
            return;
        }

        // Stop running after 15:00
        if (now.isAfter(LocalTime.of(15, 3))) {
            return;
        }

        double threshold;
        if (!now.isBefore(LocalTime.of(14, 0))) { // >= 14:00
            threshold = 0.2;
        } else if (!now.isBefore(LocalTime.of(13, 0))) { // >= 13:00
            threshold = 0.25;
        } else if (!now.isBefore(LocalTime.of(12, 0))) { // >= 12:00
            threshold = 0.3;
        } else {
            threshold = 0.4;
        }

//        System.out.println("🔄 [TrailingStop] Running at " + now + " - threshold: " + threshold + "%");

        reconciliationService.syncBrokerPositions(false);
        try {
            // Process BOTH intraday and holding positions
            List<Position> openPositions = positionManager.getAllOpenPositions().stream()
                    .filter(p -> p.getPositionType() == INTRADAY || p.getPositionType() == HOLDING)
                    .filter(p -> !p.isExitProcessed())
                    .collect(Collectors.toList());

            if (openPositions.isEmpty()) {
                System.out.println("⏭️ [TrailingStop] No open positions found - skipping");
                return;
            }

            long intradayCount = openPositions.stream().filter(p -> p.getPositionType() == INTRADAY).count();
            long holdingCount = openPositions.stream().filter(p -> p.getPositionType() == HOLDING).count();
//            System.out.println("📊 [TrailingStop] Found " + intradayCount + " INTRADAY + " + holdingCount + " HOLDING positions");

            int updatedCount = 0;
            int skippedCount = 0;
            int failedCount = 0;

            for (Position position : openPositions) {
                try {
                    boolean isHolding = position.getPositionType() == HOLDING;
                    String symbol = position.getSymbol();
                    double avgPrice = position.getAveragePrice();
                    int qty = position.getTotalQuantity();

                    double ltp = LatestPriceHelper.getLatestPrice(symbol);

                    if (ltp <= 0) {
//                        System.out.println("⚠️ [TrailingStop] " + symbol + ": LTP not available, skipping");
                        skippedCount++;
                        continue;
                    }

                    double profitPercent = ((ltp - avgPrice) / avgPrice) * 100.0;

                    if (profitPercent < threshold) {
                        skippedCount++;
                        continue;
                    }

                    if (isHolding) {
                        // ===== CNC/HOLDING: Update only the primary target sell (A).
                        // Do NOT touch the secondary quick-exit sell (B) — it should
                        // remain at its original 1% price. We cancel only the primary
                        // targetOrderId and place a new sell at LTP for total qty.
                        // Using placeOrder directly instead of updateTargetOrder to
                        // avoid the blanket "cancel all CNC SELL orders" behavior.
                        String oldPrimaryOrderId = position.getTargetOrderId();
                        if (oldPrimaryOrderId != null) {
                            try {
                                brokerOrderRouter.cancelViaBroker(position, oldPrimaryOrderId);
                                Thread.sleep(300);
                            } catch (Exception e) {
                                System.err.println("⚠️ [TrailingStop] Failed to cancel primary sell for " + symbol + ": " + e.getMessage());
                            }
                        }

                        String newOrderId = brokerOrderRouter.placeLimitSell(
                                position, symbol, ltp, qty, true, position.getStrategy());

                        if (newOrderId != null) {
                            position.setTargetOrderId(newOrderId);
                            position.setTarget(ltp);
                            positionManager.updatePosition(position);

                            // Broker-tagged PendingOrder so the fill event is matched to the
                            // owning broker (KITE/SHOONYA/INDSTOCKS)
                            PendingOrder targetPO = brokerOrderRouter.createSellPending(
                                    position, newOrderId, symbol,
                                    position.getStrategy(), qty, ltp, true);
                            pendingOrderRepository.save(targetPO);

                            updatedCount++;
//                            System.out.println("✅ [TrailingStop] " + symbol + " (CNC)" +
//                                    ": Updated primary sell to LTP ₹" + String.format("%.2f", ltp) +
//                                    " (Profit: " + String.format("%.2f", profitPercent) + "%, " +
//                                    "Entry: ₹" + String.format("%.2f", avgPrice) + ")");
                        } else {
                            failedCount++;
                            System.err.println("❌ [TrailingStop] " + symbol + " (CNC): Failed to update primary sell");
                        }
                    } else {
                        // ===== INTRADAY/MIS: update single target sell to LTP, routed to the
                        // position's owning broker. KITE keeps updateTargetOrder (dedup); non-KITE
                        // cancels the prior target then places a fresh LIMIT SELL at that broker.
                        String oldOrderId = position.getTargetOrderId();
                        String newOrderId = brokerOrderRouter.placeTargetSell(
                                position, symbol, ltp, qty, oldOrderId, false, position.getStrategy());

                        if (newOrderId != null) {
                            position.setTargetOrderId(newOrderId);
                            position.setTarget(ltp);
                            positionManager.updatePosition(position);

                            // For non-KITE brokers a NEW sell order was placed, so register a
                            // broker-tagged PendingOrder to match its fill. KITE's updateTargetOrder
                            // dedups and is reconciled separately, so keep its behavior unchanged.
                            if (!brokerOrderRouter.isKite(position)) {
                                PendingOrder targetPO = brokerOrderRouter.createSellPending(
                                        position, newOrderId, symbol,
                                        position.getStrategy(), qty, ltp, false);
                                pendingOrderRepository.save(targetPO);
                            }

                            updatedCount++;
//                            System.out.println("✅ [TrailingStop] " + symbol +
//                                    ": Updated target to LTP ₹" + String.format("%.2f", ltp) +
//                                    " (Profit: " + String.format("%.2f", profitPercent) + "%, " +
//                                    "Entry: ₹" + String.format("%.2f", avgPrice) + ")");
                        } else {
                            failedCount++;
                            System.err.println("❌ [TrailingStop] " + symbol + ": Failed to update target order");
                        }
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
//            System.out.println("📊 " + summary);

        } catch (Exception e) {
            System.err.println("❌ [TrailingStop] Error in updateTrailingStops: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
