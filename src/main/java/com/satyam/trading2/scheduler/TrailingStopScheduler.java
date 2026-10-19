package com.satyam.trading2.scheduler;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.helpers.LatestPriceHelper;
import com.satyam.trading2.order.BrokerOrderRouter;
import com.satyam.trading2.order.SellLifecycleLockRegistry;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.service.ReconciliationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.satyam.trading2.datamodel.Position.PositionType.HOLDING;
import static com.satyam.trading2.datamodel.Position.PositionType.INTRADAY;

@Service
@RequiredArgsConstructor
public class TrailingStopScheduler {

    private final PositionManager positionManager;
    private final ReconciliationService reconciliationService;
    private final com.satyam.trading2.service.KillSwitchService killSwitchService;
    private final PendingOrderRepository pendingOrderRepository;
    private final BrokerOrderRouter brokerOrderRouter;

    @Scheduled(cron = "0/5 * * * * MON-FRI")
    public void updateTrailingStops() {
        if (killSwitchService.isActive()) {
            System.out.println("🛑 [TrailingStopScheduler] Skipped - Kill switch is ACTIVE");
            return;
        }

        LocalTime now = LocalTime.now();
        if (now.isBefore(LocalTime.of(9, 15,30))) {
            return;
        }

        // Stop running after 15:00
        if (now.isAfter(LocalTime.of(15, 3))) {
            return;
        }

        double threshold;
        if (!now.isBefore(LocalTime.of(14, 0))) { // >= 14:00
            threshold = 0.05;
        } else if (!now.isBefore(LocalTime.of(13, 0))) { // >= 13:00
            threshold = 0.1;
        } else if (!now.isBefore(LocalTime.of(12, 0))) { // >= 12:00
            threshold = 0.2;
        } else if (!now.isBefore(LocalTime.of(11, 0))) { // >= 11:00
            threshold = 0.3;
        } else if (!now.isBefore(LocalTime.of(10, 0))) { // >= 10:00
            threshold = 0.4;
        } else {
            threshold = 0.5;
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
            System.out.println("📊 [TrailingStop] Found " + intradayCount + " INTRADAY + " + holdingCount + " HOLDING positions");

            int updatedCount = 0;
            int skippedCount = 0;
            int failedCount = 0;

            // ===== PHASE 1: decide + cancel. For every position that qualifies for
            // trailing, cancel its existing primary target sell and record a decision.
            // All cancels are issued first so PHASE 2 can verify them with a SINGLE
            // fetchOrders() snapshot instead of one GET /orders per position.
            List<TrailDecision> decisions = new ArrayList<>();
            for (Position position : openPositions) {
                try {
                    boolean isHolding = position.getPositionType() == HOLDING;

                    // Legacy protection: older versions could leave a secondary quick-exit
                    // SELL active alongside the full-quantity holding target. Do not replace
                    // the full-quantity target while that second order is still associated
                    // with the position; reconcile/cancel the legacy order first.
                    if (isHolding && position.getSecondaryTargetOrderId() != null) {
                        System.err.println("🛑 [TrailingStop] " + position.getSymbol()
                                + ": skipping target replacement while legacy secondary SELL "
                                + position.getSecondaryTargetOrderId() + " is associated with the holding.");
                        skippedCount++;
                        continue;
                    }

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
                        skippedCount++;
                        continue;
                    }

                    // CNC/HOLDING updates ONLY the primary target sell (A); the secondary
                    // quick-exit sell (B) is left at its original 1% price. INTRADAY/MIS
                    // cancels the prior target. Either way we cancel only the primary
                    // targetOrderId here and place the replacement in PHASE 2, after the
                    // short-sell guard has re-checked the cancelled order's status.
                    String oldOrderId = position.getTargetOrderId();
                    // Do not send cancellation during collection. Cancellation and
                    // replacement must happen atomically under the shared position lock
                    // in phase 2, with broker-confirmed zero fills between the two.
                    decisions.add(new TrailDecision(position, symbol, avgPrice, qty, ltp, profitPercent, isHolding, oldOrderId));

                } catch (Exception e) {
                    failedCount++;
                    System.err.println("❌ [TrailingStop] Error processing " + position.getSymbol() +
                            ": " + e.getMessage());
                }
            }

            if (decisions.isEmpty()) {
                System.out.println("📊 " + String.format(
                        "[TrailingStop] Complete - Updated: %d, Skipped: %d, Failed: %d",
                        updatedCount, skippedCount, failedCount));
                return;
            }

            // ===== PHASE 2: atomically cancel-confirm and replace each target SELL.
            for (TrailDecision d : decisions) {
                try {
                    synchronized (SellLifecycleLockRegistry.lockFor(d.position)) {
                        if (d.oldOrderId != null
                                && !brokerOrderRouter.cancelAndConfirmSell(d.position, d.oldOrderId, 2500L)) {
                            skippedCount++;
                            System.err.println("🛑 [TrailingStop] " + d.symbol
                                    + ": previous SELL " + d.oldOrderId
                                    + " is not confirmed terminal with zero fills; replacement blocked.");
                            continue;
                        }

                        String newOrderId = brokerOrderRouter.placeTargetSell(
                                d.position, d.symbol, d.ltp, d.qty, d.oldOrderId,
                                d.isHolding, d.position.getStrategy());

                    if (newOrderId != null) {
                        d.position.setTargetOrderId(newOrderId);
                        d.position.setTarget(d.ltp);
                        positionManager.updatePosition(d.position);

                        // Broker-tagged SELL PendingOrder so the fill event is matched to the
                        // owning broker (KITE/SHOONYA/INDSTOCKS) and routed to onSellCompleted()
                        // — P&L recorded, dashboard updated, exit metrics written.
                        PendingOrder targetPO = brokerOrderRouter.createSellPending(
                                d.position, newOrderId, d.symbol,
                                d.position.getStrategy(), d.qty, d.ltp, d.isHolding);
                        pendingOrderRepository.save(targetPO);

                        updatedCount++;
                        System.out.println("✅ [TrailingStop] " + d.symbol + (d.isHolding ? " (CNC)" : "") +
                                ": Updated " + (d.isHolding ? "primary" : "target") + " sell to LTP ₹" + String.format("%.2f", d.ltp) +
                                " (Profit: " + String.format("%.2f", d.profitPercent) + "%, " +
                                "Entry: ₹" + String.format("%.2f", d.avgPrice) + ")");
                    } else {
                        failedCount++;
                        System.err.println("❌ [TrailingStop] " + d.symbol + ": Failed to update " +
                                (d.isHolding ? "primary" : "target") + " sell");
                    }

                    Thread.sleep(100);
                    }

                } catch (Exception e) {
                    failedCount++;
                    System.err.println("❌ [TrailingStop] Error processing " + d.position.getSymbol() +
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

    /**
     * Per-position trailing decision captured in PHASE 1 (after the cancel is issued)
     * and consumed in PHASE 2 (after the single orders snapshot is taken).
     */
    private static final class TrailDecision {
        final Position position;
        final String symbol;
        final double avgPrice;
        final int qty;
        final double ltp;
        final double profitPercent;
        final boolean isHolding;
        final String oldOrderId;

        TrailDecision(Position position, String symbol, double avgPrice, int qty, double ltp,
                      double profitPercent, boolean isHolding, String oldOrderId) {
            this.position = position;
            this.symbol = symbol;
            this.avgPrice = avgPrice;
            this.qty = qty;
            this.ltp = ltp;
            this.profitPercent = profitPercent;
            this.isHolding = isHolding;
            this.oldOrderId = oldOrderId;
        }
    }
}
