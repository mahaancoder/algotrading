package com.satyam.trading2.scheduler;

import com.satyam.trading2.datamodel.DipState;
import com.satyam.trading2.datamodel.HoldingMetadata;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.infrastructure.messaging.BroadcastService;
import com.satyam.trading2.order.BrokerOrderRouter;
import com.satyam.trading2.order.SellLifecycleLockRegistry;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.repository.HoldingMetadataRepository;
import com.satyam.trading2.service.DipAccumulatorService;
import com.satyam.trading2.service.ExecutionEngine;
import com.satyam.trading2.service.OrderServiceV2;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.satyam.trading2.datamodel.Position.PositionType.HOLDING;
import static com.satyam.trading2.datamodel.TradeSignal.SignalType.EXIT_LONG;
import static com.satyam.trading2.helpers.LatestPriceHelper.getLatestPrice;

@Service
@RequiredArgsConstructor
public class ConvertOrSellPositionScheduler {

    private final PositionManager positionManager;
    private final OrderServiceV2 orderServiceV2;
    private final BroadcastService broadcastService;
    private final ExecutionEngine executionEngine;
    private final DipAccumulatorService dipAccumulatorService;
    private final PendingOrderRepository pendingOrderRepository;
    private final HoldingMetadataRepository holdingMetadataRepository;
    private final com.satyam.trading2.service.EntryTypeAnalyticsService entryTypeAnalytics;
    private final BrokerOrderRouter brokerOrderRouter;

    // ===== FIX: Use ExecutionEngine's locks to prevent race conditions =====
    private Object getSymbolStrategyLock(String symbol, String strategy) {
        return executionEngine.getSymbolStrategyLocks().computeIfAbsent(
            symbol + "_" + strategy,
            k -> new Object()
        );
    }

    // Suffix on a forced-MIS leg (MIS-only mode). The leg is tracked under
    // "<strategy>#MIS" intraday; when it converts to a CNC holding at EOD it is
    // re-keyed back to the base strategy so it merges with the normal holding book.
    private static final String MIS_ONLY_SUFFIX = "#MIS";

    private boolean isForcedMisLeg(Position position) {
        String s = position.getStrategy();
        return s != null && s.endsWith(MIS_ONLY_SUFFIX);
    }

    private static String baseStrategy(String strategy) {
        if (strategy != null && strategy.endsWith(MIS_ONLY_SUFFIX)) {
            return strategy.substring(0, strategy.length() - MIS_ONLY_SUFFIX.length());
        }
        return strategy;
    }

    @Scheduled(cron = "0 05 15 * * MON-FRI")
    public void handle315ExitRule() {
        // 🛑 KILL SWITCH CHECK
        if (executionEngine.getKillSwitchService().isActive()) {
            System.out.println("🛑 [ConvertOrSellPositionScheduler] Skipped - Kill switch is ACTIVE");
            return;
        }
        handleAllOpenPositions();
    }

    public void handleAllOpenPositions() {
        // Step 1: Get all INTRADAY positions and calculate their PnL
        List<Position> intradayPositions = new ArrayList<>();
        for (Position position : positionManager.getAllOpenPositions()) {
            if (position.getPositionType() == HOLDING) continue;
            intradayPositions.add(position);
        }

        // Step 2: Sort positions by PnL in descending order (most profitable first)
        intradayPositions.sort((p1, p2) -> {
            double ltp1 = getLatestPrice(p1.getSymbol());
            double ltp2 = getLatestPrice(p2.getSymbol());
            double pnl1 = (ltp1 - p1.getAveragePrice()) * p1.getTotalQuantity();
            double pnl2 = (ltp2 - p2.getAveragePrice()) * p2.getTotalQuantity();
            return Double.compare(pnl2, pnl1); // Descending order
        });

        // Step 3: Process profitable positions (PnL >= 0) - update target to LTP
        List<Position> lossPositions = new ArrayList<>();
        for (Position position : intradayPositions) {
            String symbol = position.getSymbol();
            String strategy = position.getStrategy();

            // ===== FIX: Skip if position is already exit processed =====
            if (position.isExitProcessed()) {
                System.out.println("⏭️ [EOD Scheduler] Skipping " + symbol + " - already exit processed");
                continue;
            }

            double ltp = getLatestPrice(symbol);
            double pnl = (ltp - position.getAveragePrice()) * position.getTotalQuantity();

            if (pnl >= 0) {
                // ===== FIX: Synchronize on symbol+strategy lock to prevent race with webhook handler =====
                synchronized (getSymbolStrategyLock(symbol, strategy)) {
                    try {
                        // ===== FIX: Double-check exit status before placing order (prevent race condition) =====
                        if (position.isExitProcessed()) {
                            System.out.println("⏭️ [EOD Scheduler] Skipping " + symbol + " - exit processed during iteration");
                            continue;
                        }

                        // Update existing target order to LTP, routed to the position's owning
                        // broker. KITE keeps updateTargetOrder (dedup); non-KITE cancels the prior
                        // target then places a fresh LIMIT SELL at that broker.
                        String updatedTargetOrderId = brokerOrderRouter.placeTargetSell(
                                position,
                                symbol,
                                ltp,
                                position.getTotalQuantity(),
                                position.getTargetOrderId(),
                                false, // MIS positions (not holdings)
                                strategy
                        );

                        if (updatedTargetOrderId != null) {
                            // Update the position with new target order ID
                            position.setTargetOrderId(updatedTargetOrderId);

                            // Broker-tagged PendingOrder so the fill event is matched to the
                            // owning broker (KITE/SHOONYA/INDSTOCKS)
                            PendingOrder targetPO = brokerOrderRouter.createSellPending(
                                    position,
                                    updatedTargetOrderId,
                                    symbol,
                                    strategy,
                                    position.getTotalQuantity(),
                                    ltp,
                                    false // MIS positions (not holdings)
                            );
                            pendingOrderRepository.save(targetPO);

                            // Update the position in position manager
                            positionManager.updatePosition(position);

                            broadcastService.broadcastLog("Updated target to LTP for profitable position: " + symbol + " PnL=" + String.format("%.2f", pnl) + " New Target=" + ltp);
                        }
                    } catch (Exception e) {
                        System.out.println("Error updating target for profitable position: " + symbol);
                        e.printStackTrace();
                    }
                }
            } else {
                // Add to loss positions list for later processing
                lossPositions.add(position);
            }
        }

        // Step 4: Process loss-making positions from lowest PnL (end of list) first
        // Iterate in reverse order so lowest PnL gets converted first
        for (int i = lossPositions.size() - 1; i >= 0; i--) {
            Position position = lossPositions.get(i);
            String symbol = position.getSymbol();
            double ltp = getLatestPrice(symbol);
            double pnl = (ltp - position.getAveragePrice()) * position.getTotalQuantity();

            try {
                synchronized (SellLifecycleLockRegistry.lockFor(position)) {
                // ===== MIS-ONLY LEG: re-key to the base strategy before converting =====
                // A forced-MIS leg is tracked under "<strategy>#MIS" so it coexists
                // with any existing holding intraday. When it converts to a CNC holding
                // at EOD, it must live under the BASE strategy (the key holdings are
                // fetched/restored under) — so drop the old "#MIS"-keyed entry and
                // rewrite the position's strategy to the base before persisting.
                if (isForcedMisLeg(position)) {
                    String posBroker = position.getBroker() != null ? position.getBroker() : "KITE";
                    String misStrategy = position.getStrategy();
                    positionManager.removePosition(symbol, misStrategy, posBroker);
                    dipAccumulatorService.remove(symbol, misStrategy);
                    position.setStrategy(baseStrategy(misStrategy));
                }

                // Confirm all existing SELL orders are cancelled with zero fills BEFORE
                // converting/re-keying the position. If fills are unknown, leave the MIS
                // position intact and let reconciliation establish the real quantity.
                if (position.getTargetOrderId() != null
                        && !brokerOrderRouter.cancelAndConfirmSell(position, position.getTargetOrderId(), 2500L)) {
                    System.err.println("🛑 [ConvertOrSellPositionScheduler] Cannot convert " + symbol
                            + ": target SELL is not confirmed cancelled with zero fills.");
                    continue;
                }
                if (position.getStopLossOrderId() != null
                        && !brokerOrderRouter.cancelAndConfirmSell(position, position.getStopLossOrderId(), 2500L)) {
                    System.err.println("🛑 [ConvertOrSellPositionScheduler] Cannot convert " + symbol
                            + ": secondary SELL/stop order is not confirmed cancelled with zero fills.");
                    continue;
                }
                position.setTargetOrderId(null);
                position.setTargetPlaced(false);
                position.setStopLossOrderId(null);

                // convertToHolding (MIS→CNC) is a Kite-only API. Non-KITE brokers
                // (Shoonya/INDstocks) have no product-conversion endpoint on the Broker
                // interface, so for them we only flip the position type internally.
                if (brokerOrderRouter.isKite(position)) {
                    orderServiceV2.convertToHolding(symbol, position.getTotalQuantity());
                } else {
                    System.out.println("ℹ️ [ConvertOrSellPositionScheduler] " + brokerOrderRouter.brokerOf(position) +
                            " has no MIS→CNC convert API — marking " + symbol + " as HOLDING internally only.");
                }
                position.setPositionType(HOLDING);

                // Re-insert under the (possibly re-keyed) strategy so a converted
                // forced-MIS leg is now tracked as a HOLDING under the base strategy.
                positionManager.updatePosition(position);

                // ===== ANALYTICS: Mark as converted to holding =====
                position.setConvertedToHolding(true);
                position.setConversionTime(System.currentTimeMillis());

                // ===== PERSISTENCE: Save holding metadata for next day restoration =====
                HoldingMetadata metadata = HoldingMetadata.fromPosition(position);
                holdingMetadataRepository.save(metadata);

                // ===== ANALYTICS: Record conversion to holding for entry type analytics =====
                entryTypeAnalytics.recordConversionToHolding(position.getEntryType());

                // Mark DipState as converted to holding
                DipState dipState = dipAccumulatorService.get(symbol, position.getStrategy());
                if (dipState != null) {
                    dipState.setConvertedToHolding(true);
                    dipAccumulatorService.save(dipState);
                }
                broadcastService.broadcastLog("Converted to HOLDING: " + symbol + " PnL=" + String.format("%.2f", pnl) +
                                             " | EntryType=" + (position.getEntryType() != null ? position.getEntryType() : "UNKNOWN"));
                }
            } catch (Exception e) {
                System.out.println("Error converting position to holding: " + symbol);
                e.printStackTrace();
            }
        }
    }
}
