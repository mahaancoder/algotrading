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
                position.setPositionType(HOLDING);
                // convertToHolding (MIS→CNC) is a Kite-only API. Non-KITE brokers
                // (Shoonya/INDstocks) have no product-conversion endpoint on the Broker
                // interface, so for them we only flip the position type internally.
                if (brokerOrderRouter.isKite(position)) {
                    orderServiceV2.convertToHolding(symbol, position.getTotalQuantity());
                } else {
                    System.out.println("ℹ️ [ConvertOrSellPositionScheduler] " + brokerOrderRouter.brokerOf(position) +
                            " has no MIS→CNC convert API — marking " + symbol + " as HOLDING internally only.");
                }

                // Cancel target and SL orders at the broker that owns this position
                brokerOrderRouter.cancelViaBroker(position, position.getTargetOrderId());
                brokerOrderRouter.cancelViaBroker(position, position.getStopLossOrderId());
                position.setTargetOrderId(null);
                position.setStopLossOrderId(null);

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
            } catch (Exception e) {
                System.out.println("Error converting position to holding: " + symbol);
                e.printStackTrace();
            }
        }
    }
}
