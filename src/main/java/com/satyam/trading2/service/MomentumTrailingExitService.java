package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.Instrument;
import com.satyam.trading2.datamodel.MomentumTrailingState;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.helpers.LatestPriceHelper;
import com.satyam.trading2.order.BrokerOrderRouter;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.order.SellLifecycleLockRegistry;
import com.satyam.trading2.service.TelegramNotificationService;
import com.satyam.trading2.datamodel.Nifty500Stocks;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Dedicated exit manager for Dip-Accumulator-Momentum.
 *
 * Feature is disabled by default. Live order placement additionally requires
 * momentum.trailing.live-enabled=true. Broker-specific round-trip cost estimates
 * must be configured before an exit can be submitted.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MomentumTrailingExitService {
    private static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");
    private static final double ACTIVATION_PERCENT = 0.7;
    private static final double INITIAL_TRIGGER_PERCENT = 0.5;
    private static final double TRAIL_STEP_PERCENT = 0.1;
    private static final double TRAIL_GAP_PERCENT = 0.2;
    private static final double LIMIT_OFFSET_PERCENT = 0.05;

    private final PositionManager positionManager;
    private final BrokerOrderRouter brokerOrderRouter;
    private final PendingOrderRepository pendingOrderRepository;
    private final MomentumTrailingStateRepository stateRepository;
    private final TelegramNotificationService telegram;
    private final Environment environment;
    private final Map<String, Long> buyCancelAttemptAt = new ConcurrentHashMap<>();
    private final Map<String, Long> buyCancelAlertAt = new ConcurrentHashMap<>();

    @Value("${momentum.trailing.enabled:false}")
    private boolean enabled;

    @Value("${momentum.trailing.live-enabled:false}")
    private boolean liveEnabled;

    @Value("${momentum.trailing.entry-cutoff:14:55}")
    private String entryCutoffText;

    @Value("${momentum.trailing.eod-cutoff:15:00}")
    private String eodCutoffText;

    @Value("${momentum.trailing.action-interval-ms:1000}")
    private long actionIntervalMillis;

    @Value("${momentum.trailing.alert-repeat-ms:60000}")
    private long alertRepeatMillis;

    @Value("${momentum.trailing.ltp-stale-ms:5000}")
    private long ltpStaleMillis;

    @Value("${momentum.trailing.mis-squareoff-deadline:15:20}")
    private String defaultMisSquareoffDeadline;

    /**
     * Called from the market-data tick path. This method only updates durable
     * strategy state; broker REST operations are performed by the scheduled worker.
     */
    public void onPriceTick(String symbol, double ltp) {
        if (!enabled || symbol == null || !Double.isFinite(ltp) || ltp <= 0) return;
        if (!MomentumTrailingPolicy.isStateStoreWritable(environment)) return;
        for (Position position : positionManager.getPositionsForSymbol(symbol).values()) {
            if (position == null || !position.isOpen()
                    || position.getPositionType() != Position.PositionType.INTRADAY) continue;
            synchronized (SellLifecycleLockRegistry.lockFor(position)) {
                stateRepository.restoreBuyOrderIds(position, LocalDate.now(ZONE).toString());
            }
            if (!MomentumTrailingPolicy.isManagedPosition(position, pendingOrderRepository)) continue;
            MomentumTrailingState state = getOrCreateState(position, LocalDate.now(ZONE).toString());
            synchronized (state) {
                if (!LocalDate.now(ZONE).toString().equals(state.getTradingDate()) || state.isFeedGapObserved()) continue;
                // The desired trigger may be ahead of the broker order while a
                // cancel/replace is in flight. Protect against that logical stop too.
                double triggerToWatch = Math.max(state.getWorkingTriggerPrice(), state.getDesiredTriggerPrice());
                if (state.isActivated() && !state.isExitTriggered() && !state.isEodExitRequested()
                        && triggerToWatch > 0 && ltp <= triggerToWatch) {
                    // Detect trigger crossings on the tick callback itself; the one-second
                    // broker worker must not miss a brief dip and subsequent rebound.
                    state.setExitTriggered(true);
                    state.setPhase("EXIT_TRIGGERED");
                    state.setLastOrderActionAt(System.currentTimeMillis());
                    stateRepository.save(state);
                } else {
                    updateTrailingLevels(state, position, ltp);
                }
            }
        }
    }

    @Scheduled(fixedDelayString = "${momentum.trailing.poll-ms:1000}")
    public void manageMomentumPositions() {
        if (!enabled) return;
        if (!MomentumTrailingPolicy.isStateStoreWritable(environment)) {
            log.warn("[MomentumTrailing] State store is not writable; leaving legacy exit management active.");
            return;
        }
        LocalTime now = LocalTime.now(ZONE);
        LocalTime entryCutoff = parseTime(entryCutoffText, LocalTime.of(14, 55));
        LocalTime eodCutoff = parseTime(eodCutoffText, LocalTime.of(15, 0));
        if (liveEnabled && !now.isBefore(entryCutoff)) {
            // Start draining pending BUYs at the entry cutoff, leaving time for
            // terminal events and partial-fill reconciliation before EOD liquidation.
            cancelAllPendingMomentumBuys();
        }
        List<Position> positions = positionManager.getAllOpenPositions();
        for (Position position : positions) {
            if (position == null || !position.isOpen()
                    || position.getPositionType() != Position.PositionType.INTRADAY) continue;
            synchronized (SellLifecycleLockRegistry.lockFor(position)) {
                stateRepository.restoreBuyOrderIds(position, LocalDate.now(ZONE).toString());
            }
            if (!MomentumTrailingPolicy.isManagedPosition(position, pendingOrderRepository)) continue;
            try {
                manageOne(position, now, eodCutoff);
            } catch (Exception e) {
                log.error("[MomentumTrailing] {} failed closed: {}", position.getSymbol(), e.getMessage(), e);
                alert(position, "TRAILING_MANAGER_ERROR",
                        "Exit manager failed; no replacement SELL was assumed. " + e.getMessage());
            }
        }
    }

    private void manageOne(Position position, LocalTime now, LocalTime eodCutoff) {
        if (position.getPositionType() != Position.PositionType.INTRADAY) {
            alert(position, "UNEXPECTED_MOMENTUM_HOLDING",
                    "Momentum trailing policy only manages INTRADAY positions. Manual review required.");
            return;
        }
        double ltp = LatestPriceHelper.getLatestPrice(position.getSymbol());
        long tickAt = LatestPriceHelper.getLastPriceUpdateTime(position.getSymbol());
        String today = LocalDate.now(ZONE).toString();
        if (ltp <= 0 || tickAt <= 0 || System.currentTimeMillis() - tickAt > ltpStaleMillis) {
            MomentumTrailingState staleState = getOrCreateState(position, today);
            if (staleState.isActivated() || staleState.getExitOrderId() != null || staleState.isEodExitRequested()) {
                staleState.setFeedGapObserved(true);
                stateRepository.save(staleState);
                alert(position, "TRAILING_FEED_GAP",
                        "Market-data gap observed while an exit was active. Trailing/order replacement is paused until broker order status is reconciled.");
            } else {
                alert(position, "STALE_OR_MISSING_LTP", "Fresh LTP is unavailable; trailing activation is paused.");
            }
            return;
        }

        MomentumTrailingState state = getOrCreateState(position, today);
        if (!today.equals(state.getTradingDate())) {
            alert(position, "TRAILING_STATE_DATE_MISMATCH",
                    "Persisted trailing state belongs to another trading date. No automated order changes were made.");
            return;
        }
        if (state.getExitOrderId() != null) {
            synchronized (SellLifecycleLockRegistry.lockFor(position)) {
                if (position.getStopLossOrderId() == null) {
                    // Restore the order-to-position link before broker events are handled.
                    position.setStopLossOrderId(state.getExitOrderId());
                    positionManager.updatePosition(position);
                } else if (!state.getExitOrderId().equals(position.getStopLossOrderId())) {
                    alert(position, "TRAILING_ORDER_ID_MISMATCH",
                            "Persisted trailing order ID differs from the position's SELL reference. No automated replacement is safe.");
                    return;
                }
            }
        }
        if (state.isFeedGapObserved()) {
            if (state.getExitOrderId() != null && state.isExitOrderIsStopLoss()) {
                com.satyam.trading2.broker.model.BrokerOrderStatus brokerStatus =
                        brokerOrderRouter.getOrderStatus(position, state.getExitOrderId());
                if (brokerStatus == com.satyam.trading2.broker.model.BrokerOrderStatus.TRIGGER_PENDING) {
                    state.setFeedGapObserved(false);
                    stateRepository.save(state);
                } else if (brokerStatus == com.satyam.trading2.broker.model.BrokerOrderStatus.OPEN
                        || brokerStatus == com.satyam.trading2.broker.model.BrokerOrderStatus.PARTIALLY_FILLED
                        || brokerStatus == com.satyam.trading2.broker.model.BrokerOrderStatus.COMPLETE) {
                    state.setExitTriggered(true);
                    state.setPhase("EXIT_TRIGGERED");
                    state.setFeedGapObserved(false);
                    stateRepository.save(state);
                } else {
                    alert(position, "TRAILING_FEED_GAP_ORDER_UNRESOLVED",
                            "Could not prove whether the protective SELL triggered during the market-data gap. Manual reconciliation required; no replacement was submitted.");
                    return;
                }
            } else if (state.isActivated() && state.getExitOrderId() == null) {
                // There was no broker stop to inspect during the gap. Conservatively
                // treat the stop as crossed rather than resume trailing after a missed dip.
                state.setExitTriggered(true);
                state.setPhase("EXIT_TRIGGERED");
                state.setFeedGapObserved(false);
                stateRepository.save(state);
            } else {
                state.setFeedGapObserved(false);
                stateRepository.save(state);
            }
        }
        if (!state.isActivated() && (state.getExitOrderId() != null || position.getStopLossOrderId() != null)) {
            alert(position, "TRAILING_STATE_MISSING_FOR_EXISTING_ORDER",
                    "A protective SELL exists but its persisted activation/high-water state is missing. Manual reconciliation required.");
            return;
        }

        if (state.isEodExitRequested() && liveEnabled
                && !areMomentumBuysDrained(position)) {
            return; // Never SELL against an unresolved or still-live BUY.
        }

        if (liveEnabled && !MomentumTrailingPolicy.isLivePolicyReady(
                position, pendingOrderRepository, environment,
                brokerOrderRouter.supportsSellStopLossLimit(position))) {
            alert(position, "TRAILING_LIVE_POLICY_NOT_READY",
                    "Live trailing prerequisites are incomplete (broker SL-LIMIT capability, completed momentum BUY ledger, instrument tick size or broker-specific round-trip cost). Legacy fixed-target/EOD management remains active.");
            return;
        }

        // Keep the state monitor only around in-memory/tiny persistence updates.
        // Never hold it while a broker cancel/reconciliation call is waiting.
        synchronized (state) {
            synchronizePositionBasis(state, position);
            updateTrailingLevels(state, position, ltp);

            if (!state.isEodExitRequested() && !now.isBefore(eodCutoff)) {
                state.setEodExitRequested(true);
                state.setPhase("EOD_EXIT");
                stateRepository.save(state);
                alert(position, "EOD_EXIT_STARTED",
                        liveEnabled
                                ? "New entries must stop. Momentum position is being managed for EOD exit; no holding conversion is permitted."
                                : "Shadow tracking reached EOD. No trailing order was sent; legacy exit management remains active.");
            }
        }

        // Tracking can be enabled independently, but broker order placement requires
        // a second explicit opt-in so a deploy cannot silently activate live trading.
        double breakEven = netBreakEven(position);
        if (breakEven <= 0) {
            alert(position, "BREAK_EVEN_NOT_CONFIGURED",
                    "Missing/invalid momentum.trailing.round-trip-cost-percent." + brokerOf(position)
                            + ". Configure the broker-specific estimated all-in round-trip cost before enabling exits.");
            return;
        }
        if (Double.compare(state.getEstimatedNetBreakEvenPrice(), breakEven) != 0) {
            state.setEstimatedNetBreakEvenPrice(breakEven);
            stateRepository.save(state);
        }

        if (state.isEodExitRequested() && ltp < breakEven) {
            LocalTime deadline = parseTime(environment.getProperty(
                    "momentum.trailing.mis-squareoff-deadline." + brokerOf(position), defaultMisSquareoffDeadline),
                    LocalTime.of(15, 20));
            if (!now.isBefore(deadline.minusMinutes(5))) {
                alert(position, "MIS_SQUAREOFF_DEADLINE_APPROACHING",
                        "Position remains below net break-even and broker MIS square-off deadline is approaching at "
                                + deadline + ". Manual intervention is required; the broker may force-square-off.");
            }
        }

        if (!liveEnabled) {
            if (state.isEodExitRequested()) {
                alert(position, "EOD_EXIT_NOT_SENT_LIVE_DISABLED",
                        "Shadow mode only: no trailing order was sent. Legacy fixed-target/EOD management remains responsible for this position.");
            }
            return;
        }

        if (state.isEodExitRequested()) {
            manageExitOrder(position, state, ltp, breakEven, false);
            return;
        }

        if (state.isExitTriggered()) {
            manageExitOrder(position, state, ltp, breakEven, true);
            return;
        }

        if (!state.isActivated()) return;
        double triggerToWatch = Math.max(state.getWorkingTriggerPrice(), state.getDesiredTriggerPrice());
        if (triggerToWatch > 0 && ltp <= triggerToWatch) {
            state.setExitTriggered(true);
            state.setPhase("EXIT_TRIGGERED");
            // Give the broker-native SL-LIMIT a short opportunity to execute before
            // switching to controlled limit repricing.
            state.setLastOrderActionAt(System.currentTimeMillis());
            stateRepository.save(state);
            manageExitOrder(position, state, ltp, breakEven, true);
            return;
        }
        ensureProtectiveStop(position, state, breakEven, ltp);
    }

    /**
     * Stop late BUY fills before beginning EOD liquidation. A terminal partial BUY
     * is not considered drained until its cumulative fill has been applied to the
     * local position by the existing broker event/reconciliation lifecycle.
     */
    private void cancelAllPendingMomentumBuys() {
        long now = System.currentTimeMillis();
        for (PendingOrder pending : pendingOrderRepository.findAll()) {
            if (pending == null || pending.getSide() != com.satyam.trading2.datamodel.TradeSide.BUY
                    || (pending.getPositionType() != null
                        && pending.getPositionType() != Position.PositionType.INTRADAY)
                    || !MomentumTrailingPolicy.isManagedStrategy(pending.getStrategy())) continue;
            String broker = pending.getBroker() != null ? pending.getBroker().trim().toUpperCase(java.util.Locale.ROOT)
                    : pending.getExecutionRoute() != null ? pending.getExecutionRoute().name() : "KITE";
            String orderId = pending.getBrokerOrderId() != null ? pending.getBrokerOrderId() : pending.getOrderId();
            String key = broker + "::" + orderId;
            boolean terminal = pending.getState() == com.satyam.trading2.datamodel.PendingOrderState.COMPLETE
                    || pending.getState() == com.satyam.trading2.datamodel.PendingOrderState.CANCELLED
                    || pending.getState() == com.satyam.trading2.datamodel.PendingOrderState.REJECTED;
            if (terminal) {
                buyCancelAttemptAt.remove(key);
                if (pending.getFilledQty() > 0 && !pending.isCompletionProcessed()) {
                    alertPendingBuy(pending, "Terminal BUY has fills not yet applied to position accounting. No EOD SELL may proceed.");
                }
                continue;
            }

            Long lastAttempt = buyCancelAttemptAt.get(key);
            if (lastAttempt != null && now - lastAttempt < actionIntervalMillis) continue;
            try {
                if (brokerOrderRouter.requestBuyCancellation(pending)) {
                    buyCancelAttemptAt.put(key, now);
                    log.warn("[MomentumTrailing] Requested EOD cancellation for pending BUY {} {}",
                            pending.getSymbol(), orderId);
                } else {
                    buyCancelAttemptAt.put(key, now);
                    alertPendingBuy(pending, "Could not route broker cancellation. EOD SELLs remain blocked until this BUY is terminal.");
                }
            } catch (Exception e) {
                buyCancelAttemptAt.put(key, now);
                alertPendingBuy(pending, "BUY cancellation request failed: " + e.getMessage());
            }
        }
    }

    private boolean areMomentumBuysDrained(Position position) {
        String broker = brokerOrderRouter.brokerOf(position);
        for (PendingOrder buy : pendingOrderRepository.findAll()) {
            if (buy == null || buy.getSide() != com.satyam.trading2.datamodel.TradeSide.BUY
                    || (buy.getPositionType() != null
                        && buy.getPositionType() != Position.PositionType.INTRADAY)
                    || !MomentumTrailingPolicy.isManagedStrategy(buy.getStrategy())
                    || buy.getSymbol() == null || !position.getSymbol().equalsIgnoreCase(buy.getSymbol())
                    || !broker.equals(buy.getBroker() != null ? buy.getBroker().trim().toUpperCase(java.util.Locale.ROOT)
                        : buy.getExecutionRoute() != null ? buy.getExecutionRoute().name() : "KITE")) continue;
            boolean terminal = buy.getState() == com.satyam.trading2.datamodel.PendingOrderState.COMPLETE
                    || buy.getState() == com.satyam.trading2.datamodel.PendingOrderState.CANCELLED
                    || buy.getState() == com.satyam.trading2.datamodel.PendingOrderState.REJECTED;
            if (!terminal || (buy.getFilledQty() > 0 && !buy.isCompletionProcessed())) {
                alert(position, "EOD_WAITING_FOR_BUY_DRAIN",
                        "Momentum BUY " + buy.getOrderId() + " is still live or its fills are unreconciled. No SELL will be submitted until final BUY quantity is known.");
                return false;
            }
        }
        return true;
    }

    private void alertPendingBuy(PendingOrder pending, String message) {
        String broker = pending.getBroker() != null ? pending.getBroker().trim().toUpperCase(java.util.Locale.ROOT)
                : pending.getExecutionRoute() != null ? pending.getExecutionRoute().name() : "KITE";
        String orderId = pending.getBrokerOrderId() != null ? pending.getBrokerOrderId() : pending.getOrderId();
        String key = broker + "::" + orderId;
        long now = System.currentTimeMillis();
        Long last = buyCancelAlertAt.get(key);
        if (last != null && now - last < alertRepeatMillis) return;
        buyCancelAlertAt.put(key, now);
        String body = "Symbol=" + pending.getSymbol() + " Broker=" + broker + " BUY=" + orderId
                + " Filled=" + pending.getFilledQty() + "/" + pending.getRequestQty() + ". " + message;
        telegram.systemAlert("Momentum EOD pending BUY — " + pending.getSymbol(), body);
        log.error("[MomentumTrailing] {}", body);
    }

    private void synchronizePositionBasis(MomentumTrailingState state, Position position) {
        if (position.getAveragePrice() <= 0 || position.getTotalQuantity() <= 0) {
            alert(position, "INVALID_POSITION_BASIS", "Average BUY price or remaining quantity is invalid.");
            return;
        }
        boolean quantityChanged = state.getTrackedQuantity() != position.getTotalQuantity();
        if (Double.compare(state.getAverageBuyPrice(), position.getAveragePrice()) != 0
                || quantityChanged) {
            state.setAverageBuyPrice(position.getAveragePrice());
            state.setTrackedQuantity(position.getTotalQuantity());
            state.setActivationPrice(position.getAveragePrice() * (1.0 + ACTIVATION_PERCENT / 100.0));
            // An accumulation changes both the average cost and the quantity covered
            // by the broker stop. Force a cancel/reconcile/replace even if the stop
            // trigger itself did not move; never leave newly bought shares uncovered.
            if (quantityChanged && state.isActivated() && !state.isExitTriggered()) {
                state.setOrderResizeRequired(true);
            }
            // A higher blended cost basis may increase the required stop. Never lower
            // an already-established trigger after an accumulation or reconciliation.
            if (state.isActivated() && !state.isExitTriggered()) {
                double initial = position.getAveragePrice() * (1.0 + INITIAL_TRIGGER_PERCENT / 100.0);
                state.setDesiredTriggerPrice(Math.max(state.getDesiredTriggerPrice(), initial));
            }
            stateRepository.save(state);
        }
    }

    private void updateTrailingLevels(MomentumTrailingState state, Position position, double ltp) {
        double average = position.getAveragePrice();
        if (average <= 0 || state.isEodExitRequested() || state.isExitTriggered()) return;
        double activation = average * (1.0 + ACTIVATION_PERCENT / 100.0);
        if (!state.isActivated()) {
            if (ltp < activation) return;
            state.setActivated(true);
            state.setPhase("ACTIVE");
            state.setActivationPrice(activation);
            state.setHighWaterMark(ltp);
            state.setDesiredTriggerPrice(average * (1.0 + INITIAL_TRIGGER_PERCENT / 100.0));
            state.setDesiredLimitPrice(state.getDesiredTriggerPrice() * (1.0 - LIMIT_OFFSET_PERCENT / 100.0));
            stateRepository.save(state);
            log.info("[MomentumTrailing] {} activated at LTP={} avg={}", position.getSymbol(), ltp, average);
            return;
        }

        if (ltp <= state.getHighWaterMark()) return;
        double highWater = ltp;
        double stepSize = average * TRAIL_STEP_PERCENT / 100.0;
        if (stepSize <= 0) return;
        long completedSteps = Math.max(0L,
                (long) Math.floor((highWater - state.getActivationPrice()) / stepSize + 1.0e-9));
        double baseTrigger = state.getActivationPrice() - average * TRAIL_GAP_PERCENT / 100.0;
        double desiredTrigger = baseTrigger + completedSteps * stepSize;
        desiredTrigger = Math.max(state.getDesiredTriggerPrice(), desiredTrigger);
        state.setHighWaterMark(highWater);
        if (desiredTrigger > state.getDesiredTriggerPrice()) {
            state.setDesiredTriggerPrice(desiredTrigger);
            state.setDesiredLimitPrice(desiredTrigger * (1.0 - LIMIT_OFFSET_PERCENT / 100.0));
            stateRepository.save(state);
        }
    }

    private void ensureProtectiveStop(Position position, MomentumTrailingState state,
                                      double breakEven, double ltp) {
        if (state.isExitTriggered()) {
            manageExitOrder(position, state, LatestPriceHelper.getLatestPrice(position.getSymbol()), breakEven, true);
            return;
        }
        if (state.getExitOrderId() == null && position.getStopLossOrderId() != null) {
            state.setExitOrderId(position.getStopLossOrderId());
            state.setExitOrderIsStopLoss(true);
            stateRepository.save(state);
        }
        if (state.getExitOrderId() == null) {
            if (!retireLegacyTargets(position)) return;
            placeStop(position, state, breakEven);
            return;
        }
        ensurePendingOrderRecord(position, state);
        PendingOrder working = pendingOrderRepository.findByBrokerAndBrokerOrderId(
                brokerOrderRouter.brokerOf(position), state.getExitOrderId()).orElse(null);
        if (working == null) {
            alert(position, "PROTECTIVE_ORDER_LEDGER_MISSING",
                    "Cannot find the local pending-order record for protective SELL " + state.getExitOrderId());
            return;
        }
        if (working.getState() == com.satyam.trading2.datamodel.PendingOrderState.COMPLETE) {
            alert(position, "PROTECTIVE_ORDER_COMPLETE_UNRECONCILED",
                    "Broker reports protective SELL complete but position is still open locally. Waiting for reconciliation.");
            return;
        }
        if (working.getFilledQty() > 0
                || working.getState() == com.satyam.trading2.datamodel.PendingOrderState.PARTIALLY_FILLED) {
            // Any broker-confirmed fill on the protective SELL means the stop has
            // activated. Freeze trailing and manage only the broker-reconciled remainder.
            state.setExitTriggered(true);
            state.setPhase("EXIT_TRIGGERED");
            stateRepository.save(state);
            manageExitOrder(position, state, ltp, breakEven, true);
            return;
        }
        if (working.getState() == com.satyam.trading2.datamodel.PendingOrderState.CANCELLED
                || working.getState() == com.satyam.trading2.datamodel.PendingOrderState.REJECTED) {
            if (!brokerOrderRouter.cancelAndConfirmSellForReplacement(position, state.getExitOrderId(), 3000L)) {
                alert(position, "PROTECTIVE_ORDER_TERMINAL_UNRECONCILED",
                        "Protective SELL is terminal but its fills/cancellation are not reconciled. Replacement blocked.");
                return;
            }
            clearCurrentExitOrder(position, state);
            placeStop(position, state, breakEven);
            return;
        }
        if (state.isExitTriggered()) {
            manageExitOrder(position, state, LatestPriceHelper.getLatestPrice(position.getSymbol()), breakEven, true);
            return;
        }
        if (state.isOrderResizeRequired()
                || (state.getWorkingLimitPrice() > 0 && state.getWorkingLimitPrice() + 1.0e-9 < breakEven)
                || (state.getWorkingTriggerPrice() > 0
                    && state.getDesiredTriggerPrice() > state.getWorkingTriggerPrice() + tickSize(position.getSymbol()) / 2.0)) {
            if (!brokerOrderRouter.cancelAndConfirmSellForReplacement(
                    position, state.getExitOrderId(), 3000L)) {
                alert(position, "TRAILING_STOP_REPLACE_BLOCKED",
                        "Existing SELL has not reached a broker-confirmed terminal/reconciled state. Stop replacement is blocked.");
                return;
            }
            clearCurrentExitOrder(position, state);
            if (state.isExitTriggered()) {
                manageExitOrder(position, state, LatestPriceHelper.getLatestPrice(position.getSymbol()), breakEven, true);
                return;
            }
            placeStop(position, state, breakEven);
        }
    }

    private boolean retireLegacyTargets(Position position) {
        String target = position.getTargetOrderId();
        String secondary = position.getSecondaryTargetOrderId();
        if (target != null && !brokerOrderRouter.cancelAndConfirmSell(position, target, 3000L)) {
            alert(position, "LEGACY_TARGET_UNRESOLVED",
                    "Legacy fixed target SELL " + target + " is not confirmed cancelled with zero fills.");
            return false;
        }
        if (secondary != null && !brokerOrderRouter.cancelAndConfirmSell(position, secondary, 3000L)) {
            alert(position, "LEGACY_SECONDARY_SELL_UNRESOLVED",
                    "Legacy secondary SELL " + secondary + " is not confirmed cancelled with zero fills.");
            return false;
        }
        if (target != null) position.setTargetOrderId(null);
        if (secondary != null) {
            position.setSecondaryTargetOrderId(null);
            position.setSecondaryTargetQty(0);
            position.setSecondaryTargetPlaced(false);
        }
        position.setTargetPlaced(false);
        positionManager.updatePosition(position);
        return true;
    }

    private void placeStop(Position position, MomentumTrailingState state, double breakEven) {
        PricePair prices = stopPrices(position.getSymbol(), state.getDesiredTriggerPrice(),
                state.getDesiredLimitPrice(), breakEven);
        if (prices == null) {
            alert(position, "INVALID_SL_LIMIT_PRICES",
                    "Tick-size rounding cannot produce a valid SELL SL-LIMIT with limit at/above net break-even.");
            return;
        }
        String orderId = brokerOrderRouter.placeStopLossSell(position, position.getSymbol(),
                prices.trigger, prices.limit, position.getTotalQuantity(), position.getStrategy());
        if (orderId == null) {
            alert(position, "SL_LIMIT_SUBMISSION_FAILED",
                    "Broker did not confirm acceptance of the protective SELL SL-LIMIT order.");
            return;
        }
        position.setStopLossOrderId(orderId);
        positionManager.updatePosition(position);
        state.setExitOrderId(orderId);
        state.setExitOrderIsStopLoss(true);
        state.setWorkingTriggerPrice(prices.trigger);
        state.setWorkingLimitPrice(prices.limit);
        state.setWorkingOrderQuantity(position.getTotalQuantity());
        state.setOrderResizeRequired(false);
        state.setLastOrderActionAt(System.currentTimeMillis());
        state.setPhase("ACTIVE");
        stateRepository.save(state);
        persistPendingOrder(position, orderId, prices.limit, state.getWorkingOrderQuantity());
        log.info("[MomentumTrailing] Protective SL-LIMIT placed {} qty={} trigger={} limit={}",
                position.getSymbol(), position.getTotalQuantity(), prices.trigger, prices.limit);
    }

    private void manageExitOrder(Position position, MomentumTrailingState state,
                                 double ltp, double breakEven, boolean trailingTriggered) {
        if (state.getExitOrderId() == null && position.getStopLossOrderId() != null) {
            state.setExitOrderId(position.getStopLossOrderId());
            state.setExitOrderIsStopLoss(state.getWorkingTriggerPrice() > 0);
            stateRepository.save(state);
        }
        if (state.getExitOrderId() != null) {
            ensurePendingOrderRecord(position, state);
        }
        if (state.isExitOrderIsStopLoss() && state.getExitOrderId() != null
                && state.getWorkingLimitPrice() + 1.0e-9 < breakEven) {
            if (!brokerOrderRouter.cancelAndConfirmSellForReplacement(
                    position, state.getExitOrderId(), 3000L)) {
                alert(position, "BE_FLOOR_STOP_CANCEL_UNCONFIRMED",
                        "Protective SELL limit is below the updated break-even floor, but broker cancellation/fill reconciliation is not confirmed. No replacement was submitted.");
                return;
            }
            clearCurrentExitOrder(position, state);
        }
        if (ltp + 1.0e-9 < breakEven) {
            if (!"EXIT_BLOCKED".equals(state.getPhase())) {
                state.setPhase("EXIT_BLOCKED");
                stateRepository.save(state);
            }
            alert(position, trailingTriggered ? "TRAILING_EXIT_BELOW_BREAK_EVEN" : "EOD_EXIT_BELOW_BREAK_EVEN",
                    "LTP is below estimated net break-even ₹" + String.format("%.2f", breakEven)
                            + ". Strict protection is active: no lower SELL limit will be submitted. Position remains unresolved.");
            return;
        }

        long now = System.currentTimeMillis();
        if (now - state.getLastOrderActionAt() < actionIntervalMillis) return;

        // Do not churn an already-working LIMIT order. Reprice only when the market
        // has moved down enough that the current SELL limit is no longer competitive.
        double desiredLimit = ceilToTick(position.getSymbol(), Math.max(breakEven, ltp));
        if (desiredLimit < breakEven || desiredLimit <= 0) {
            alert(position, "EXIT_LIMIT_ROUNDING_BLOCKED",
                    "Cannot round the exit limit to a valid tick without crossing net break-even.");
            return;
        }
        double tick = tickSize(position.getSymbol());
        boolean needsReplacement = state.getExitOrderId() == null
                || state.isExitOrderIsStopLoss()
                || (state.getWorkingLimitPrice() > 0
                    && desiredLimit < state.getWorkingLimitPrice() - Math.max(tick / 2.0, 0.000001));
        if (state.getExitOrderId() != null) {
            PendingOrder working = pendingOrderRepository.findByBrokerAndBrokerOrderId(
                    brokerOrderRouter.brokerOf(position), state.getExitOrderId()).orElse(null);
            if (working == null) {
                alert(position, "EXIT_ORDER_LEDGER_MISSING",
                        "Cannot find local pending-order record for exit SELL " + state.getExitOrderId());
                return;
            }
            if (working.getState() == com.satyam.trading2.datamodel.PendingOrderState.COMPLETE) {
                alert(position, "EXIT_ORDER_COMPLETE_UNRECONCILED",
                        "Broker reports exit SELL complete but the position is still open locally. Waiting for reconciliation.");
                return;
            }
            if (working.getState() == com.satyam.trading2.datamodel.PendingOrderState.CANCELLED
                    || working.getState() == com.satyam.trading2.datamodel.PendingOrderState.REJECTED) {
                needsReplacement = true;
            }
        }
        if (!needsReplacement) return;

        if (state.getExitOrderId() != null) {
            if (!brokerOrderRouter.cancelAndConfirmSellForReplacement(
                    position, state.getExitOrderId(), 3000L)) {
                alert(position, "EXIT_ORDER_RECONCILIATION_PENDING",
                        "Triggered/EOD SELL is not terminal with all cumulative fills reconciled. No replacement order was submitted.");
                return;
            }
            clearCurrentExitOrder(position, state);
        }

        if (!position.isOpen() || position.getTotalQuantity() <= 0) {
            state.setPhase("CLOSED");
            stateRepository.save(state);
            return;
        }

        // The current Kite WebSocket integration stores LTP but not best bid depth.
        // LTP is therefore only a quote proxy. Live enablement is OFF by default until
        // bid-depth sourcing and broker-specific fills are independently verified.
        double limit = desiredLimit;
        String orderId = brokerOrderRouter.placeLimitSell(position, position.getSymbol(),
                limit, position.getTotalQuantity(), false, position.getStrategy());
        if (orderId == null) {
            alert(position, "EXIT_LIMIT_SUBMISSION_FAILED",
                    "Broker did not confirm acceptance of the repriced remaining-quantity SELL.");
            return;
        }
        position.setStopLossOrderId(orderId);
        positionManager.updatePosition(position);
        state.setExitOrderId(orderId);
        state.setExitOrderIsStopLoss(false);
        state.setWorkingTriggerPrice(0);
        state.setWorkingLimitPrice(limit);
        state.setWorkingOrderQuantity(position.getTotalQuantity());
        state.setLastOrderActionAt(now);
        state.setPhase(state.isEodExitRequested() ? "EOD_EXIT" : "EXIT_REPRICING");
        stateRepository.save(state);
        persistPendingOrder(position, orderId, limit, state.getWorkingOrderQuantity());
    }

    private void clearCurrentExitOrder(Position position, MomentumTrailingState state) {
        position.setStopLossOrderId(null);
        positionManager.updatePosition(position);
        state.setExitOrderId(null);
        state.setExitOrderIsStopLoss(false);
        state.setWorkingTriggerPrice(0);
        state.setWorkingLimitPrice(0);
        state.setWorkingOrderQuantity(0);
        state.setLastOrderActionAt(System.currentTimeMillis());
        stateRepository.save(state);
    }

    private void ensurePendingOrderRecord(Position position, MomentumTrailingState state) {
        String orderId = state.getExitOrderId();
        if (orderId == null) return;
        String broker = brokerOrderRouter.brokerOf(position);
        if (pendingOrderRepository.findByBrokerAndBrokerOrderId(broker, orderId).isPresent()) return;
        double requestedPrice = state.getWorkingLimitPrice() > 0
                ? state.getWorkingLimitPrice() : state.getDesiredLimitPrice();
        int requestedQuantity = state.getWorkingOrderQuantity();
        if (requestedQuantity <= 0) {
            alert(position, "SELL_LEDGER_RECOVERY_QUANTITY_MISSING",
                    "Cannot reconstruct the original accepted SELL quantity. Manual broker reconciliation required.");
            throw new IllegalStateException("Missing original quantity for accepted SELL " + orderId);
        }
        persistPendingOrder(position, orderId, requestedPrice, requestedQuantity);
    }

    private void persistPendingOrder(Position position, String orderId, double requestedPrice,
                                     int requestedQuantity) {
        String broker = brokerOrderRouter.brokerOf(position);
        if (pendingOrderRepository.findByBrokerAndBrokerOrderId(broker, orderId).isPresent()) return;
        PendingOrder pending = brokerOrderRouter.createSellPending(position, orderId,
                position.getSymbol(), position.getStrategy(), requestedQuantity,
                requestedPrice, false);
        try {
            pendingOrderRepository.save(pending);
        } catch (IOException e) {
            alert(position, "SELL_LEDGER_PERSISTENCE_FAILED",
                    "Broker accepted SELL " + orderId + " but the local pending-order ledger write failed. Do not submit another SELL; reconcile immediately.");
            throw new IllegalStateException("Could not persist accepted momentum SELL " + orderId, e);
        }
    }

    private PricePair stopPrices(String symbol, double rawTrigger, double rawLimit, double breakEven) {
        double tick = tickSize(symbol);
        if (tick <= 0 || rawTrigger <= 0 || rawLimit <= 0) return null;
        // Round a SELL stop trigger upward so tick rounding does not weaken the intended protection.
        double trigger = roundToTick(rawTrigger, tick, RoundingMode.CEILING);
        double limit = roundToTick(rawLimit, tick, RoundingMode.CEILING);
        if (limit >= trigger) limit = roundToTick(trigger - tick, tick, RoundingMode.FLOOR);
        if (limit < breakEven || limit <= 0 || trigger <= limit) return null;
        return new PricePair(trigger, limit);
    }

    private double netBreakEven(Position position) {
        String key = "momentum.trailing.round-trip-cost-percent." + brokerOf(position);
        String configured = environment.getProperty(key);
        if (configured == null || configured.trim().isEmpty()) return -1;
        try {
            double percent = Double.parseDouble(configured.trim());
            if (!Double.isFinite(percent) || percent < 0 || percent > 5) return -1;
            return position.getAveragePrice() * (1.0 + percent / 100.0);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private String brokerOf(Position position) {
        return position.getBroker() != null ? position.getBroker() : "KITE";
    }

    private double tickSize(String symbol) {
        Instrument instrument = Nifty500Stocks.Nifty500SymbolToInstrument.get(
                symbol == null ? "" : symbol.toUpperCase(java.util.Locale.ROOT));
        return instrument != null && instrument.getTickSize() > 0 ? instrument.getTickSize() : 0;
    }

    private double ceilToTick(String symbol, double price) {
        double tick = tickSize(symbol);
        return tick > 0 ? roundToTick(price, tick, RoundingMode.CEILING) : 0;
    }

    private double roundToTick(double price, double tick, RoundingMode mode) {
        BigDecimal steps = BigDecimal.valueOf(price).divide(BigDecimal.valueOf(tick), 0, mode);
        return steps.multiply(BigDecimal.valueOf(tick)).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    private LocalTime parseTime(String text, LocalTime fallback) {
        try { return LocalTime.parse(text); } catch (Exception e) { return fallback; }
    }

    private MomentumTrailingState getOrCreateState(Position position, String tradingDate) {
        synchronized (SellLifecycleLockRegistry.lockFor(position)) {
            return stateRepository.getOrCreate(position, tradingDate);
        }
    }

    private void alert(Position position, String code, String message) {
        // Alert throttling is per position. Critical unresolved states remain visible
        // in logs even when Telegram is disabled.
        MomentumTrailingState state = getOrCreateState(position, LocalDate.now(ZONE).toString());
        long now = System.currentTimeMillis();
        if (now - state.getLastAlertAt() < alertRepeatMillis) {
            log.debug("[MomentumTrailing][{}] {} {}: {}", code, position.getSymbol(), position.getStrategy(), message);
            return;
        }
        state.setLastAlertAt(now);
        stateRepository.save(state);
        String title = "Momentum trailing " + code + " — " + position.getSymbol();
        telegram.systemAlert(title, message + " | Broker=" + brokerOf(position)
                + " | Qty=" + position.getTotalQuantity()
                + " | LTP=₹" + String.format("%.2f", LatestPriceHelper.getLatestPrice(position.getSymbol())));
        log.warn("[MomentumTrailing][{}] {}: {}", code, position.getSymbol(), message);
    }

    private static final class PricePair {
        final double trigger;
        final double limit;
        PricePair(double trigger, double limit) {
            this.trigger = trigger;
            this.limit = limit;
        }
    }
}
