package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.Instrument;
import com.satyam.trading2.datamodel.MomentumTrailingState;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.helpers.LatestPriceHelper;
import com.satyam.trading2.infrastructure.messaging.BroadcastService;
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

    @Value("${momentum.trailing.enabled:false}")
    private boolean enabled;

    @Value("${momentum.trailing.live-enabled:false}")
    private boolean liveEnabled;

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
        for (Position position : positionManager.getPositionsForSymbol(symbol).values()) {
            if (position == null || !position.isOpen()
                    || position.getPositionType() != Position.PositionType.INTRADAY
                    || !MomentumTrailingPolicy.isManagedStrategy(position.getStrategy())) continue;
            MomentumTrailingState state = stateRepository.getOrCreate(position, LocalDate.now(ZONE).toString());
            synchronized (state) {
                if (!LocalDate.now(ZONE).toString().equals(state.getTradingDate())) continue;
                updateTrailingLevels(state, position, ltp);
            }
        }
    }

    @Scheduled(fixedDelayString = "${momentum.trailing.poll-ms:500}")
    public void manageMomentumPositions() {
        if (!enabled) return;
        LocalTime now = LocalTime.now(ZONE);
        LocalTime eodCutoff = parseTime(eodCutoffText, LocalTime.of(15, 0));
        List<Position> positions = positionManager.getAllOpenPositions();
        for (Position position : positions) {
            if (position == null || !position.isOpen()
                    || !MomentumTrailingPolicy.isManagedStrategy(position.getStrategy())) continue;
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
        if (ltp <= 0 || tickAt <= 0 || System.currentTimeMillis() - tickAt > ltpStaleMillis) {
            alert(position, "STALE_OR_MISSING_LTP", "Fresh LTP is unavailable; trailing/exit changes are paused.");
            return;
        }

        String today = LocalDate.now(ZONE).toString();
        MomentumTrailingState state = stateRepository.getOrCreate(position, today);
        synchronized (state) {
        if (!today.equals(state.getTradingDate())) {
            alert(position, "TRAILING_STATE_DATE_MISMATCH",
                    "Persisted trailing state belongs to another trading date. No automated order changes were made.");
            return;
        }

        synchronizePositionBasis(state, position);
        updateTrailingLevels(state, position, ltp);

        if (!state.isEodExitRequested() && !now.isBefore(eodCutoff)) {
            state.setEodExitRequested(true);
            state.setPhase("EOD_EXIT");
            stateRepository.save(state);
            alert(position, "EOD_EXIT_STARTED",
                    "New entries must stop. Momentum position is being managed for EOD exit; no holding conversion is permitted.");
        }

        // Tracking can be enabled independently, but broker order placement requires
        // a second explicit opt-in so a deploy cannot silently activate live trading.
        if (!liveEnabled) return;

        double breakEven = netBreakEven(position);
        if (breakEven <= 0) {
            alert(position, "BREAK_EVEN_NOT_CONFIGURED",
                    "Missing/invalid momentum.trailing.round-trip-cost-percent." + brokerOf(position)
                            + ". Configure the broker-specific estimated all-in round-trip cost before enabling exits.");
            return;
        }
        state.setEstimatedNetBreakEvenPrice(breakEven);
        stateRepository.save(state);

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

        if (state.isEodExitRequested()) {
            manageExitOrder(position, state, ltp, breakEven, false);
            return;
        }

        if (state.isExitTriggered()) {
            manageExitOrder(position, state, ltp, breakEven, true);
            return;
        }

        if (!state.isActivated()) return;
        if (ltp <= state.getWorkingTriggerPrice() && state.getWorkingTriggerPrice() > 0) {
            state.setExitTriggered(true);
            state.setPhase("EXIT_TRIGGERED");
            stateRepository.save(state);
            manageExitOrder(position, state, ltp, breakEven, true);
            return;
        }
        ensureProtectiveStop(position, state, breakEven);
        }
    }

    private void synchronizePositionBasis(MomentumTrailingState state, Position position) {
        if (position.getAveragePrice() <= 0 || position.getTotalQuantity() <= 0) {
            alert(position, "INVALID_POSITION_BASIS", "Average BUY price or remaining quantity is invalid.");
            return;
        }
        if (Double.compare(state.getAverageBuyPrice(), position.getAveragePrice()) != 0
                || state.getTrackedQuantity() != position.getTotalQuantity()) {
            state.setAverageBuyPrice(position.getAveragePrice());
            state.setTrackedQuantity(position.getTotalQuantity());
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
        long completedSteps = (long) Math.floor((highWater - state.getActivationPrice()) / stepSize + 1.0e-9);
        double desiredTrigger = average * (1.0 + INITIAL_TRIGGER_PERCENT / 100.0)
                + completedSteps * stepSize;
        desiredTrigger = Math.max(state.getDesiredTriggerPrice(), desiredTrigger);
        state.setHighWaterMark(highWater);
        if (desiredTrigger > state.getDesiredTriggerPrice()) {
            state.setDesiredTriggerPrice(desiredTrigger);
            state.setDesiredLimitPrice(desiredTrigger * (1.0 - LIMIT_OFFSET_PERCENT / 100.0));
            stateRepository.save(state);
        }
    }

    private void ensureProtectiveStop(Position position, MomentumTrailingState state, double breakEven) {
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
        if (state.getWorkingTriggerPrice() > 0
                && state.getDesiredTriggerPrice() > state.getWorkingTriggerPrice() + tickSize(position.getSymbol()) / 2.0) {
            if (!brokerOrderRouter.cancelAndConfirmSellForReplacement(
                    position, state.getExitOrderId(), 3000L)) {
                alert(position, "TRAILING_STOP_REPLACE_BLOCKED",
                        "Existing SELL has not reached a broker-confirmed terminal/reconciled state. Stop replacement is blocked.");
                return;
            }
            clearCurrentExitOrder(position, state);
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
        state.setLastOrderActionAt(System.currentTimeMillis());
        state.setPhase("ACTIVE");
        stateRepository.save(state);
        persistPendingOrder(position, orderId, prices.limit);
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
        if (ltp + 1.0e-9 < breakEven) {
            state.setPhase("EXIT_BLOCKED");
            stateRepository.save(state);
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
        state.setLastOrderActionAt(now);
        state.setPhase(state.isEodExitRequested() ? "EOD_EXIT" : "EXIT_REPRICING");
        stateRepository.save(state);
        persistPendingOrder(position, orderId, limit);
    }

    private void clearCurrentExitOrder(Position position, MomentumTrailingState state) {
        position.setStopLossOrderId(null);
        positionManager.updatePosition(position);
        state.setExitOrderId(null);
        state.setExitOrderIsStopLoss(false);
        state.setWorkingTriggerPrice(0);
        state.setWorkingLimitPrice(0);
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
        persistPendingOrder(position, orderId, requestedPrice);
    }

    private void persistPendingOrder(Position position, String orderId, double requestedPrice) {
        String broker = brokerOrderRouter.brokerOf(position);
        if (pendingOrderRepository.findByBrokerAndBrokerOrderId(broker, orderId).isPresent()) return;
        PendingOrder pending = brokerOrderRouter.createSellPending(position, orderId,
                position.getSymbol(), position.getStrategy(), position.getTotalQuantity(),
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
        double trigger = roundToTick(rawTrigger, tick, RoundingMode.HALF_UP);
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
        for (Instrument instrument : Nifty500Stocks.Nifty500tokenToInstrument.values()) {
            if (instrument != null && symbol.equalsIgnoreCase(instrument.getTradingSymbol())
                    && instrument.getTickSize() > 0) return instrument.getTickSize();
        }
        return 0;
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

    private void alert(Position position, String code, String message) {
        // Alert throttling is per position. Critical unresolved states remain visible
        // in logs even when Telegram is disabled.
        MomentumTrailingState state = stateRepository.getOrCreate(position, LocalDate.now(ZONE).toString());
        long now = System.currentTimeMillis();
        if (now - state.getLastAlertAt() < alertRepeatMillis) {
            log.warn("[MomentumTrailing][{}] {} {}: {}", code, position.getSymbol(), position.getStrategy(), message);
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
