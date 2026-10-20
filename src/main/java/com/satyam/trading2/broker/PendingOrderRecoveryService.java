package com.satyam.trading2.broker;

import com.satyam.trading2.broker.model.BrokerOrderEvent;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.order.PendingOrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Recovers order events lost during a disconnect or JVM restart.
 *
 * WebSocket/webhook delivery remains the fast path. This poller is deliberately
 * fail-closed: missing broker, status, or cumulative-fill data never releases a
 * SELL reservation. Terminal partial fills are replayed through the same
 * BrokerOrderEventProcessor path as live events so position reduction and
 * replacement-order safety remain centralized and idempotent.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PendingOrderRecoveryService {

    private static final long MIN_ORDER_AGE_MS = 30_000L;

    private final BrokerRouter brokerRouter;
    private final PendingOrderRepository pendingOrderRepository;
    private final BrokerOrderEventProcessor brokerOrderEventProcessor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    @EventListener(ApplicationReadyEvent.class)
    public void recoverAfterStartup() {
        reconcilePendingOrders();
    }

    @Scheduled(fixedDelayString = "${trading.pending-order-recovery-delay-ms:60000}")
    public void reconcilePendingOrders() {
        // Avoid needless polling outside the regular session. Startup recovery
        // still runs once regardless of time, so overnight terminal events recover.
        LocalTime now = LocalTime.now();
        if (now.isBefore(LocalTime.of(9, 15)) || now.isAfter(LocalTime.of(15, 30))) {
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }

        try {
            List<PendingOrder> openOrders = pendingOrderRepository.findOpenOrders();
            long nowMillis = System.currentTimeMillis();
            for (PendingOrder pending : openOrders) {
                if (pending == null || pending.getBrokerOrderId() == null
                        || pending.getBrokerOrderId().trim().isEmpty()) {
                    continue;
                }
                if (nowMillis - pending.getCreatedTime() < MIN_ORDER_AGE_MS
                        && pending.getState() != PendingOrderState.PARTIALLY_FILLED) {
                    continue;
                }
                reconcileOne(pending);
            }
        } catch (RuntimeException e) {
            log.error("[PendingOrderRecovery] Reconciliation pass failed: {}", e.getMessage(), e);
        } finally {
            running.set(false);
        }
    }

    private void reconcileOne(PendingOrder pending) {
        ExecutionRoute route = routeOf(pending);
        Broker broker = brokerRouter.getBroker(route);
        if (broker == null) {
            log.warn("[PendingOrderRecovery] No broker registered for {} order {}; reservation retained",
                    route, pending.getBrokerOrderId());
            return;
        }

        try {
            BrokerOrderStatus status = broker.getOrderStatus(pending.getBrokerOrderId());
            Integer cumulativeFilled = broker.getOrderFilledQuantity(pending.getBrokerOrderId());

            // A missing order/status/fill quantity is not proof of zero fills.
            if (status == null || cumulativeFilled == null
                    || cumulativeFilled < 0 || cumulativeFilled > pending.getRequestQty()) {
                log.warn("[PendingOrderRecovery] Cannot authoritatively reconcile {} order {} "
                                + "(status={}, filled={}); reservation retained",
                        broker.getBrokerName(), pending.getBrokerOrderId(), status, cumulativeFilled);
                return;
            }

            BrokerOrderStatus normalizedStatus = status;
            if ((status == BrokerOrderStatus.OPEN || status == BrokerOrderStatus.PARTIALLY_FILLED)
                    && cumulativeFilled > 0 && cumulativeFilled < pending.getRequestQty()) {
                normalizedStatus = BrokerOrderStatus.PARTIALLY_FILLED;
            }

            // Stable OPEN/zero-fill orders need no synthetic event. For all
            // terminal states, dispatch even when the cumulative fill is unchanged:
            // this retries a completion callback that failed before restart.
            if ((normalizedStatus == BrokerOrderStatus.OPEN && cumulativeFilled == 0)
                    && cumulativeFilled == pending.getFilledQty()) {
                return;
            }
            if (normalizedStatus == BrokerOrderStatus.PARTIALLY_FILLED
                    && cumulativeFilled <= pending.getFilledQty()) {
                return;
            }

            BrokerOrderEvent recoveredEvent = BrokerOrderEvent.builder()
                    .broker(broker.getBrokerName())
                    .brokerOrderId(pending.getBrokerOrderId())
                    .status(normalizedStatus)
                    .filledQuantity(cumulativeFilled)
                    // The broker-neutral contract currently exposes cumulative fill
                    // quantity but not average fill price. Retain the last persisted
                    // price rather than inventing a price from an untrusted response.
                    .averagePrice(pending.getAvgPrice())
                    .build();
            brokerOrderEventProcessor.process(recoveredEvent);
        } catch (IOException | RuntimeException e) {
            log.error("[PendingOrderRecovery] Failed to reconcile {} order {}: {}; "
                            + "reservation remains active",
                    broker.getBrokerName(), pending.getBrokerOrderId(), e.getMessage(), e);
        }
    }

    private ExecutionRoute routeOf(PendingOrder pending) {
        if (pending.getExecutionRoute() != null) {
            return pending.getExecutionRoute();
        }
        String broker = pending.getBroker();
        if (broker != null) {
            try {
                return ExecutionRoute.valueOf(broker.trim().toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException ignored) {
                // Unknown legacy labels must not silently route to a different broker.
            }
        }
        // Legacy orders without broker metadata predate multi-broker routing and
        // were Kite orders. Explicit unknown labels above fail closed.
        if (pending.getBroker() == null || pending.getBroker().trim().isEmpty()) {
            return ExecutionRoute.KITE;
        }
        return null;
    }
}
