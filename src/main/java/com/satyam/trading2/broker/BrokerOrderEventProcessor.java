package com.satyam.trading2.broker;

import com.satyam.trading2.broker.model.BrokerOrderEvent;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.order.OrderCompletionProcessor;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.order.SellLifecycleLockRegistry;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.service.DipAccumulatorService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * BrokerOrderEventProcessor — the single, broker-neutral order-status dispatcher.
 *
 * All three brokers (Kite webhook, INDstocks WebSocket, Shoonya WebSocket)
 * normalize their order-update events into BrokerOrderEvent and call
 * process(event) here. This replaces OrderLifecycleManager.
 *
 * Key differences from OrderLifecycleManager:
 *  - Looks up PendingOrder by (broker, brokerOrderId) — no cross-broker collision.
 *  - Reads `side` (BUY/SELL) from the PendingOrder, not from the event, since
 *    BrokerOrderEvent intentionally drops side (the PO is the source of truth).
 *  - Feeds OrderCompletionProcessor.onBuyCompleted / onSellCompleted unchanged.
 *
 * Idempotency: each state transition checks the current state and returns
 * early if already in that state, preventing duplicate processing of
 * duplicate broker events (common with WebSocket reconnects).
 */
@Service
@RequiredArgsConstructor
public class BrokerOrderEventProcessor {

    // Lazy injections break the bean-creation cycle that runs through the
    // broker registry:
    //   riskManager → marginFetchScheduler → brokerRouter → (broker) →
    //   brokerOrderEventProcessor → {orderCompletionProcessor, riskManager}.
    // Both collaborators are only used at runtime (order-status handling),
    // never during construction, so lazy proxies are safe.
    @Lazy
    @Autowired
    private OrderCompletionProcessor orderCompletionProcessor;
    private final PendingOrderRepository pendingOrderRepository;
    @Lazy
    @Autowired
    private RiskManager riskManager;
    private final DipAccumulatorService dipAccumulatorService;

    public void process(BrokerOrderEvent event) throws IOException {
        if (event == null || event.getBrokerOrderId() == null) {
            System.out.println("[BrokerOrderEventProcessor] Null event or brokerOrderId — ignoring");
            return;
        }

        String broker = event.getBroker() != null ? event.getBroker() : "KITE";
        PendingOrder pendingOrder = pendingOrderRepository
                .findByBrokerAndBrokerOrderId(broker, event.getBrokerOrderId())
                .orElse(null);

        if (pendingOrder == null) {
            // Visible (was DEBUG): a broker DID push an order update, but no stored
            // PendingOrder matched (broker, brokerOrderId). For INDstocks this means the
            // WS fill event arrived yet the buy's saved brokerOrderId differs from the
            // order_id on the feed — so the position/target-sell chain never fires.
            System.out.println("[BrokerOrderEventProcessor] No PendingOrder for " + broker + "::"
                    + event.getBrokerOrderId() + " (status=" + event.getStatus()
                    + ") — event received but unmatched, ignoring");
            return;
        }

        System.out.println("[BrokerOrderEventProcessor] " + broker + " " + pendingOrder.getSymbol() + " "
                + event.getBrokerOrderId() + " → status=" + event.getStatus());

        switch (event.getStatus()) {
            case COMPLETE:
                handleComplete(pendingOrder, event);
                break;

            case REJECTED:
                handleRejected(pendingOrder, event);
                break;

            case CANCELLED:
                handleCancelled(pendingOrder, event);
                break;

            case PARTIALLY_FILLED:
                handlePartiallyFilled(pendingOrder, event);
                break;

            case OPEN:
                System.out.println("[BrokerOrderEventProcessor] OPEN — ignoring: " + broker + " "
                        + event.getBrokerOrderId());
                break;

            default:
                System.out.println("[BrokerOrderEventProcessor] Unknown status " + event.getStatus()
                        + " for " + broker + " " + event.getBrokerOrderId());
        }
    }

    // ─────────────────────────────────────────────────────────────────────

    private Object lifecycleLock(PendingOrder pendingOrder) {
        String broker = pendingOrder.getBroker();
        if (broker == null || broker.trim().isEmpty()) {
            broker = pendingOrder.getExecutionRoute() != null
                    ? pendingOrder.getExecutionRoute().name() : "KITE";
        }
        return SellLifecycleLockRegistry.lockFor(
                pendingOrder.getSymbol(), pendingOrder.getStrategy(), broker,
                pendingOrder.getPositionType());
    }

    private void handleComplete(PendingOrder pendingOrder, BrokerOrderEvent event) throws IOException {
        // Keep reservation release and the position-quantity update atomic with
        // respect to scheduler/manual SELL placement for this broker + symbol.
        synchronized (lifecycleLock(pendingOrder)) {
            if (pendingOrder.getState().equals(PendingOrderState.COMPLETE)
                    && pendingOrder.isCompletionProcessed()) {
                System.out.println("[BrokerOrderEventProcessor] Already COMPLETE and processed — idempotency skip: "
                        + pendingOrder.getOrderId());
                return;
            }

            if (!pendingOrder.getState().equals(PendingOrderState.COMPLETE)) {
                if (event.getFilledQuantity() == null
                        || event.getFilledQuantity() != pendingOrder.getRequestQty()) {
                    System.err.println("[BrokerOrderEventProcessor] COMPLETE event for "
                            + pendingOrder.getOrderId() + " has unknown/mismatched fill quantity "
                            + event.getFilledQuantity() + " (requested " + pendingOrder.getRequestQty()
                            + "); retaining reservation until reconciliation.");
                    return;
                }
                PendingOrderState previousState = pendingOrder.getState();
                int previousFilledQty = pendingOrder.getFilledQty();
                double previousAvgPrice = pendingOrder.getAvgPrice();
                long previousUpdatedTime = pendingOrder.getUpdatedTime();
                pendingOrder.markCompleted(event.getAveragePrice(), event.getFilledQuantity());
                try {
                    pendingOrderRepository.save(pendingOrder);
                } catch (IOException persistenceError) {
                    pendingOrder.setState(previousState);
                    pendingOrder.setFilledQty(previousFilledQty);
                    pendingOrder.setAvgPrice(previousAvgPrice);
                    pendingOrder.setUpdatedTime(previousUpdatedTime);
                    throw persistenceError;
                }
            }

            TradeSide side = pendingOrder.getSide();
            if (side == null) {
                System.err.println("[BrokerOrderEventProcessor] PendingOrder has null side — retaining reservation: "
                        + pendingOrder.getOrderId());
                restoreReservationIfUnprocessed(pendingOrder, null);
                return;
            }

            try {
                if (side == TradeSide.BUY) {
                    orderCompletionProcessor.onBuyCompleted(pendingOrder);
                } else if (side == TradeSide.SELL) {
                    orderCompletionProcessor.onSellCompleted(pendingOrder);
                }

                // A completion callback can return early (e.g. position/order ID
                // mismatch). Never leave a COMPLETE-but-unprocessed SELL unreserved.
                if (!pendingOrder.isCompletionProcessed()) {
                    System.err.println("[BrokerOrderEventProcessor] Completion callback did not process "
                            + pendingOrder.getOrderId() + "; retaining reservation for reconciliation.");
                    restoreReservationIfUnprocessed(pendingOrder, null);
                }
            } catch (IOException | RuntimeException e) {
                restoreReservationIfUnprocessed(pendingOrder, e);
                throw e;
            }
        }
    }

    private void restoreReservationIfUnprocessed(PendingOrder pendingOrder, Throwable originalFailure) {
        if (pendingOrder.isCompletionProcessed()) return;
        pendingOrder.setState(PendingOrderState.PENDING);
        try {
            pendingOrderRepository.save(pendingOrder);
        } catch (IOException persistError) {
            if (originalFailure != null) {
                originalFailure.addSuppressed(persistError);
            }
            System.err.println("[BrokerOrderEventProcessor] CRITICAL: unable to persist retained SELL reservation "
                    + pendingOrder.getOrderId() + ": " + persistError.getMessage());
        }
    }

    /**
     * Persist broker cumulative partial fills without applying them to Position
     * yet. Position mutation is intentionally deferred until COMPLETE because the
     * current completion callbacks are order-level/idempotent; applying deltas here
     * and then again on COMPLETE would double-count. SELL capacity uses the fresh
     * broker inventory and this cumulative filled quantity to reserve only the
     * unfilled remainder. A cancelled partial SELL remains unresolved until the
     * position reconciliation path confirms its resulting inventory.
     */
    private void handlePartiallyFilled(PendingOrder pendingOrder, BrokerOrderEvent event) throws IOException {
        synchronized (lifecycleLock(pendingOrder)) {
            if (pendingOrder.isClosed()) return;
            Integer cumulative = event.getFilledQuantity();
            if (cumulative == null || cumulative <= 0 || cumulative >= pendingOrder.getRequestQty()) {
                System.err.println("[BrokerOrderEventProcessor] Invalid partial fill quantity for "
                        + pendingOrder.getOrderId() + ": " + cumulative + "; retaining current state.");
                return;
            }
            if (cumulative <= pendingOrder.getFilledQty()) return;

            PendingOrderState previousState = pendingOrder.getState();
            int previousFilledQty = pendingOrder.getFilledQty();
            double previousAvgPrice = pendingOrder.getAvgPrice();
            long previousUpdatedTime = pendingOrder.getUpdatedTime();
            pendingOrder.markPartiallyFilled(cumulative,
                    event.getAveragePrice() > 0 ? event.getAveragePrice() : pendingOrder.getAvgPrice());
            if (pendingOrder.getFilledQty() != cumulative) return;
            try {
                pendingOrderRepository.save(pendingOrder);
            } catch (IOException persistenceError) {
                pendingOrder.setState(previousState);
                pendingOrder.setFilledQty(previousFilledQty);
                pendingOrder.setAvgPrice(previousAvgPrice);
                pendingOrder.setUpdatedTime(previousUpdatedTime);
                throw persistenceError;
            }
        }
    }

    private void handleCancelled(PendingOrder pendingOrder, BrokerOrderEvent event) throws IOException {
        synchronized (lifecycleLock(pendingOrder)) {
            if (pendingOrder.isClosed()) {
                handleLateTerminalSellFill(pendingOrder, event, "cancelled");
                return;
            }

            // A terminal CANCELLED SELL can still have executed shares. Persist the
            // broker's cumulative fill before returning, otherwise a cancellation-only
            // event (with no preceding PARTIALLY_FILLED event) leaves filledQty at zero
            // and a later SELL may treat the stale local position as fully sellable.
            if (pendingOrder.getSide() == TradeSide.SELL
                    && Integer.valueOf(pendingOrder.getRequestQty()).equals(event.getFilledQuantity())) {
                // Some broker feeds can deliver COMPLETE quantity with a terminal
                // CANCELLED/REJECTED status due to event ordering. The cumulative
                // quantity is authoritative: process the full execution exactly once.
                handleComplete(pendingOrder, completionEvent(event));
                return;
            }
            if (pendingOrder.getSide() == TradeSide.SELL
                    && (!Integer.valueOf(0).equals(event.getFilledQuantity()) || pendingOrder.getFilledQty() != 0)) {
                persistUnresolvedTerminalSellFill(pendingOrder, event, "cancelled");
                return;
            }

            PendingOrderState previousState = pendingOrder.getState();
            long previousUpdatedTime = pendingOrder.getUpdatedTime();
            pendingOrder.markCancelled();
            try {
                pendingOrderRepository.save(pendingOrder);
            } catch (IOException persistenceError) {
                pendingOrder.setState(previousState);
                pendingOrder.setUpdatedTime(previousUpdatedTime);
                throw persistenceError;
            }
            releaseCapitalAndCleanup(pendingOrder, event);
        }
    }

    private BrokerOrderEvent completionEvent(BrokerOrderEvent event) {
        return BrokerOrderEvent.builder()
                .broker(event.getBroker())
                .brokerOrderId(event.getBrokerOrderId())
                .status(BrokerOrderStatus.COMPLETE)
                .filledQuantity(event.getFilledQuantity())
                .averagePrice(event.getAveragePrice())
                .rejectionReason(event.getRejectionReason())
                .build();
    }

    /**
     * Record cumulative executions carried by a terminal SELL event before
     * leaving the order unresolved. Never retire a terminal SELL as zero-fill
     * unless the broker explicitly confirms zero; a later lifecycle/reconciliation
     * pass must account for these shares before another SELL can be authorized.
     */
    private void persistUnresolvedTerminalSellFill(PendingOrder pendingOrder,
                                                    BrokerOrderEvent event,
                                                    String terminalStatus) throws IOException {
        Integer cumulative = event.getFilledQuantity();
        if (cumulative == null) {
            // A prior PARTIALLY_FILLED event may already have persisted the cumulative
            // quantity. Use that known quantity if the terminal event omits it.
            if (pendingOrder.getFilledQty() <= 0) {
                System.err.println("[BrokerOrderEventProcessor] SELL " + pendingOrder.getOrderId()
                        + " " + terminalStatus + " with unknown fills and no previously recorded executions"
                        + "; retaining reservation.");
                return;
            }
            cumulative = pendingOrder.getFilledQty();
        }
        if (cumulative < 0 || cumulative > pendingOrder.getRequestQty()) {
            System.err.println("[BrokerOrderEventProcessor] SELL " + pendingOrder.getOrderId()
                    + " " + terminalStatus + " with invalid cumulative fills " + cumulative
                    + "; retaining reservation.");
            return;
        }
        if (cumulative == 0 && pendingOrder.getFilledQty() == 0) {
            // Caller should only route non-zero/unknown cases here; defensive guard.
            return;
        }
        int recorded = Math.max(pendingOrder.getFilledQty(), cumulative);
        if (recorded <= 0 || recorded >= pendingOrder.getRequestQty()) {
            System.err.println("[BrokerOrderEventProcessor] SELL " + pendingOrder.getOrderId()
                    + " terminal fill quantity " + recorded
                    + " requires full order reconciliation; retaining reservation.");
            return;
        }
        if (recorded > pendingOrder.getFilledQty()) {
            PendingOrderState previousState = pendingOrder.getState();
            int previousFilledQty = pendingOrder.getFilledQty();
            double previousAvgPrice = pendingOrder.getAvgPrice();
            long previousUpdatedTime = pendingOrder.getUpdatedTime();
            double avgPrice = event.getAveragePrice() > 0
                    ? event.getAveragePrice() : previousAvgPrice;
            // markPartiallyFilled intentionally ignores terminal orders. A broker may,
            // however, deliver a late cumulative-fill update after CANCELLED/REJECTED.
            // Preserve the terminal status while recording the execution so the
            // completion processor can apply the sold quantity exactly once.
            pendingOrder.setFilledQty(recorded);
            pendingOrder.setAvgPrice(avgPrice);
            // The broker order is terminal, but the local position update may not
            // have succeeded yet. Keep it in the open/unresolved set until that
            // update is applied and completionProcessed is durably persisted.
            pendingOrder.setState(PendingOrderState.PARTIALLY_FILLED);
            pendingOrder.setUpdatedTime(System.currentTimeMillis());
            try {
                pendingOrderRepository.save(pendingOrder);
            } catch (IOException persistenceError) {
                pendingOrder.setState(previousState);
                pendingOrder.setFilledQty(previousFilledQty);
                pendingOrder.setAvgPrice(previousAvgPrice);
                pendingOrder.setUpdatedTime(previousUpdatedTime);
                throw persistenceError;
            }
        }
        // Apply terminal partial executions through the same idempotent position
        // lifecycle as a complete SELL. The completion processor reduces the long
        // position by the cumulative fill, cancels/re-arms the remaining target only
        // when safe, and marks this terminal order processed.
        try {
            orderCompletionProcessor.onSellCompleted(pendingOrder);
        } catch (RuntimeException | IOException failure) {
            System.err.println("[BrokerOrderEventProcessor] Could not apply terminal partial SELL "
                    + pendingOrder.getOrderId() + "; retaining reservation: " + failure.getMessage());
            throw failure;
        }
        if (!pendingOrder.isCompletionProcessed()) {
            System.err.println("[BrokerOrderEventProcessor] Terminal partial SELL "
                    + pendingOrder.getOrderId() + " fill=" + recorded
                    + " persisted but not applied; retaining reservation for reconciliation.");
        }
    }

    /**
     * Broker event streams can reorder terminal status and cumulative-fill updates.
     * A late CANCELLED/REJECTED event must not hide newly confirmed executions just
     * because an earlier zero-fill terminal event already closed the local order.
     */
    private void handleLateTerminalSellFill(PendingOrder pendingOrder,
                                            BrokerOrderEvent event,
                                            String terminalStatus) throws IOException {
        if (pendingOrder.getSide() != TradeSide.SELL || pendingOrder.isCompletionProcessed()) {
            return;
        }
        Integer cumulative = event.getFilledQuantity();
        if (cumulative == null || cumulative <= pendingOrder.getFilledQty()
                || cumulative <= 0 || cumulative > pendingOrder.getRequestQty()) {
            return;
        }
        if (cumulative == pendingOrder.getRequestQty()) {
            // Re-open only the local lifecycle state so the existing idempotent
            // COMPLETE handler can apply the full execution. The broker status stays
            // terminal; this is not a retry of the broker order itself.
            pendingOrder.setState(PendingOrderState.PENDING);
            handleComplete(pendingOrder, completionEvent(event));
            return;
        }
        persistUnresolvedTerminalSellFill(pendingOrder, event, terminalStatus);
    }

    private void handleRejected(PendingOrder pendingOrder, BrokerOrderEvent event) throws IOException {
        synchronized (lifecycleLock(pendingOrder)) {
            if (pendingOrder.isClosed()) {
                handleLateTerminalSellFill(pendingOrder, event, "rejected");
                return;
            }

            // Treat a rejected SELL with non-zero/unknown fills as unresolved too.
            // Some broker feeds report terminal status and cumulative quantity in
            // the same event without ever delivering a partial-fill event.
            if (pendingOrder.getSide() == TradeSide.SELL
                    && Integer.valueOf(pendingOrder.getRequestQty()).equals(event.getFilledQuantity())) {
                handleComplete(pendingOrder, completionEvent(event));
                return;
            }
            if (pendingOrder.getSide() == TradeSide.SELL && !Integer.valueOf(0).equals(event.getFilledQuantity())) {
                persistUnresolvedTerminalSellFill(pendingOrder, event, "rejected");
                return;
            }

            PendingOrderState previousState = pendingOrder.getState();
            long previousUpdatedTime = pendingOrder.getUpdatedTime();
            String previousRejectionReason = pendingOrder.getRejectionReason();
            pendingOrder.markRejected();
            if (event.getRejectionReason() != null) {
                pendingOrder.setRejectionReason(event.getRejectionReason());
            }
            try {
                pendingOrderRepository.save(pendingOrder);
            } catch (IOException persistenceError) {
                pendingOrder.setState(previousState);
                pendingOrder.setUpdatedTime(previousUpdatedTime);
                pendingOrder.setRejectionReason(previousRejectionReason);
                throw persistenceError;
            }
            releaseCapitalAndCleanup(pendingOrder, event);
        }
    }

    /**
     * Release reserved capital and clean up dip state for cancelled/rejected
     * BUY orders. Shared between handleCancelled and handleRejected.
     */
    private void releaseCapitalAndCleanup(PendingOrder pendingOrder, BrokerOrderEvent event) {
        double reservedCapital = pendingOrder.getAvgPrice() * pendingOrder.getRequestQty();
        String symbol = pendingOrder.getSymbol();
        String strategy = pendingOrder.getStrategy();
        // Release against the order's OWN broker. The broker-unaware overload
        // defaults to KITE, which left INDSTOCKS/SHOONYA reservations stuck in
        // reservedCapitalPerBrokerStrategy on cancel/reject — the per-broker
        // available capital then shrank over the day until every new signal was
        // wrongly rejected with "Insufficient <broker> broker capital".
        String broker = pendingOrder.getBroker() != null ? pendingOrder.getBroker() : "KITE";

        // SELL orders do not reserve BUY capital. Release capital only for BUY
        // cancellation/rejection; the SELL reservation is represented by the
        // PendingOrder state and is handled by the broker-symbol lifecycle lock.
        if (pendingOrder.getSide() == TradeSide.BUY) {
            riskManager.releaseCapital(strategy, reservedCapital, broker);
            riskManager.releaseCapitalForSymbol(symbol, reservedCapital);
            dipAccumulatorService.remove(symbol, strategy);
        }
    }
}
