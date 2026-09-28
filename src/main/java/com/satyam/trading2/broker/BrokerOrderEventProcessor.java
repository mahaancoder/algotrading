package com.satyam.trading2.broker;

import com.satyam.trading2.broker.model.BrokerOrderEvent;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.order.OrderCompletionProcessor;
import com.satyam.trading2.order.PendingOrderRepository;
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
                System.out.println("[BrokerOrderEventProcessor] Partially filled — ignoring for now: "
                        + pendingOrder.getSymbol() + " " + event.getBrokerOrderId());
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

    private void handleComplete(PendingOrder pendingOrder, BrokerOrderEvent event) throws IOException {
        if (pendingOrder.getState().equals(PendingOrderState.COMPLETE)) {
            System.out.println("[BrokerOrderEventProcessor] Already COMPLETE — idempotency skip: "
                    + pendingOrder.getOrderId());
            return;
        }

        pendingOrder.markCompleted(event.getAveragePrice(), event.getFilledQuantity());
        pendingOrderRepository.save(pendingOrder);

        // Read side from the PendingOrder (source of truth), not the event
        TradeSide side = pendingOrder.getSide();
        if (side == null) {
            System.out.println("[BrokerOrderEventProcessor] PendingOrder has null side — cannot dispatch: "
                    + pendingOrder.getOrderId());
            return;
        }

        if (side.equals(TradeSide.BUY)) {
            orderCompletionProcessor.onBuyCompleted(pendingOrder);
        } else if (side.equals(TradeSide.SELL)) {
            orderCompletionProcessor.onSellCompleted(pendingOrder);
        }
    }

    private void handleCancelled(PendingOrder pendingOrder, BrokerOrderEvent event) throws IOException {
        if (pendingOrder.getState().equals(PendingOrderState.CANCELLED)) {
            return;
        }

        pendingOrder.markCancelled();
        pendingOrderRepository.save(pendingOrder);

        releaseCapitalAndCleanup(pendingOrder, event);
    }

    private void handleRejected(PendingOrder pendingOrder, BrokerOrderEvent event) throws IOException {
        if (pendingOrder.getState().equals(PendingOrderState.REJECTED)) {
            return;
        }

        pendingOrder.markRejected();
        if (event.getRejectionReason() != null) {
            pendingOrder.setRejectionReason(event.getRejectionReason());
        }
        pendingOrderRepository.save(pendingOrder);

        releaseCapitalAndCleanup(pendingOrder, event);
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

        riskManager.releaseCapital(strategy, reservedCapital, broker);
        riskManager.releaseCapitalForSymbol(symbol, reservedCapital);

        // Remove DipState if this was a BUY order
        if (pendingOrder.getSide() != null && pendingOrder.getSide().equals(TradeSide.BUY)) {
            dipAccumulatorService.remove(symbol, strategy);
        }
    }
}
