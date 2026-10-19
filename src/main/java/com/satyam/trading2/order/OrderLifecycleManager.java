package com.satyam.trading2.order;

import com.satyam.trading2.datamodel.BrokerWebhookEvent;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.service.DipAccumulatorService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@RequiredArgsConstructor
public class OrderLifecycleManager {

    private final OrderCompletionProcessor orderCompletionProcessor;
    private final PendingOrderRepository pendingOrderRepository;
    private final RiskManager riskManager;
    private final DipAccumulatorService dipAccumulatorService;

    public void handle(BrokerWebhookEvent event) throws IOException {
//        System.out.println("orderliefcycle called : "+ event.getSymbol() + "  " + event.getSide() );
        PendingOrder pendingOrder = pendingOrderRepository.findByBrokerOrderId(event.getBrokerOrderId()).orElse(null);
        if (pendingOrder == null) {
            return;
        }

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
                System.out.println("Partially filled : ignore for now");
                break;

            case OPEN:
                System.out.println("OPEN : ignore for now");
                break;

            default:
        }
    }

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

    private void handleComplete(PendingOrder pendingOrder, BrokerWebhookEvent event) throws IOException {
        // Marking a SELL complete releases its pending-order reservation. Keep that
        // transition and the position quantity update in one broker-symbol critical
        // section, otherwise a scheduler can submit a replacement in between them.
        synchronized (lifecycleLock(pendingOrder)) {
            if (pendingOrder.getState().equals(PendingOrderState.COMPLETE)) return;
            pendingOrder.markCompleted(event.getAveragePrice(), event.getFilledQuantity());
            pendingOrderRepository.save(pendingOrder);
            TradeSide side = pendingOrder.getSide() != null ? pendingOrder.getSide() : event.getSide();
            if (side == TradeSide.BUY) {
                orderCompletionProcessor.onBuyCompleted(pendingOrder);
            } else if (side == TradeSide.SELL) {
                orderCompletionProcessor.onSellCompleted(pendingOrder);
            }
        }
    }

    private void handleCancelled(PendingOrder pendingOrder, BrokerWebhookEvent event) throws IOException {
        synchronized (lifecycleLock(pendingOrder)) {
            if (pendingOrder.isClosed()) return;

            String symbol = pendingOrder.getSymbol();
            String strategy = pendingOrder.getStrategy();

            TradeSide side = pendingOrder.getSide() != null ? pendingOrder.getSide() : event.getSide();

            // A CANCELLED SELL can have partial fills. Do not release its reservation
            // unless the event explicitly confirms zero fills; the local position may
            // not yet reflect any shares sold by the cancelled order.
            if (side == TradeSide.SELL
                    && !Integer.valueOf(0).equals(event.getFilledQuantity())) {
                System.err.println("🚨 [OrderLifecycleManager] SELL " + pendingOrder.getOrderId()
                        + " reported CANCELLED with unknown/non-zero filled quantity ("
                        + event.getFilledQuantity() + "). Keeping reservation and blocking new SELLs until reconciliation.");
                return;
            }

            pendingOrder.markCancelled();
            pendingOrderRepository.save(pendingOrder);

            if (side == TradeSide.BUY) {
                double reservedCapital = pendingOrder.getAvgPrice() * pendingOrder.getRequestQty();
                riskManager.releaseCapital(strategy, reservedCapital);
                riskManager.releaseCapitalForSymbol(symbol, reservedCapital);
                dipAccumulatorService.remove(symbol, strategy);
            }
        }
    }

    private void handleRejected(PendingOrder pendingOrder, BrokerWebhookEvent event) throws IOException {
        synchronized (lifecycleLock(pendingOrder)) {
            if (pendingOrder.isClosed()) return;

            String symbol = pendingOrder.getSymbol();
            String strategy = pendingOrder.getStrategy();

            TradeSide side = pendingOrder.getSide() != null ? pendingOrder.getSide() : event.getSide();

            // Reject is only safe to retire when the broker explicitly reports zero
            // fills. Unknown fill data fails closed for SELLs just like cancellation.
            if (side == TradeSide.SELL
                    && !Integer.valueOf(0).equals(event.getFilledQuantity())) {
                System.err.println("🚨 [OrderLifecycleManager] SELL " + pendingOrder.getOrderId()
                        + " reported REJECTED with unknown/non-zero filled quantity ("
                        + event.getFilledQuantity() + "). Keeping reservation pending reconciliation.");
                return;
            }

            pendingOrder.markRejected();
            pendingOrderRepository.save(pendingOrder);

            if (side == TradeSide.BUY) {
                double reservedCapital = pendingOrder.getAvgPrice() * pendingOrder.getRequestQty();
                riskManager.releaseCapital(strategy, reservedCapital);
                riskManager.releaseCapitalForSymbol(symbol, reservedCapital);
                dipAccumulatorService.remove(symbol, strategy);
            }
        }
    }

}