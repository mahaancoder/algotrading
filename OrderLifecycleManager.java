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
        System.out.println("orderliefcycle called : "+ event.getSymbol() + "  " + event.getSide() );
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

    private void handleComplete(PendingOrder pendingOrder, BrokerWebhookEvent event) throws IOException {
        if (pendingOrder.getState().equals(PendingOrderState.COMPLETE))  return;
        pendingOrder.markCompleted(event.getAveragePrice(), event.getFilledQuantity());
        pendingOrderRepository.save(pendingOrder);
        if(event.getSide().equals(TradeSide.BUY))
            orderCompletionProcessor.onBuyCompleted(pendingOrder);
        else if(event.getSide().equals(TradeSide.SELL))
            orderCompletionProcessor.onSellCompleted(pendingOrder);
    }

    private void handleCancelled(PendingOrder pendingOrder, BrokerWebhookEvent event) throws IOException {

        // 1. Check if already cancelled to prevent duplicate processing
        if (pendingOrder.getState().equals(PendingOrderState.CANCELLED)) {
            return;
        }

        // 2. Mark the order as cancelled
        pendingOrder.markCancelled();
        pendingOrderRepository.save(pendingOrder);

        // 3. Release reserved capital (both strategy and symbol level)
        // Calculate capital that was reserved for this order
        double reservedCapital = pendingOrder.getAvgPrice() * pendingOrder.getRequestQty();
        String symbol = pendingOrder.getSymbol();
        String strategy = pendingOrder.getStrategy();

        riskManager.releaseCapital(strategy, reservedCapital);
        riskManager.releaseCapitalForSymbol(symbol, reservedCapital);

        // 4. Remove DipState if this was a BUY order
        if (event.getSide().equals(TradeSide.BUY)) {
            dipAccumulatorService.remove(symbol, strategy);
        }
    }

    private void handleRejected(PendingOrder pendingOrder, BrokerWebhookEvent event) throws IOException {

        // 1. Check if already rejected to prevent duplicate processing
        if (pendingOrder.getState().equals(PendingOrderState.REJECTED)) {
            return;
        }

        // 2. Mark the order as rejected
        pendingOrder.markRejected();
        pendingOrderRepository.save(pendingOrder);

        // 3. Release reserved capital (both strategy and symbol level)
        // Calculate capital that was reserved for this order
        double reservedCapital = pendingOrder.getAvgPrice() * pendingOrder.getRequestQty();
        String symbol = pendingOrder.getSymbol();
        String strategy = pendingOrder.getStrategy();

        riskManager.releaseCapital(strategy, reservedCapital);
        riskManager.releaseCapitalForSymbol(symbol, reservedCapital);

        // 4. Remove DipState if this was a BUY order
        if (event.getSide().equals(TradeSide.BUY)) {
            dipAccumulatorService.remove(symbol, strategy);
        }
    }

}