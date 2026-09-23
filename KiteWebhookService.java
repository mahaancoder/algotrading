package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.BrokerOrderStatus;
import com.satyam.trading2.datamodel.BrokerWebhookEvent;
import com.satyam.trading2.datamodel.KiteWebhookPayload;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.order.KiteChecksumValidator;
import com.satyam.trading2.order.OrderLifecycleManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;


@Service
@RequiredArgsConstructor
public class KiteWebhookService {

    private final OrderLifecycleManager orderLifecycleManager;
    private final KiteChecksumValidator checksumValidator;

    public void process(String rawBody, KiteWebhookPayload payload) throws IOException {
//        validateChecksum(payload);
//        System.out.println("starting webhook processing");
        BrokerWebhookEvent event = map(payload, rawBody);
        orderLifecycleManager.handle(event);
    }

    private void validateChecksum(KiteWebhookPayload payload) {
        if (!checksumValidator.isValid(payload)) {
            System.out.println("checksum is not valid");
            throw new RuntimeException("Invalid checksum");
        }
    }

    private BrokerWebhookEvent map(KiteWebhookPayload payload, String rawBody) {

        return BrokerWebhookEvent.builder()
                .brokerOrderId(payload.getOrderId())
                .status(mapStatus(payload.getStatus(), payload.getFilledQuantity(), payload.getQuantity()))
                .symbol(payload.getTradingSymbol())
                .side(mapSide(payload.getTransactionType()))
                .filledQuantity(payload.getFilledQuantity())
                .pendingQuantity(payload.getPendingQuantity())
                .averagePrice(payload.getAveragePrice())
                .rawPayload(rawBody)
                .build();
    }

    private BrokerOrderStatus mapStatus(String kiteStatus, int filledQty, int totalQty){
        switch (kiteStatus){
            case "COMPLETE":
                return BrokerOrderStatus.COMPLETE;
            case "REJECTED":
                return BrokerOrderStatus.REJECTED;
            case "CANCELLED":
                return BrokerOrderStatus.CANCELLED;
            case "OPEN":
            case "UPDATE":
                if (filledQty > 0 && filledQty < totalQty) {
                    return BrokerOrderStatus.PARTIALLY_FILLED;
                } else {
                    return BrokerOrderStatus.OPEN;
                }
            default:
                throw new IllegalArgumentException("Unknown status: " + kiteStatus);

        }
    }

    private TradeSide mapSide(String kiteSide) {
        switch (kiteSide) {
            case "BUY":
                return TradeSide.BUY;
            case "SELL":
                return TradeSide.SELL;
            default:
                throw new IllegalArgumentException("Unknown side: " + kiteSide);
        }
    }
}