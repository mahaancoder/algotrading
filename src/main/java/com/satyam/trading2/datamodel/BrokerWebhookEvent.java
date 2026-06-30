package com.satyam.trading2.datamodel;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class BrokerWebhookEvent {

    private String brokerOrderId;

    private BrokerOrderStatus status;

    private String symbol;

    private TradeSide side;

    private Integer filledQuantity;

    private Integer pendingQuantity;

    private Double averagePrice;

    private Instant exchangeTimestamp;

    private String rawPayload;
}