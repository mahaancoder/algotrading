package com.satyam.trading2.datamodel;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KiteWebhookPayload {

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("status")
    private String status;

    @JsonProperty("tradingsymbol")
    private String tradingSymbol;

    @JsonProperty("transaction_type")
    private String transactionType;

    @JsonProperty("quantity")
    private Integer quantity;

    @JsonProperty("filled_quantity")
    private Integer filledQuantity;

    @JsonProperty("pending_quantity")
    private Integer pendingQuantity;

    @JsonProperty("average_price")
    private Double averagePrice;

    @JsonProperty("exchange_timestamp")
    private String exchangeTimestamp;

    @JsonProperty("checksum")
    private String checksum;

    @JsonProperty("order_timestamp")
    private String orderTimestamp;
}