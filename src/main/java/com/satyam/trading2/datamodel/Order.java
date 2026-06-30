package com.satyam.trading2.datamodel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a Kite order with all relevant details
 * Maps to Kite API /orders response structure
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Order {
    
    @JsonProperty("order_id")
    private String orderId;
    
    @JsonProperty("parent_order_id")
    private String parentOrderId;
    
    @JsonProperty("exchange_order_id")
    private String exchangeOrderId;
    
    @JsonProperty("tradingsymbol")
    private String tradingSymbol;
    
    @JsonProperty("exchange")
    private String exchange;
    
    @JsonProperty("instrument_token")
    private Long instrumentToken;
    
    @JsonProperty("order_type")
    private String orderType; // MARKET, LIMIT, SL, SL-M
    
    @JsonProperty("transaction_type")
    private String transactionType; // BUY, SELL
    
    @JsonProperty("validity")
    private String validity; // DAY, IOC
    
    @JsonProperty("product")
    private String product; // MIS, CNC, NRML
    
    @JsonProperty("quantity")
    private Integer quantity;
    
    @JsonProperty("disclosed_quantity")
    private Integer disclosedQuantity;
    
    @JsonProperty("price")
    private Double price;
    
    @JsonProperty("trigger_price")
    private Double triggerPrice;
    
    @JsonProperty("average_price")
    private Double averagePrice;
    
    @JsonProperty("filled_quantity")
    private Integer filledQuantity;
    
    @JsonProperty("pending_quantity")
    private Integer pendingQuantity;
    
    @JsonProperty("cancelled_quantity")
    private Integer cancelledQuantity;
    
    @JsonProperty("status")
    private String status; // OPEN, COMPLETE, CANCELLED, REJECTED, etc.
    
    @JsonProperty("status_message")
    private String statusMessage;
    
    @JsonProperty("status_message_raw")
    private String statusMessageRaw;
    
    @JsonProperty("order_timestamp")
    private String orderTimestamp;
    
    @JsonProperty("exchange_timestamp")
    private String exchangeTimestamp;
    
    @JsonProperty("exchange_update_timestamp")
    private String exchangeUpdateTimestamp;
    
    @JsonProperty("placed_by")
    private String placedBy;
    
    @JsonProperty("variety")
    private String variety; // regular, amo, co, iceberg
    
    @JsonProperty("tag")
    private String tag;
    
    @JsonProperty("guid")
    private String guid;
    
    /**
     * Check if order is open (pending execution)
     */
    public boolean isOpen() {
        return "OPEN".equalsIgnoreCase(status) || "TRIGGER PENDING".equalsIgnoreCase(status);
    }
    
    /**
     * Check if order is completed
     */
    public boolean isComplete() {
        return "COMPLETE".equalsIgnoreCase(status);
    }
    
    /**
     * Check if order is cancelled
     */
    public boolean isCancelled() {
        return "CANCELLED".equalsIgnoreCase(status);
    }
    
    /**
     * Check if order is rejected
     */
    public boolean isRejected() {
        return "REJECTED".equalsIgnoreCase(status);
    }
}

