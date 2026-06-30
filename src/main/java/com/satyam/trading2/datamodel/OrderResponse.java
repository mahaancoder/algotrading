package com.satyam.trading2.datamodel;

import lombok.Data;

@Data
public class OrderResponse {
    private String entryOrderId;
    private String stopLossOrderId;
    private String targetOrderId;
}