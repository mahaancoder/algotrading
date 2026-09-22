package com.satyam.trading2.datamodel;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OrderRequest {
    private String symbol;
    private String exchange;
    private TradeSide side;
    private String orderType; // LIMIT, MARKET, SL, SL-M
    private int quantity;
    private double price;
    private Double triggerPrice; // nullable
    private String product; // MIS, CNC, NRML
    private String strategy;
}
