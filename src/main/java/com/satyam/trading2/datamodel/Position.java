package com.satyam.trading2.datamodel;

import lombok.*;

import java.util.HashSet;
import java.util.Set;

@Data
public class Position {
    private String positionId;
    private String symbol;
    private String strategy;
    private int totalQuantity;
    private double averagePrice;
    private boolean targetPlaced;
    private String targetOrderId;
    private boolean exitProcessed;
    private Set<String> buyOrderIds;
    private double realizedPnl;

    private double target;
    private boolean open;
    private String entryOrderId;
    private String stopLossOrderId;


    private PositionType positionType;
    private long createdAt;
    private long updatedAt;

    // Analytics: Track how this position was entered (gainer/loser/accumulation)
    private EntryType entryType;

    // Analytics: Track if this position was converted from intraday to holding
    private boolean convertedToHolding;
    private Long conversionTime;  // Timestamp when converted to holding

    public Position(String symbol, int qty, double entry, double target, boolean b, String strategy, String entryOrderId, String stopLossOrderId, String targetOrderId, PositionType positionType) {
        this.symbol = symbol;
        this.totalQuantity = qty;
        this.averagePrice = entry;
        this.target = target;
        this.open = b;
        this.strategy = strategy;
        this.entryOrderId = entryOrderId;
        this.stopLossOrderId = stopLossOrderId;
        this.targetOrderId = targetOrderId;
        this.positionType = positionType;
        this.buyOrderIds = new HashSet<>();  // ✅ Initialize to prevent NPE
        this.entryType = EntryType.UNKNOWN;  // Default to unknown
        this.convertedToHolding = false;

    }
    public enum PositionType {
        INTRADAY,
        HOLDING
    }


}