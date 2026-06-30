package com.satyam.trading2.datamodel;

import lombok.Data;

@Data
public class TradeEvent {
    private String symbol;
    private String strategy;
    private String type; // BUY / SELL / CONVERTED_TO_HOLDING
    private double price;
    private int qty;
    private double pnl;
    private long time;

    // Analytics: Track entry type for BUY events
    private EntryType entryType;
}