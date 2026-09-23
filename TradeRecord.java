package com.satyam.trading2.datamodel;

import lombok.Data;

import javax.persistence.*;
@Data
public class TradeRecord {
    private String  instrument;
    private String  strategyName;
    private double  entryPrice;
    private double  exitPrice;
    private int     quantity;
    private double  pnl;
    private long  timestamp; // When the trade was closed

    // Analytics fields
    private EntryType entryType;  // How the position was entered (gainer/loser/accumulation)
    private boolean wasConvertedToHolding;  // Was this converted from intraday to holding?
    private Long holdingDuration;  // How many days was it held (null for pure intraday)

    // ===== Broker dimension (added in multi-broker refactor) =====
    // Which broker executed this trade. Null for legacy trades (treated as KITE).
    private String broker;

    // Full constructor (with analytics + broker) — canonical form
    public TradeRecord(String instrument, String strategyName, double entryPrice,
                      double exitPrice, int quantity, double pnl, long timestamp,
                      EntryType entryType, boolean wasConvertedToHolding, Long holdingDuration,
                      String broker) {
        this.instrument = instrument;
        this.strategyName = strategyName;
        this.entryPrice = entryPrice;
        this.exitPrice = exitPrice;
        this.quantity = quantity;
        this.pnl = pnl;
        this.timestamp = timestamp;
        this.entryType = entryType;
        this.wasConvertedToHolding = wasConvertedToHolding;
        this.holdingDuration = holdingDuration;
        this.broker = broker;
    }

    // Constructor for backward compat (10-arg, without broker — defaults to KITE)
    public TradeRecord(String instrument, String strategyName, double entryPrice,
                      double exitPrice, int quantity, double pnl, long timestamp,
                      EntryType entryType, boolean wasConvertedToHolding, Long holdingDuration) {
        this(instrument, strategyName, entryPrice, exitPrice, quantity, pnl, timestamp,
             entryType, wasConvertedToHolding, holdingDuration, "KITE");
    }

    // Constructor for legacy support (without analytics fields or broker)
    public TradeRecord(String instrument, String strategyName, double entryPrice,
                      double exitPrice, int quantity, double pnl, long timestamp) {
        this(instrument, strategyName, entryPrice, exitPrice, quantity, pnl, timestamp,
             EntryType.UNKNOWN, false, null, "KITE");
    }
}
