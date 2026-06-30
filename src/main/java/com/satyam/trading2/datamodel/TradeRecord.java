package com.satyam.trading2.datamodel;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.persistence.*;
@Data
@AllArgsConstructor
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

    // Constructor for legacy support (without analytics fields)
    public TradeRecord(String instrument, String strategyName, double entryPrice,
                      double exitPrice, int quantity, double pnl, long timestamp) {
        this(instrument, strategyName, entryPrice, exitPrice, quantity, pnl, timestamp,
             EntryType.UNKNOWN, false, null);
    }
}
