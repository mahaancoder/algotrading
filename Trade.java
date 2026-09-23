package com.satyam.trading2.datamodel;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Trade {

    private String symbol;
    private double entryPrice;
    private double exitPrice;
    private int quantity;

    @Override
    public String toString() {
        return "Trade{" +
                "symbol='" + symbol + '\'' +
                ", entryPrice=" + entryPrice +
                ", exitPrice=" + exitPrice +
                ", quantity=" + quantity +
                ", type='" + type + '\'' +
                ", open=" + open +
                ", strategy='" + strategy + '\'' +
                '}';
    }

    private String type; // BUY / SELL
    private boolean open;
    private String strategy;
    private double pnl;
}