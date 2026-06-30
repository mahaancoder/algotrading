package com.satyam.trading2.datamodel;

import lombok.Data;

@Data
public class StockChange {

    private String symbol;
    private double change;

    public StockChange(String symbol, double change) {
        this.symbol = symbol;
        this.change = change;
    }
}
