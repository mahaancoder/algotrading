package com.satyam.trading2.datamodel;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DipState {

    private String symbol;
    private String ownerStrategy;

    // 0 none bought yet
    // 1 first tranche filled
    // 2 second filled
    // 3 third filled
    // 4 fourth filled
    private int levelFilled;

    private double totalCapital;
    private int totalQty;
    private double lastPrice;
    private double avgCost;
    private long lastBuyTime;

    private double targetPrice;
    private double lastObservedPrice;
    private boolean convertedToHolding;

    public void addBuy(double capital, int qty){
        this.totalCapital += capital;
        this.totalQty += qty;
        if(totalQty>0){this.avgCost = this.totalCapital / this.totalQty;
        }
    }
}