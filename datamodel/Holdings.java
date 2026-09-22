package com.satyam.trading2.datamodel;

import lombok.Data;

@Data
public class Holdings  {
    public String symbol;
    public int qty;
    public double avgPrice;
    public double pnl;
    public String strategyName;
}
