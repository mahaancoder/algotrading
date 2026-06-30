package com.satyam.trading2.datamodel;

import com.satyam.trading2.service.TickWindow;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MarketContext {
    @Override
    public String toString() {
        return "MarketContext{" +
                "symbol='" + symbol + '\'' +
                ", atr=" + atr +
                ", candle=" + candle +
                ", currentTime=" + currentTime +
                '}';
    }

    private final String symbol;
    private final double atr;           // ATR (14-period) — volatility measure
    private Candle candle;
    private double previousClose;
    private double Vwap;
    private TickWindow tickWindow;

    // ---- Market Time ----
    private final java.time.LocalTime currentTime;

    public java.time.LocalTime getTime() { return currentTime; }

}
