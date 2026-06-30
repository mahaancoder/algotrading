package com.satyam.trading2.datamodel;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Candle {

    private double open;
    private double high;
    private double low;
    private double close;
    private LocalDateTime time;
    private double volume;

    public Candle(double open, double high, double low, double close, LocalDateTime time) {
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.time = time;
    }

}