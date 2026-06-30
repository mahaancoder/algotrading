package com.satyam.trading2.datamodel;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;

@Data
public class Instrument {
    @CsvBindByName(column = "tradingsymbol")
    private String tradingSymbol;
    @CsvBindByName(column = "instrument_token")
    private String instrumentToken;
    @CsvBindByName(column = "exchange")
    private String exchange;
    @CsvBindByName(column = "tick_size")
    private double tickSize;

    // Circuit limits and OHLC data (populated from /quote API)
    private Double upperCircuitLimit;
    private Double lowerCircuitLimit;
    private Double todayOpenPrice;

}
