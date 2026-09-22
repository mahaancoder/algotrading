package com.satyam.trading2.datamodel;

import java.util.HashMap;
import java.util.Map;

public class Bse100Stocks {
    public static final Map<Long, String> Bse100tokenToSymbol = new HashMap<>();

    static {
        Bse100tokenToSymbol.put(128083204L, "RELIANCE");
        Bse100tokenToSymbol.put(128046084L, "HDFCBANK");
        Bse100tokenToSymbol.put(136308228L, "BHARTIARTL");
        Bse100tokenToSymbol.put(136236548L, "ICICIBANK");
        Bse100tokenToSymbol.put(128028676L, "SBIN");
        Bse100tokenToSymbol.put(136330244L, "TCS");



    }



    public static String getSymbolFromToken(Long token) {
        return Bse100tokenToSymbol.get(token);
    }
}
