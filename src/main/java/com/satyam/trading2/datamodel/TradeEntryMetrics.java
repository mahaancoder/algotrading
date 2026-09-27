package com.satyam.trading2.datamodel;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Captures comprehensive metrics at the time of trade entry (buy execution)
 * Used for post-trade analysis and filtering
 */
@Data
@Builder
public class TradeEntryMetrics {
    
    // ===== Basic Trade Info =====
    private String symbol;
    private String strategy;
    private double buyPrice;     // Buy price (renamed from entryPrice)
    private Double sellPrice;    // Sell price (null until sold)
    private int quantity;
    private String buyTime;      // Buy time in GMT+5:30 format
    private String sellTime;     // Sell time in GMT+5:30 format (null until sold)
    private String orderId;
    private String status;       // "open" or "closed"
    
    // ===== Gap Analysis =====
    private double previousClose;
    private double openPrice;
    private double gapPercent;  // (Open - PreviousClose) / PreviousClose * 100
    private double distanceFromOpenPrice;  // (BuyPrice - OpenPrice) / OpenPrice * 100

    // ===== EMA =====
    private double ema20;
    private double ema50;
    private boolean aboveEma20;
    private boolean aboveEma50;
    
    // ===== Volume =====
    private double currentVolume;
    private double averageVolume20;  // Average volume of last 20 one-minute candles
    private double volumeRatio;      // currentVolume / averageVolume20
    
    // ===== Relative Strength =====
    private double stock5DayReturn;
    private double nifty5DayReturn;
    private double relativeStrength;  // Stock Return - Nifty Return
    
    // ===== Recovery =====
    private boolean twoGreenCandles;
    private double dayLow;
    private double distanceFromDayLow;  // (BuyPrice - DayLow) / DayLow * 100 at buy time
    
    // ===== Volatility =====
    private double atr;
    private double atrPercent;  // ATR / CurrentPrice * 100
    
    // ===== Opening Range =====
    private double openingRangeHigh;  // High between 9:15-9:30
    private double openingRangeLow;   // Low between 9:15-9:30
    private boolean buyAboveOpeningRangeHigh;  // Is buy price > opening range high?
    
    // ===== Price Levels =====
    private double yesterdayHigh;
    private boolean buyAboveYesterdayHigh;  // Is buy price > yesterday's high?
    
    // ===== Position Tracking =====
    private EntryType entryType;
    private Boolean convertedToHolding;  // Set at end of day if converted from intraday to holding

    // ===== ATR Target Tracking =====
    private double atrTarget;            // Calculated as: (atr * 0.5) + (buyPrice * 1.001)
    private boolean atrTargetTouched;    // Set to true if price touches atrTarget during the trade

    // ===== Exit Tracking (filled later) =====
    private Integer daysToTarget;         // Set when position is sold
    private Double pnl;
    private String positionType; // Set when position is sold
    private String broker;      // Which broker executed the trade (KITE/INDSTOCKS/SHOONYA)
    
    /**
     * Convert to CSV line for persistence
     */
    public String toCsvLine() {
        return symbol + "," +
               strategy + "," +
               buyPrice + "," +
               nvl(sellPrice) + "," +
               quantity + "," +
               quote(buyTime) + "," +
               quote(sellTime) + "," +
               quote(orderId) + "," +
               quote(status) + "," +
               previousClose + "," +
               openPrice + "," +
               gapPercent + "," +
               distanceFromOpenPrice + "," +
               ema20 + "," +
               ema50 + "," +
               aboveEma20 + "," +
               aboveEma50 + "," +
               currentVolume + "," +
               averageVolume20 + "," +
               volumeRatio + "," +
               stock5DayReturn + "," +
               nifty5DayReturn + "," +
               relativeStrength + "," +
               twoGreenCandles + "," +
               dayLow + "," +
               distanceFromDayLow + "," +
               atr + "," +
               atrPercent + "," +
               openingRangeHigh + "," +
               openingRangeLow + "," +
               buyAboveOpeningRangeHigh + "," +
               yesterdayHigh + "," +
               buyAboveYesterdayHigh + "," +
               (entryType != null ? entryType : "UNKNOWN") + "," +
               nvl(convertedToHolding) + "," +
               atrTarget + "," +
               atrTargetTouched + "," +
               nvl(daysToTarget) + "," +
               nvl(pnl)+ "," +
                positionType + "," +
                nvl(broker)
                ;
    }
    
    /**
     * CSV header
     */
    public static String csvHeader() {
        return "symbol,strategy,buyPrice,sellPrice,quantity,buyTime,sellTime,orderId,status," +
               "previousClose,openPrice,gapPercent,distanceFromOpenPrice," +
               "ema20,ema50,aboveEma20,aboveEma50," +
               "currentVolume,averageVolume20,volumeRatio," +
               "stock5DayReturn,nifty5DayReturn,relativeStrength," +
               "twoGreenCandles,dayLow,distanceFromDayLow," +
               "atr,atrPercent," +
               "openingRangeHigh,openingRangeLow,buyAboveOpeningRangeHigh," +
               "yesterdayHigh,buyAboveYesterdayHigh," +
               "entryType,convertedToHolding,atrTarget,atrTargetTouched,daysToTarget,pnl,positionType,broker";
    }

    /**
     * Parse CSV line back into TradeEntryMetrics object
     */
    public static TradeEntryMetrics fromCsvLine(String csvLine) {
        if (csvLine == null || csvLine.trim().isEmpty()) {
            return null;
        }

        try {
            // Split by comma, handling quoted fields
            String[] parts = parseCsvLine(csvLine);

            if (parts.length < 39) {
                return null; // Invalid line
            }

            return TradeEntryMetrics.builder()
                .symbol(parts[0])
                .strategy(parts[1])
                .buyPrice(parseDouble(parts[2]))
                .sellPrice(parseDoubleOrNull(parts[3]))
                .quantity(parseInt(parts[4]))
                .buyTime(unquote(parts[5]))
                .sellTime(unquote(parts[6]))
                .orderId(unquote(parts[7]))
                .status(unquote(parts[8]))
                .previousClose(parseDouble(parts[9]))
                .openPrice(parseDouble(parts[10]))
                .gapPercent(parseDouble(parts[11]))
                .distanceFromOpenPrice(parseDouble(parts[12]))
                .ema20(parseDouble(parts[13]))
                .ema50(parseDouble(parts[14]))
                .aboveEma20(parseBoolean(parts[15]))
                .aboveEma50(parseBoolean(parts[16]))
                .currentVolume(parseDouble(parts[17]))
                .averageVolume20(parseDouble(parts[18]))
                .volumeRatio(parseDouble(parts[19]))
                .stock5DayReturn(parseDouble(parts[20]))
                .nifty5DayReturn(parseDouble(parts[21]))
                .relativeStrength(parseDouble(parts[22]))
                .twoGreenCandles(parseBoolean(parts[23]))
                .dayLow(parseDouble(parts[24]))
                .distanceFromDayLow(parseDouble(parts[25]))
                .atr(parseDouble(parts[26]))
                .atrPercent(parseDouble(parts[27]))
                .openingRangeHigh(parseDouble(parts[28]))
                .openingRangeLow(parseDouble(parts[29]))
                .buyAboveOpeningRangeHigh(parseBoolean(parts[30]))
                .yesterdayHigh(parseDouble(parts[31]))
                .buyAboveYesterdayHigh(parseBoolean(parts[32]))
                .entryType(parseEntryType(parts[33]))
                .convertedToHolding(parseBooleanOrNull(parts[34]))
                .atrTarget(parseDouble(parts[35]))
                .atrTargetTouched(parseBoolean(parts[36]))
                .daysToTarget(parseIntegerOrNull(parts[37]))
                .pnl(parseDoubleOrNull(parts[38]))
                .positionType(parts.length>39 ? parts[39]: null)
                .broker(parts.length>40 ? unquote(parts[40]) : null)
                .build();
        } catch (Exception e) {
            // Log error and return null for invalid lines
            return null;
        }
    }

    /**
     * Parse CSV line handling quoted fields properly
     */
    private static String[] parseCsvLine(String line) {
        java.util.List<String> result = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());

        return result.toArray(new String[0]);
    }

    private static String unquote(String s) {
        if (s == null || s.isEmpty()) return null;
        s = s.trim();
        if (s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        if (s.isEmpty()) return null;
        return s;
    }

    private static double parseDouble(String s) {
        if (s == null || s.trim().isEmpty()) return 0.0;
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static Double parseDoubleOrNull(String s) {
        if (s == null || s.trim().isEmpty()) return null;
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int parseInt(String s) {
        if (s == null || s.trim().isEmpty()) return 0;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static Integer parseIntegerOrNull(String s) {
        if (s == null || s.trim().isEmpty()) return null;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static boolean parseBoolean(String s) {
        if (s == null || s.trim().isEmpty()) return false;
        return Boolean.parseBoolean(s.trim());
    }

    private static Boolean parseBooleanOrNull(String s) {
        if (s == null || s.trim().isEmpty()) return null;
        return Boolean.parseBoolean(s.trim());
    }

    private static EntryType parseEntryType(String s) {
        if (s == null || s.trim().isEmpty() || "UNKNOWN".equals(s.trim())) return null;
        try {
            return EntryType.valueOf(s.trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Format current time to GMT+5:30 format
     */
    public static String formatCurrentTimeGMT530() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter);
    }
    
    // Helper methods
    private String quote(String s) {
        if (s == null) return "\"\"";
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }
    
    private String nvl(Object o) {
        return o == null ? "" : o.toString();
    }
    
    /**
     * Calculate percentage difference
     */
    public static double percentDiff(double current, double base) {
        if (base == 0) return 0;
        return ((current - base) / base) * 100.0;
    }
}

