package com.satyam.trading2.datamodel;

import lombok.Data;

@Data
public class TradeSignal {

    public enum SignalType {
        BUY,                    // Open a long position (intraday MIS)
        SELL,                   // Open a short position
        EXIT_LONG,              // Close existing long position
        HOLD,                   // Do nothing this tick
        SQUAREOFF,              // Force exit all positions (end of day)
        CONVERT_TO_POSITIONAL,  // Don't exit — convert MIS to CNC and hold for days/weeks
        BUY_PUT_HEDGE,          // Buy a Put option to hedge existing long equity position
        SELL_CALL_COVERED,      // Sell a Call (covered call) on existing equity for premium income
        BUY_CALL_SPREAD,        // Buy Bull Call Spread (buy lower strike call, sell higher strike call)
        CLOSE_FNO_LEG           // Close an open F&O hedge/position
    }

    private final SignalType type;
    private String symbol;
    private final double entryPrice;
    private final double stopLoss;
    private final double target;
    private final int quantity;
    private final String reason;
    private final String strategyName;   // which strategy generated this
    private double score;

    // Analytics: How this signal was generated (for tracking performance)
    private EntryType entryType;

    public TradeSignal(SignalType type, String symbol, double entryPrice, double stopLoss,
                       double target, int quantity, String reason) {
        this(type, symbol, entryPrice, stopLoss, target, quantity, reason, "Unknown");
    }

    public TradeSignal(SignalType type, String symbol, double entryPrice, double stopLoss,
                       double target, int quantity, String reason, String strategyName) {
        this.type         = type;
        this.symbol       = symbol;
        this.entryPrice   = entryPrice;
        this.stopLoss     = stopLoss;
        this.target       = target;
        this.quantity     = quantity;
        this.reason       = reason;
        this.strategyName = strategyName;
    }

    // Static factory for simple signals that don't need price levels
    public static TradeSignal hold(String symbol, String reason) {
        return new TradeSignal(SignalType.HOLD, symbol, 0, 0, 0, 0, reason);
    }

    public static TradeSignal squareOff(String symbol) {
        return new TradeSignal(SignalType.SQUAREOFF, symbol, 0, 0, 0, 0, "EOD SquareOff");
    }

    public static TradeSignal exitLong(String symbol, double price, String reason) {
        return new TradeSignal(
                SignalType.EXIT_LONG,
                symbol,
                price,
                0,
                0,
                0,
                reason
        );
    }

    public static TradeSignal convertToPositional(String reason, String strategyName, String symbol) {
        return new TradeSignal(SignalType.CONVERT_TO_POSITIONAL, symbol, 0, 0, 0, 0, reason, strategyName);
    }

    @Override
    public String toString() {
        return String.format("TradeSignal[%s | Entry=%.2f | SL=%.2f | Target=%.2f | Qty=%d | %s] | Strategy: %s | Symbol: %s  | Score=%.2f ",
                type, entryPrice, stopLoss, target, quantity, reason, strategyName, symbol,score);
    }
}
