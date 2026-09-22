package com.satyam.trading2.datamodel;

import com.satyam.trading2.broker.ExecutionRoute;
import lombok.*;

import java.util.HashSet;
import java.util.Set;

@Data
public class Position {
    private String positionId;
    private String symbol;
    private String strategy;
    private int totalQuantity;
    private double averagePrice;
    private boolean targetPlaced;
    private String targetOrderId;

    // ===== Dual sell-order support for CNC positions =====
    // When a new CNC buy completes, two sell orders are placed:
    //   1. targetOrderId       — "holding target": qty = total holdings, price = 0.03% above blended avg
    //   2. secondaryTargetOrderId — "quick exit": qty = new buy qty only, price = 1% above new buy avg
    // When either fills, the other must be cancelled.
    private String secondaryTargetOrderId;
    private int secondaryTargetQty;       // qty of the quick-exit order (new buy qty only)
    private boolean secondaryTargetPlaced;

    private boolean exitProcessed;
    private Set<String> buyOrderIds;
    private double realizedPnl;

    private double target;
    private boolean open;
    private String entryOrderId;
    private String stopLossOrderId;


    private PositionType positionType;
    private long createdAt;
    private long updatedAt;

    // ===== Broker dimension (added in multi-broker refactor) =====
    // A position is uniquely identified by broker + strategy + symbol.
    // RELIANCE at KITE and RELIANCE at INDSTOCKS are independent positions.
    private String broker;               // "KITE", "INDSTOCKS", "SHOONYA"
    private ExecutionRoute executionRoute;
    private String accountId;            // broker-specific account id (if multiple accounts per broker)

    // Analytics: Track how this position was entered (gainer/loser/accumulation)
    private EntryType entryType;

    // Analytics: Track if this position was converted from intraday to holding
    private boolean convertedToHolding;
    private Long conversionTime;  // Timestamp when converted to holding

    public Position(String symbol, int qty, double entry, double target, boolean b, String strategy, String entryOrderId, String stopLossOrderId, String targetOrderId, PositionType positionType) {
        this.symbol = symbol;
        this.totalQuantity = qty;
        this.averagePrice = entry;
        this.target = target;
        this.open = b;
        this.strategy = strategy;
        this.entryOrderId = entryOrderId;
        this.stopLossOrderId = stopLossOrderId;
        this.targetOrderId = targetOrderId;
        this.positionType = positionType;
        this.buyOrderIds = new HashSet<>();  // ✅ Initialize to prevent NPE
        this.entryType = EntryType.UNKNOWN;  // Default to unknown
        this.convertedToHolding = false;
        // Default broker to KITE for backward compat — existing callers don't set it yet.
        this.broker = "KITE";
        this.executionRoute = ExecutionRoute.KITE;

    }

    // ─────────────────────────────────────────────────────────────────────
    // Composite key helper: the effective identity of a position is
    // broker + strategy + symbol. Used by PositionManager and callers that
    // need to compare positions across brokers.
    // ─────────────────────────────────────────────────────────────────────
    public String compositeKey() {
        return (broker != null ? broker : "KITE") + "::" + strategy + "::" + symbol;
    }

    public enum PositionType {
        INTRADAY,
        HOLDING
    }


}