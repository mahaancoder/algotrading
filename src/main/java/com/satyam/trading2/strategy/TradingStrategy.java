package com.satyam.trading2.strategy;


import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.datamodel.MarketContext;

/**
 * All trading strategies implement this interface.
 * The StrategyEngine calls evaluate() on each, then picks the best signal.
 */
public interface TradingStrategy {

    /**
     * Name of this strategy (used in logs and dashboard).
     */
    String getName();

    /**
     * Evaluate current market context and return a signal.
     * @param ctx  Snapshot of all market data for this tick
     * @return     A TradeSignal (BUY / EXIT / HOLD / CONVERT_TO_POSITIONAL)
     */
    TradeSignal evaluate(MarketContext ctx);

    /**
     * Whether this strategy supports conversion to a positional/swing hold.
     * If true, the engine may hold the trade for days/weeks instead of cutting loss.
     */
    default boolean supportsPositionalConversion() {
        return false;
    }
}
