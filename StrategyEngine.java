package com.satyam.trading2.strategy;


import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.datamodel.MarketContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class StrategyEngine {
    private final List<TradingStrategy> equityStrategies;

    public StrategyEngine(MomentumDipAccumulatorStrategy momentumDipAccumulatorStrategy) {
        this.equityStrategies = List.of(momentumDipAccumulatorStrategy);
    }

    /**
     * Evaluate all strategies in PARALLEL for maximum performance
     * Each strategy runs concurrently and checks its own limits independently
     * Returns ALL BUY signals from all strategies (multiple strategies can buy same symbol)
     *
     * IMPORTANT: With symbol+strategy level locking in ExecutionEngine, multiple strategies
     * can and should take positions in the same symbol simultaneously!
     *
     * THREAD-SAFE: Each strategy evaluates independently with no shared state during read
     */
    public List<TradeSignal> evaluate(MarketContext ctx) {
        // ─── RUN ALL STRATEGIES IN PARALLEL ───
        // parallelStream() automatically distributes strategies across available CPU cores
        // Collect ALL BUY signals (don't short-circuit!)
        List<TradeSignal> buySignals = equityStrategies.parallelStream()
            .map(strategy -> {
                try {
                    TradeSignal signal = strategy.evaluate(ctx);
                    if (signal == null) return null;
                    if (signal.getType() == TradeSignal.SignalType.BUY) {
                        return signal; // BUY signal found
                    } else if (signal.getType() == TradeSignal.SignalType.HOLD) {
                        System.out.println("⏸️  [" + strategy.getName() + "] HOLD " + ctx.getSymbol() + " - " + signal.getReason());
                    }
                } catch (Exception e) {
                    System.out.println("❌ [" + strategy.getName() + "] Exception for " + ctx.getSymbol() + ": " + e.getMessage());
                    e.printStackTrace();  // Print full stack trace to see exactly where it's failing
                }
                return null;
            })
            .filter(signal -> signal != null && signal.getType() == TradeSignal.SignalType.BUY)
            .collect(java.util.stream.Collectors.toList()); // Collect ALL BUY signals

        return buySignals;
    }

    /**
     * Get all registered strategy names
     */
    public List<String> getAllStrategyNames() {
        return equityStrategies.stream()
            .map(TradingStrategy::getName)
            .collect(java.util.stream.Collectors.toList());
    }
}
