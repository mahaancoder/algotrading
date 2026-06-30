package com.satyam.trading2.strategy;

import com.satyam.trading2.datamodel.DipState;
import com.satyam.trading2.datamodel.MarketContext;
import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.domain.service.PnLCalculator;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.scheduler.MarginFetchScheduler;
import com.satyam.trading2.service.DipAccumulatorService;
import org.springframework.stereotype.Component;

@Component
public class DipAccumulatorNoRegime implements TradingStrategy {

    private static final double TRANCHE = 50000;
    private static final double MAX_CAP = 200000;
    private static final double CAPITAL_PER_BUY = 25000; // Capital per buy signal

    private final DipAccumulatorService dipService;
    private final MarginFetchScheduler marginFetchScheduler;
    private final PositionManager positionManager;
    private final PnLCalculator pnlCalculator;

    public DipAccumulatorNoRegime(DipAccumulatorService dipService,
                                  MarginFetchScheduler marginFetchScheduler,
                                  PositionManager positionManager,
                                  PnLCalculator pnlCalculator) {
        this.dipService = dipService;
        this.marginFetchScheduler = marginFetchScheduler;
        this.positionManager = positionManager;
        this.pnlCalculator = pnlCalculator;
    }

    @Override
    public String getName() {
        return "Dip-Accumulator-NoRegime";
    }

    @Override
    public TradeSignal evaluate(MarketContext ctx) {
        if(ctx.getCandle() == null) return null;
        String symbol = ctx.getSymbol();

        // ===== EARLY EXIT: Check strategy-level capital limit BEFORE any evaluation work =====
        double strategyCapitalUsed = positionManager.getCapitalUsedByStrategy(getName());
        double strategyPnL = pnlCalculator.getStrategyPnL(getName());
        double maxCapital = marginFetchScheduler.getMaxCapitalPerStrategy(getName());

        // Check: Max capital reached for this strategy (including P&L)
        if (strategyCapitalUsed + CAPITAL_PER_BUY > (maxCapital + strategyPnL)) {
            return null;
        }

        double ltp = ctx.getCandle().getClose();
        double prevClose = ctx.getPreviousClose();
        double atr = ctx.getAtr();
        DipState state = dipService.get(symbol, getName());
        if (prevClose <= 0 ) {
            return null;
        }

        // ================= FIRST BUY =================
        if (state == null) {
            double level = Math.min(prevClose * 0.98, prevClose - (1.5 * atr));
            if (ltp <= level) {
                double baseTarget = ltp + (0.8 * atr);
                double target = Math.max(ltp * 1.01, baseTarget);
                return new TradeSignal(
                        TradeSignal.SignalType.BUY,
                        symbol,
                        ltp,
                        0,
                        target,
                        0,
                        "Dip L1",
                        getName()
                );
            }
            return null;
        }

        // ================= COOLDOWN =================
        if (System.currentTimeMillis() - state.getLastBuyTime() < 15000) {
            return null;
        }

        // ================= CAPITAL LIMIT =================
        if (state.getLevelFilled() >= 4) {
            return null;
        }

        double prevPrice = state.getLastObservedPrice();
        state.setLastObservedPrice(ltp);
        dipService.save(state);

        double lastEntry = state.getLastPrice();

        // ================= DUPLICATE GUARD =================
        if (Math.abs(ltp - lastEntry) / lastEntry < 0.002) {
            return null;
        }

        // ================= LEVEL CALCULATION =================
        double level = 0;
        double atrMultiplier = 1.2;

        if (state.getLevelFilled() == 1) {
            level = Math.min(lastEntry * 0.98, lastEntry - (1.2 * atr));
        }
        else if (state.getLevelFilled() == 2) {
            level = Math.min(lastEntry * 0.97, lastEntry - (1.5 * atr));
        }
        else if (state.getLevelFilled() == 3) {
            level = Math.min(lastEntry * 0.97, lastEntry - (2.0 * atr));
            atrMultiplier = 1.0; // tighter exit
        }

        // ================= CROSSING LOGIC =================
        boolean crossed = prevPrice > level && ltp <= level;
        if (crossed) {
            double avgCost = state.getAvgCost();

            // ================= TARGET =================
            double atrTarget = avgCost + (atrMultiplier * atr);
            double percentTarget = avgCost * 1.01;

            double finalTarget = Math.max(percentTarget, atrTarget);

            return new TradeSignal(
                    TradeSignal.SignalType.BUY,
                    symbol,
                    ltp,
                    0,
                    finalTarget,
                    0,
                    "Dip L" + (state.getLevelFilled() + 1),
                    getName()
            );
        }

        return null;
    }

    @Override
    public boolean supportsPositionalConversion() {
        return true;
    }
}