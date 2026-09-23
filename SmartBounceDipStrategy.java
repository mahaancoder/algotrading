package com.satyam.trading2.strategy;

import com.satyam.trading2.datamodel.DipState;
import com.satyam.trading2.datamodel.MarketContext;
import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.domain.service.PnLCalculator;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.scheduler.MarginFetchScheduler;
import com.satyam.trading2.service.DipAccumulatorService;
import com.satyam.trading2.service.TickWindow;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class SmartBounceDipStrategy implements TradingStrategy {

    private static final double FIRST_DIP = 0.02;
    private static final double CAPITAL_PER_BUY = 25000; // Capital per buy signal

    private final DipAccumulatorService dipService;
    private final PositionManager positionManager;
    private final PnLCalculator pnlCalculator;
    private final MarginFetchScheduler marginFetchScheduler;



    @Override
    public String getName() {
        return "Dip-Accumulator-SmartBounce";
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

        if (prevClose <= 0) return null;

        // ================= Smart Bounce Logic =================

        TickWindow w = ctx.getTickWindow();

        if (w == null || !w.isReady()) {
            return null;
        }

        List<Double> prices = w.getPrices();

        // ===== MOMENTUM SLOWDOWN =====
        double p0 = prices.get(prices.size() - 1);
        double p5 = prices.get(prices.size() - 5);
        double p10 = prices.get(prices.size() - 10);

        double momentumNow = (p0 - p5) / p5;
        double momentumPrev = (p5 - p10) / p10;

        boolean slowingDown = momentumNow > momentumPrev;

        // ===== VOLATILITY SHRINKING =====
        double recentHigh =
                prices.subList(prices.size() - 5, prices.size())
                        .stream().max(Double::compare).orElse(0.0);

        double recentLow =
                prices.subList(prices.size() - 5, prices.size())
                        .stream().min(Double::compare).orElse(0.0);

        double oldHigh =
                prices.subList(prices.size() - 10, prices.size() - 5)
                        .stream().max(Double::compare).orElse(0.0);

        double oldLow =
                prices.subList(prices.size() - 10, prices.size() - 5)
                        .stream().min(Double::compare).orElse(0.0);

        double recentRange = recentHigh - recentLow;
        double oldRange = oldHigh - oldLow;

        boolean volatilityShrinking = recentRange < oldRange;

        // ================= FIRST BUY =================
        if (state == null) {

            double dipPct = (prevClose - ltp) / prevClose;

            // ===== DIP CHECK =====
            if (dipPct < FIRST_DIP) {
                return null;
            }

            // ===== SMART ENTRY: Momentum slowing + Volatility shrinking =====
            // Buy when dip shows early signs of reversal (slowing down + calming down)

            // Debug logging for signals that pass dip check but fail smart conditions
            if (!slowingDown || !volatilityShrinking) {
//                System.out.println(String.format(
//                    "[SmartBounce-DEBUG] %s: Dip %.2f%% | MomentumNow=%.4f MomentumPrev=%.4f SlowingDown=%s | RecentRange=%.2f OldRange=%.2f Shrinking=%s",
//                    symbol, dipPct * 100, momentumNow, momentumPrev, slowingDown,
//                    recentRange, oldRange, volatilityShrinking
//                ));
            }

            if (slowingDown && volatilityShrinking) {
//                System.out.println(String.format(
//                    "✅ [SmartBounce] BUY signal for %s @ ₹%.2f | Dip: %.2f%% | MomentumSlowing: %s | VolatilityShrinking: %s",
//                    symbol, ltp, dipPct * 100, slowingDown, volatilityShrinking
//                ));
                return new TradeSignal(
                        TradeSignal.SignalType.BUY,
                        symbol,
                        ltp,
                        0,
                        ltp * 1.01,
                        0,
                        "Smart Dip Entry",
                        getName()
                );
            }

            return null;
        }

        // ================= AFTER FIRST BUY - Standard Dip Accumulation Logic =================

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

            // Standard dip accumulation - no smart bounce conditions required
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