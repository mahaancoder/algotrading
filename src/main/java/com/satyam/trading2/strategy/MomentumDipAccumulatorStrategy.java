package com.satyam.trading2.strategy;

import com.satyam.trading2.datamodel.DipState;
import com.satyam.trading2.datamodel.EntryType;
import com.satyam.trading2.datamodel.MarketContext;
import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.scheduler.MarginFetchScheduler;
import com.satyam.trading2.service.DipAccumulatorService;
import com.satyam.trading2.service.StockSelectionService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import static com.satyam.trading2.datamodel.TradeSignal.SignalType.BUY;

@Component
@RequiredArgsConstructor
public class MomentumDipAccumulatorStrategy implements TradingStrategy {

    private static final double CAPITAL_PER_BUY = 25000; // Capital per buy signal

    private final StockSelectionService selectionService;
    private final DipAccumulatorService dipService;
    private final MarginFetchScheduler marginFetchScheduler;
    private final PositionManager positionManager;

    @Override
    public String getName() {
        return "Dip-Accumulator-Momentum";
    }

    @Override
    public TradeSignal evaluate(MarketContext ctx) {

        String symbol = ctx.getSymbol();
        double strategyIntradayCapitalUsed = positionManager.getIntradayCapitalUsedByStrategy(getName());
        double maxIntradayCapital = marginFetchScheduler.getMaxCapitalPerStrategy(getName());
        double availableFundsToTrade = maxIntradayCapital - strategyIntradayCapitalUsed;
        if (CAPITAL_PER_BUY > availableFundsToTrade) {
//            System.out.println("🚫 [" + getName() + "] " + symbol + " - Insufficient capital: need ₹" + CAPITAL_PER_BUY + ", available ₹" + availableFundsToTrade);
            return null;
        }

        double ltp = ctx.getCandle().getClose();
        double prevClose = ctx.getPreviousClose();

//        if (prevClose <= 0) {
//            System.out.println("🚫 [" + getName() + "] " + symbol + " - No previous close data (prevClose=" + prevClose + ")");
//            return null;
//        }

        boolean isLoser = selectionService.isTopLoser(symbol);
        boolean isGainer = selectionService.isTopGainer(symbol);

        DipState state = dipService.get(symbol, getName());

        // ================= FIRST BUY =================
        if (state == null) {

            // ===== LOSER ENTRY =====
            if (isLoser) {
                    TradeSignal signal = new TradeSignal(
                            BUY,
                            symbol,
                            ltp,
                            0,
                            ltp * 1.01,
                            0,
                            "Top Loser Entry",
                            getName()
                    );
                    signal.setEntryType(EntryType.TOP_LOSER);
                    return signal;

            }

            // ===== GAINER ENTRY =====
            if (isGainer) {
                    TradeSignal signal = new TradeSignal(
                            BUY,
                            symbol,
                            ltp,
                            0,
                            ltp * 1.01,
                            0,
                            "Top Gainer Entry",
                            getName()
                    );
                    signal.setEntryType(EntryType.TOP_GAINER);
                    return signal;

            }
            return null;
        }

        // ================= AFTER FIRST BUY =================
        // reuse SAME Dip logic

        if (state.getLevelFilled() >= 4) return null;

        double lastEntry = state.getLastPrice();
        double prevPrice = state.getLastObservedPrice();
        state.setLastObservedPrice(ltp);
        dipService.save(state); // Persist updated lastObservedPrice for crossing logic

        double level;

        if (state.getLevelFilled() == 1) {
            level = lastEntry * 0.98;
        } else {
            level = lastEntry * 0.97;
        }

        boolean crossed = prevPrice > level && ltp <= level;

        if (crossed) {

            double avg = state.getAvgCost();

            TradeSignal signal = new TradeSignal(
                    BUY,
                    symbol,
                    ltp,
                    0,
                    avg * 1.01,
                    0,
                    "Averaging",
                    getName()
            );
            signal.setEntryType(EntryType.ACCUMULATION);
            return signal;
        }

        return null;
    }

    @Override
    public boolean supportsPositionalConversion() {
        return true;
    }


}