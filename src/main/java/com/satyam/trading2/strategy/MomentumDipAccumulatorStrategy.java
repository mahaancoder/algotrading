package com.satyam.trading2.strategy;

import com.satyam.trading2.datamodel.MarketContext;
import com.satyam.trading2.datamodel.TradeSignal;
import org.springframework.stereotype.Component;

/** Momentum BUYs are now selection-cycle driven; tick evaluation emits no BUYs. */
@Component
public class MomentumDipAccumulatorStrategy implements TradingStrategy {
    @Override public String getName(){return "Dip-Accumulator-Momentum";}
    @Override public TradeSignal evaluate(MarketContext ctx){return null;}
    @Override public boolean supportsPositionalConversion(){return true;}
}
