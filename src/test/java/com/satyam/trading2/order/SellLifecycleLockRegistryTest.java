package com.satyam.trading2.order;

import com.satyam.trading2.datamodel.Position;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class SellLifecycleLockRegistryTest {

    @Test
    void serializesDifferentStrategiesAgainstSameBrokerSymbol() {
        Object first = SellLifecycleLockRegistry.lockFor(
                "RELIANCE", "strategy-a", "KITE", Position.PositionType.INTRADAY);
        Object second = SellLifecycleLockRegistry.lockFor(
                "reliance", "strategy-b", "kite", Position.PositionType.HOLDING);

        assertSame(first, second,
                "Strategies and position types must share the physical broker-symbol lock");
    }

    @Test
    void keepsDifferentBrokersIndependent() {
        Object kite = SellLifecycleLockRegistry.lockFor(
                "RELIANCE", "strategy-a", "KITE", Position.PositionType.INTRADAY);
        Object shoonya = SellLifecycleLockRegistry.lockFor(
                "RELIANCE", "strategy-a", "SHOONYA", Position.PositionType.INTRADAY);

        assertNotSame(kite, shoonya,
                "The same symbol at different broker accounts is independent inventory");
    }

    @Test
    void keepsDifferentSymbolsIndependent() {
        Object reliance = SellLifecycleLockRegistry.lockFor(
                "RELIANCE", "strategy-a", "KITE", Position.PositionType.INTRADAY);
        Object tcs = SellLifecycleLockRegistry.lockFor(
                "TCS", "strategy-a", "KITE", Position.PositionType.INTRADAY);

        assertNotSame(reliance, tcs);
    }
}
