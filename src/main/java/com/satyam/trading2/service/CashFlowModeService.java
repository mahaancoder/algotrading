package com.satyam.trading2.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds the runtime-selected {@link CashFlowMode}. Selectable from the dashboard.
 * Defaults to HIGH to preserve the pre-existing scheduler/margin behavior.
 */
@Slf4j
@Service
public class CashFlowModeService {

    private final AtomicReference<CashFlowMode> mode = new AtomicReference<>(CashFlowMode.HIGH);

    /** Current cash-flow mode. */
    public CashFlowMode getMode() {
        return mode.get();
    }

    /**
     * Set the cash-flow mode.
     *
     * @param newMode the mode to apply
     * @return the mode now in effect
     */
    public CashFlowMode setMode(CashFlowMode newMode) {
        CashFlowMode applied = newMode != null ? newMode : CashFlowMode.HIGH;
        mode.set(applied);
        log.info("💵 [CashFlowMode] set to {}", applied);
        System.out.println("💵 [CashFlowMode] set to " + applied);
        return applied;
    }

    /** True when the 3-lakh "return net" rule in getMargin() should be skipped. */
    public boolean isSkipNetThreshold() {
        return mode.get() == CashFlowMode.LOW;
    }
}
