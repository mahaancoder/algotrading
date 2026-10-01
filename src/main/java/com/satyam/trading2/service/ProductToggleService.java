package com.satyam.trading2.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service to manage product type toggles for MIS and CNC buy signals
 * When MIS is OFF, all MIS (intraday) buy orders will be blocked
 * When CNC is OFF, all CNC (delivery/holding) buy orders will be blocked
 */
@Slf4j
@Service
public class ProductToggleService {

    private final AtomicBoolean misEnabled = new AtomicBoolean(true);   // MIS enabled by default
    private final AtomicBoolean cncEnabled = new AtomicBoolean(true);  // CNC enabled by default - only MIS buys allowed

    // MIS-only mode: when ON, every buy (even for an existing holding) is forced
    // to INTRADAY, and its corresponding sell is placed as MIS. The forced-MIS leg
    // is tracked under a distinct strategy key so it coexists with any holding.
    private final AtomicBoolean misOnlyMode = new AtomicBoolean(false); // OFF by default — nothing changes until flipped

    /**
     * Check if MIS (intraday) buy orders are enabled
     * @return true if MIS buy orders are allowed
     */
    public boolean isMisEnabled() {
        return misEnabled.get();
    }

    /**
     * Check if MIS-only mode is active (force every buy + its sell to INTRADAY)
     * @return true if MIS-only mode is ON
     */
    public boolean isMisOnlyMode() {
        return misOnlyMode.get();
    }

    /**
     * Toggle MIS-only mode
     * @return new state (true = ON, false = OFF)
     */
    public boolean toggleMisOnlyMode() {
        boolean newState = !misOnlyMode.get();
        misOnlyMode.set(newState);

        if (newState) {
            log.warn("🔁 MIS-ONLY mode ENABLED - all buys (even holdings) forced to INTRADAY");
            System.out.println("🔁 MIS-ONLY mode ENABLED - all buys (even holdings) forced to INTRADAY");
        } else {
            log.info("✅ MIS-ONLY mode DISABLED - buys use normal MIS/CNC logic");
            System.out.println("✅ MIS-ONLY mode DISABLED - buys use normal MIS/CNC logic");
        }

        return newState;
    }
    
    /**
     * Check if CNC (delivery/holding) buy orders are enabled
     * @return true if CNC buy orders are allowed
     */
    public boolean isCncEnabled() {
        return cncEnabled.get();
    }
    
    /**
     * Enable MIS buy orders
     */
    public void enableMis() {
        misEnabled.set(true);
        log.info("✅ MIS (Intraday) buy orders ENABLED");
        System.out.println("✅ MIS (Intraday) buy orders ENABLED");
    }
    
    /**
     * Disable MIS buy orders
     */
    public void disableMis() {
        misEnabled.set(false);
        log.warn("🛑 MIS (Intraday) buy orders DISABLED - No new intraday positions will be opened");
        System.out.println("🛑 MIS (Intraday) buy orders DISABLED - No new intraday positions will be opened");
    }
    
    /**
     * Enable CNC buy orders
     */
    public void enableCnc() {
        cncEnabled.set(true);
        log.info("✅ CNC (Delivery) buy orders ENABLED");
        System.out.println("✅ CNC (Delivery) buy orders ENABLED");
    }
    
    /**
     * Disable CNC buy orders
     */
    public void disableCnc() {
        cncEnabled.set(false);
        log.warn("🛑 CNC (Delivery) buy orders DISABLED - No new delivery positions will be opened");
        System.out.println("🛑 CNC (Delivery) buy orders DISABLED - No new delivery positions will be opened");
    }
    
    /**
     * Toggle MIS state
     * @return new state (true = enabled, false = disabled)
     */
    public boolean toggleMis() {
        boolean newState = !misEnabled.get();
        misEnabled.set(newState);
        
        if (newState) {
            log.info("✅ MIS (Intraday) buy orders ENABLED");
            System.out.println("✅ MIS (Intraday) buy orders ENABLED");
        } else {
            log.warn("🛑 MIS (Intraday) buy orders DISABLED - No new intraday positions will be opened");
            System.out.println("🛑 MIS (Intraday) buy orders DISABLED - No new intraday positions will be opened");
        }
        
        return newState;
    }
    
    /**
     * Toggle CNC state
     * @return new state (true = enabled, false = disabled)
     */
    public boolean toggleCnc() {
        boolean newState = !cncEnabled.get();
        cncEnabled.set(newState);
        
        if (newState) {
            log.info("✅ CNC (Delivery) buy orders ENABLED");
            System.out.println("✅ CNC (Delivery) buy orders ENABLED");
        } else {
            log.warn("🛑 CNC (Delivery) buy orders DISABLED - No new delivery positions will be opened");
            System.out.println("🛑 CNC (Delivery) buy orders DISABLED - No new delivery positions will be opened");
        }
        
        return newState;
    }
}

