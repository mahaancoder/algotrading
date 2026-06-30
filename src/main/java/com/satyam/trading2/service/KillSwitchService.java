package com.satyam.trading2.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service to manage emergency kill switch for all trading operations
 * When activated, all BUY and SELL operations will be blocked
 */
@Slf4j
@Service
public class KillSwitchService {
    
    private final AtomicBoolean killSwitchActive = new AtomicBoolean(false);
    
    /**
     * Check if kill switch is currently active
     * @return true if trading should be stopped
     */
    public boolean isActive() {
        return killSwitchActive.get();
    }
    
    /**
     * Activate the kill switch - stops all trading
     */
    public void activate() {
        killSwitchActive.set(true);
        log.warn("🛑 KILL SWITCH ACTIVATED - All trading operations stopped");
        System.out.println("🛑🛑🛑 KILL SWITCH ACTIVATED - All trading operations stopped 🛑🛑🛑");
    }
    
    /**
     * Deactivate the kill switch - resumes trading
     */
    public void deactivate() {
        killSwitchActive.set(false);
        log.info("✅ KILL SWITCH DEACTIVATED - Trading operations resumed");
        System.out.println("✅ KILL SWITCH DEACTIVATED - Trading operations resumed");
    }
    
    /**
     * Toggle kill switch state
     * @return new state (true = active, false = inactive)
     */
    public boolean toggle() {
        boolean newState = !killSwitchActive.get();
        killSwitchActive.set(newState);
        
        if (newState) {
            log.warn("🛑 KILL SWITCH ACTIVATED - All trading operations stopped");
            System.out.println("🛑🛑🛑 KILL SWITCH ACTIVATED - All trading operations stopped 🛑🛑🛑");
        } else {
            log.info("✅ KILL SWITCH DEACTIVATED - Trading operations resumed");
            System.out.println("✅ KILL SWITCH DEACTIVATED - Trading operations resumed");
        }
        
        return newState;
    }
}

