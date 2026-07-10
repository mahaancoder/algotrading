package com.satyam.trading2.risk;

import com.satyam.trading2.datamodel.DipState;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.scheduler.MarginFetchScheduler;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.satyam.trading2.risk.Exits.isExitedToday;
import static com.satyam.trading2.risk.Exits.markAsExitedToday;
import static org.apache.commons.collections.MapUtils.isNotEmpty;


@Service
@RequiredArgsConstructor
public class RiskManager {

    // Maximum capital allowed per symbol (across all strategies)
    // Set to 50K to allow 2 levels of dip accumulation (25K each)
    private static final double MAX_CAPITAL_PER_SYMBOL = 50000;

    private final MarginFetchScheduler marginFetchScheduler;
    private final PositionManager positionManager;
    private final Map<String, Double> reservedCapitalPerStrategy = new ConcurrentHashMap<>();
    private final Map<String, Double> reservedCapitalPerSymbol = new ConcurrentHashMap<>();

    // ===== PER-SYMBOL LOCKS: Prevent race conditions for concurrent orders on the same symbol =====
    private final Map<String, Object> symbolLocks = new ConcurrentHashMap<>();

    /**
     * Get or create a lock object for a specific symbol
     * This ensures that all risk checks and capital reservations for the same symbol are serialized
     */
    private Object getSymbolLock(String symbol) {
        return symbolLocks.computeIfAbsent(symbol, k -> new Object());
    }

    /**
     * Result class for risk check - contains both the decision and reason
     */
    @Getter
    @AllArgsConstructor
    public static class RiskCheckResult {
        private final boolean safe;
        private final String rejectionReason;

        public static RiskCheckResult safe() {
            return new RiskCheckResult(true, null);
        }

        public static RiskCheckResult rejected(String reason) {
            return new RiskCheckResult(false, reason);
        }
    }

    /**
     * ===== CRITICAL: Thread-safe risk check with symbol-level locking =====
     */
    public RiskCheckResult checkSignalSafety(String symbol, String strategy, double actualCapital, double entryPrice, DipState state, boolean isHolding) {

        // ===== SYMBOL-LEVEL LOCK: Serialize all checks for the same symbol =====
        synchronized (getSymbolLock(symbol)) {

            // ===== RISK CHECKS (using RiskManager) - PER STRATEGY =====
            if(state != null) {
                if ((state.getTotalCapital() + actualCapital) > 100000) {
                    return RiskCheckResult.rejected("Total capital exceeds 100K limit");
                }
                if (state.getLevelFilled() >= 4) {
                    return RiskCheckResult.rejected("Max accumulation levels reached (4)");
                }

                // ===== COOLDOWN CHECK (15 seconds between buys) =====
                if (System.currentTimeMillis() - state.getLastBuyTime() < 15000) {
                    return RiskCheckResult.rejected("Cooldown period - 15s between buys");
                }

                // ===== DUPLICATE GUARD (must be > 1% price difference from last entry) =====
                double lastEntry = state.getLastPrice();
                if (Math.abs(entryPrice - lastEntry) / lastEntry < 0.01) {
                    return RiskCheckResult.rejected("Price too close to last entry (< 1%)");
                }
            }

            // ===== SYMBOL CAPITAL LIMIT: Don't exceed MAX_CAPITAL_PER_SYMBOL per symbol =====
            Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);
            double totalCapitalInSymbol = 0.0;
            if(isNotEmpty(strategyPositions)) {
                totalCapitalInSymbol = strategyPositions.values().stream()
                        .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
                        .sum();
            }

            // Add reserved capital for this symbol to prevent race conditions
            double reservedForSymbol = reservedCapitalPerSymbol.getOrDefault(symbol, 0.0);
            double totalCommittedForSymbol = totalCapitalInSymbol + reservedForSymbol;

            if((totalCommittedForSymbol + actualCapital) > MAX_CAPITAL_PER_SYMBOL) {
                markAsExitedToday(symbol, strategy);
                return RiskCheckResult.rejected("Symbol capital limit - exceeds ₹50K per symbol");
            }

            // ===== EXIT TODAY CHECK: Don't re-enter if already exited today =====
            if (isExitedToday(symbol, strategy)) {
                return RiskCheckResult.rejected("Symbol+Strategy exited today - no re-entry");
            }

            // ===== STRATEGY CAPITAL LIMIT: Don't exceed strategy budget =====
            double strategyCapitalUsed = positionManager.getIntradayCapitalUsedByStrategy(strategy);
            double reservedCapital = reservedCapitalPerStrategy.getOrDefault(strategy, 0.0);
            double totalCommittedCapital = strategyCapitalUsed + reservedCapital;

            double maxCapital = marginFetchScheduler.getMaxCapitalPerStrategy(strategy);
            double fundsAvailableforTrading = maxCapital - totalCommittedCapital;

            if (actualCapital > fundsAvailableforTrading) {
                System.out.println("🚫 [RiskManager] Strategy capital limit reached for " + strategy +
                                 " | Max: ₹" + String.format("%.0f", maxCapital) +
                                 " | Used: ₹" + String.format("%.0f", strategyCapitalUsed) +
                                 " | Reserved: ₹" + String.format("%.0f", reservedCapital) +
                                 " | Available: ₹" + String.format("%.0f", fundsAvailableforTrading) +
                                 " | Needed: ₹" + String.format("%.0f", actualCapital));
                return RiskCheckResult.rejected("Insufficient strategy capital available");
            }

            return RiskCheckResult.safe();
        } // End symbol lock
    }

    public synchronized boolean reserveCapital(String strategy, double capitalAmount) {
        double strategyCapitalUsed = positionManager.getIntradayCapitalUsedByStrategy(strategy);
        double reservedCapital = reservedCapitalPerStrategy.getOrDefault(strategy, 0.0);
        double totalCommittedCapital = strategyCapitalUsed + reservedCapital;

        double maxCapital = marginFetchScheduler.getMaxCapitalPerStrategy(strategy);
        double fundsAvailableforTrading = maxCapital - totalCommittedCapital;

        if (capitalAmount > fundsAvailableforTrading) {
            System.out.println("🚫 [RiskManager] Cannot reserve ₹" + String.format("%.0f", capitalAmount) +
                             " for " + strategy + " - would exceed budget" +
                             " | Max: ₹" + String.format("%.0f", maxCapital) +
                             " | Used: ₹" + String.format("%.0f", strategyCapitalUsed) +
                             " | Reserved: ₹" + String.format("%.0f", reservedCapital) +
                             " | Available: ₹" + String.format("%.0f", fundsAvailableforTrading));
            return false;
        }

        reservedCapitalPerStrategy.merge(strategy, capitalAmount, Double::sum);
        System.out.println("✅ [RiskManager] Reserved ₹" + String.format("%.0f", capitalAmount) +
                         " for " + strategy +
                         " | Total reserved now: ₹" + String.format("%.0f", reservedCapitalPerStrategy.get(strategy)) +
                         " | Remaining budget: ₹" + String.format("%.0f", fundsAvailableforTrading - capitalAmount));
        return true;
    }

    public synchronized void releaseCapital(String strategy, double capitalAmount) {
        double currentReserved = reservedCapitalPerStrategy.getOrDefault(strategy, 0.0);
        double newReserved = Math.max(0, currentReserved - capitalAmount);

        if (newReserved == 0) {
            reservedCapitalPerStrategy.remove(strategy);
        } else {
            reservedCapitalPerStrategy.put(strategy, newReserved);
        }
    }

    public synchronized void confirmCapitalUsage(String strategy, double capitalAmount) {
        releaseCapital(strategy, capitalAmount);
    }

    /**
     * ===== ATOMIC: Reserve capital for a specific symbol with symbol-level locking =====
     */
    public boolean reserveCapitalForSymbol(String symbol, double capitalAmount) {
        // ===== SYMBOL-LEVEL LOCK: Same lock used in checkSignalSafety =====
        synchronized (getSymbolLock(symbol)) {
            Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);
            double totalCapitalInSymbol = 0.0;
            if(isNotEmpty(strategyPositions)) {
                totalCapitalInSymbol = strategyPositions.values().stream()
                        .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
                        .sum();
            }

            double reservedForSymbol = reservedCapitalPerSymbol.getOrDefault(symbol, 0.0);
            double totalCommittedForSymbol = totalCapitalInSymbol + reservedForSymbol;

            if((totalCommittedForSymbol + capitalAmount) > MAX_CAPITAL_PER_SYMBOL) {
                return false;
            }

            reservedCapitalPerSymbol.merge(symbol, capitalAmount, Double::sum);

            // ===== DEBUG LOG: Track reservations to diagnose race conditions =====
            System.out.println("✅ [RiskManager] Reserved ₹" + String.format("%.0f", capitalAmount) +
                             " for " + symbol + " | Total reserved: ₹" +
                             String.format("%.0f", reservedCapitalPerSymbol.get(symbol)) +
                             " | Total committed (positions + reserved): ₹" +
                             String.format("%.0f", totalCommittedForSymbol + capitalAmount));

            return true;
        }
    }

    /**
     * ===== ATOMIC: Release reserved capital for a symbol with symbol-level locking =====
     */
    public void releaseCapitalForSymbol(String symbol, double capitalAmount) {
        // ===== SYMBOL-LEVEL LOCK: Same lock used in checkSignalSafety and reserveCapitalForSymbol =====
        synchronized (getSymbolLock(symbol)) {
            double currentReserved = reservedCapitalPerSymbol.getOrDefault(symbol, 0.0);
            double newReserved = Math.max(0, currentReserved - capitalAmount);

            if (newReserved == 0) {
                reservedCapitalPerSymbol.remove(symbol);
            } else {
                reservedCapitalPerSymbol.put(symbol, newReserved);
            }

            // ===== DEBUG LOG: Track releases to diagnose race conditions =====
            System.out.println("🔓 [RiskManager] Released ₹" + String.format("%.0f", capitalAmount) +
                             " for " + symbol + " | Remaining reserved: ₹" +
                             String.format("%.0f", newReserved));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Dashboard diagnostics helpers
    // ─────────────────────────────────────────────────────────────────────────────

    /** Total reserved capital across all strategies */
    public double getTotalReservedCapital() {
        return reservedCapitalPerStrategy.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    /** Total reserved capital across all symbols */
    public double getTotalReservedCapitalBySymbol() {
        return reservedCapitalPerSymbol.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    /** Snapshot reserved capital per strategy */
    public Map<String, Double> getReservedCapitalPerStrategySnapshot() {
        return new HashMap<>(reservedCapitalPerStrategy);
    }

    /**
     * Emergency: release ALL reserved capital.
     * Note: This does NOT cancel any broker-side orders. It only clears local reservations.
     */
    public synchronized void releaseAllReservedCapital() {
        reservedCapitalPerStrategy.clear();
        reservedCapitalPerSymbol.clear();
        System.out.println("🧹 [RiskManager] Released ALL reserved capital (strategy + symbol maps cleared)");
    }

}
