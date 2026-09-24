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

    // ===== PER-BROKER reserved capital (Decision #3: per-broker capital tracking) =====
    // Key: broker::strategy → reserved amount. Lets us enforce per-broker limits.
    private final Map<String, Double> reservedCapitalPerBrokerStrategy = new ConcurrentHashMap<>();

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
     * Legacy overload — defaults to KITE broker. New callers should use the
     * broker-aware overload below.
     */
    public RiskCheckResult checkSignalSafety(String symbol, String strategy, double actualCapital, double entryPrice, DipState state, boolean isHolding) {
        return checkSignalSafety(symbol, strategy, actualCapital, entryPrice, state, isHolding, "KITE");
    }

    /**
     * ===== Broker-aware risk check with symbol-level locking =====
     * Checks per-broker strategy capital limit in addition to the global one.
     */
    public RiskCheckResult checkSignalSafety(String symbol, String strategy, double actualCapital, double entryPrice, DipState state, boolean isHolding, String broker) {

        String b = broker != null ? broker : "KITE";

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

            // ===== PER-BROKER STRATEGY CAPITAL LIMIT =====
            double brokerStrategyCapitalUsed = positionManager.getIntradayCapitalUsedByStrategyAndBroker(strategy, b);
            String brokerStrategyKey = b + "::" + strategy;
            double brokerReserved = reservedCapitalPerBrokerStrategy.getOrDefault(brokerStrategyKey, 0.0);
            double brokerCommitted = brokerStrategyCapitalUsed + brokerReserved;

            double brokerMaxCapital = marginFetchScheduler.getMaxCapitalPerStrategyAndBroker(strategy, b);
            double brokerFundsAvailable = brokerMaxCapital - brokerCommitted;

            if (actualCapital > brokerFundsAvailable) {
                return RiskCheckResult.rejected("Insufficient " + b + " broker capital (need ₹"
                    + String.format("%.0f", actualCapital) + ", avail ₹" + String.format("%.0f", brokerFundsAvailable) + ")");
            }

            // ===== GLOBAL STRATEGY CAPITAL LIMIT (safety net across all brokers) =====
            double strategyCapitalUsed = positionManager.getIntradayCapitalUsedByStrategy(strategy);
            double reservedCapital = reservedCapitalPerStrategy.getOrDefault(strategy, 0.0);
            double totalCommittedCapital = strategyCapitalUsed + reservedCapital;

            double maxCapital = marginFetchScheduler.getMaxCapitalPerStrategy(strategy);
            double fundsAvailableforTrading = maxCapital - totalCommittedCapital;

            if (actualCapital > fundsAvailableforTrading) {
                return RiskCheckResult.rejected("Insufficient strategy capital available (global limit)");
            }

            return RiskCheckResult.safe();
        } // End symbol lock
    }

    public synchronized boolean reserveCapital(String strategy, double capitalAmount) {
        return reserveCapital(strategy, capitalAmount, "KITE");
    }

    /**
     * Per-broker capital reservation (Decision #3: per-broker capital tracking).
     * Tracks reserved capital per broker+strategy so each broker's limits are
     * enforced independently. Also updates the global strategy-level map for
     * backward-compat with existing dashboard/diagnostic code.
     */
    public synchronized boolean reserveCapital(String strategy, double capitalAmount, String broker) {
        String b = broker != null ? broker : "KITE";
        String brokerStrategyKey = b + "::" + strategy;

        // Per-broker check — use THIS broker's own budget (available funds × time
        // multiplier), not the global Kite budget. Sourcing the cap from the global
        // key let INDSTOCKS/SHOONYA orders reserve against Kite's funds and ignore
        // their own available margin, so orders kept flowing past the broker's limit.
        double brokerStrategyCapitalUsed = positionManager.getIntradayCapitalUsedByStrategyAndBroker(strategy, b);
        double brokerReserved = reservedCapitalPerBrokerStrategy.getOrDefault(brokerStrategyKey, 0.0);
        double brokerCommitted = brokerStrategyCapitalUsed + brokerReserved;

        double brokerMaxCapital = marginFetchScheduler.getMaxCapitalPerStrategyAndBroker(strategy, b);
        double brokerFundsAvailable = brokerMaxCapital - brokerCommitted;

        if (capitalAmount > brokerFundsAvailable) {
            return false;
        }

        // Update per-broker+strategy map
        reservedCapitalPerBrokerStrategy.merge(brokerStrategyKey, capitalAmount, Double::sum);

        // Global strategy-level safety net across all brokers (uses the global budget).
        double maxCapital = marginFetchScheduler.getMaxCapitalPerStrategy(strategy);
        double globalStrategyCapitalUsed = positionManager.getIntradayCapitalUsedByStrategy(strategy);
        double globalReserved = reservedCapitalPerStrategy.getOrDefault(strategy, 0.0);
        double globalCommitted = globalStrategyCapitalUsed + globalReserved;
        double globalFundsAvailable = maxCapital - globalCommitted;

        if (capitalAmount > globalFundsAvailable) {
            // Roll back the per-broker reservation
            double cur = reservedCapitalPerBrokerStrategy.getOrDefault(brokerStrategyKey, 0.0);
            reservedCapitalPerBrokerStrategy.put(brokerStrategyKey, Math.max(0, cur - capitalAmount));
            return false;
        }

        reservedCapitalPerStrategy.merge(strategy, capitalAmount, Double::sum);
        return true;
    }

    public synchronized void releaseCapital(String strategy, double capitalAmount) {
        releaseCapital(strategy, capitalAmount, "KITE");
    }

    /** Per-broker overload — releases from both the per-broker and global maps. */
    public synchronized void releaseCapital(String strategy, double capitalAmount, String broker) {
        String b = broker != null ? broker : "KITE";
        String brokerStrategyKey = b + "::" + strategy;

        // Release per-broker+strategy
        double brokerCurrent = reservedCapitalPerBrokerStrategy.getOrDefault(brokerStrategyKey, 0.0);
        double brokerNew = Math.max(0, brokerCurrent - capitalAmount);
        if (brokerNew == 0) {
            reservedCapitalPerBrokerStrategy.remove(brokerStrategyKey);
        } else {
            reservedCapitalPerBrokerStrategy.put(brokerStrategyKey, brokerNew);
        }

        // Release global strategy-level
        double currentReserved = reservedCapitalPerStrategy.getOrDefault(strategy, 0.0);
        double newReserved = Math.max(0, currentReserved - capitalAmount);
        if (newReserved == 0) {
            reservedCapitalPerStrategy.remove(strategy);
        } else {
            reservedCapitalPerStrategy.put(strategy, newReserved);
        }
    }

    public synchronized void confirmCapitalUsage(String strategy, double capitalAmount) {
        confirmCapitalUsage(strategy, capitalAmount, "KITE");
    }

    /** Per-broker overload — confirms usage (releases reservation) for a specific broker. */
    public synchronized void confirmCapitalUsage(String strategy, double capitalAmount, String broker) {
        releaseCapital(strategy, capitalAmount, broker);
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
           /* System.out.println("✅ [RiskManager] Reserved ₹" + String.format("%.0f", capitalAmount) +
                             " for " + symbol + " | Total reserved: ₹" +
                             String.format("%.0f", reservedCapitalPerSymbol.get(symbol)) +
                             " | Total committed (positions + reserved): ₹" +
                             String.format("%.0f", totalCommittedForSymbol + capitalAmount));*/

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
          /*  System.out.println("🔓 [RiskManager] Released ₹" + String.format("%.0f", capitalAmount) +
                             " for " + symbol + " | Remaining reserved: ₹" +
                             String.format("%.0f", newReserved));*/
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
