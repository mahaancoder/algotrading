package com.satyam.trading2.scheduler;

import com.satyam.trading2.broker.Broker;
import com.satyam.trading2.broker.BrokerRouter;
import com.satyam.trading2.broker.ExecutionRoute;
import com.satyam.trading2.broker.model.BrokerFunds;
import com.satyam.trading2.config.KiteConfig;
import com.satyam.trading2.service.OrderServiceV2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class MarginFetchScheduler {

    private final OrderServiceV2 brokerService;
    private final BrokerRouter brokerRouter;
    private final KiteConfig kiteConfig;

    // strategy → max capital (Kite funds × time multiplier) — legacy global map
    private static final Map<String, Double> MAX_CAPITAL_PER_STRATEGY = new ConcurrentHashMap<>();

    // broker → available funds (cached, refreshed by setDailyBudgets)
    private final Map<String, Double> brokerAvailableFunds = new ConcurrentHashMap<>();


    // Runs shortly after startup and then every 5 minutes, all day. This keeps
    // the dashboard's available-margin cache populated even after market hours
    // or after a restart, and retries until each broker is logged in.
    @Scheduled(initialDelay = 30_000, fixedDelay = 300_000)
    public void setDailyBudgets() {
        try {
            // Fetch Kite funds only when logged in (avoids noisy 401s off-hours)
            if (kiteConfig.isAuthenticated()) {
                Double availableFunds = brokerService.getMargin();
                if (availableFunds != null) {
                    brokerAvailableFunds.put("KITE", availableFunds);
                    applyTimeBasedBudget("KITE", availableFunds);
                } else {
                    // Live fetch failed this tick — still apply the current time-slot
                    // multiplier against the last known balance so the budget stays
                    // fresh across time slots instead of falling back to the default.
                    Double cached = brokerAvailableFunds.get("KITE");
                    if (cached != null && cached > 0) {
                        applyTimeBasedBudget("KITE", cached);
                    } else {
                        System.out.println("⚠️ [MarginFetchScheduler] KITE | getMargin() null and no cached funds — budget not set this tick");
                    }
                }
            } else {
                System.out.println("⚠️ [MarginFetchScheduler] KITE | not authenticated — skipping budget (will retry in 5m)");
            }

            // Fetch funds from all other registered brokers (INDstocks, Shoonya)
            for (Broker broker : brokerRouter.getAllBrokers()) {
                String name = broker.getBrokerName();
                if ("KITE".equals(name)) continue;
                try {
                    BrokerFunds funds = broker.getFunds();
                    if (funds == null) {
                        System.out.println("⚠️ [MarginFetchScheduler] " + name
                                + " | getFunds() returned null — skipping (likely not authenticated)");
                        continue;
                    }
                    // Diagnostics: show exactly what the broker reported so a blank
                    // dashboard can be traced to auth / API / field-mapping issues.
                    System.out.println("💰 [MarginFetchScheduler] " + name
                            + " | getFunds() → available=₹" + String.format("%.0f", funds.getAvailableMargin())
                            + " used=₹" + String.format("%.0f", funds.getUsedMargin())
                            + " freeCash=₹" + String.format("%.0f", funds.getFreeCash()));
                    if (funds.getAvailableMargin() > 0) {
                        brokerAvailableFunds.put(name, funds.getAvailableMargin());
                        // Per-broker budget uses the same time-based multiplier
                        applyTimeBasedBudget(name, funds.getAvailableMargin());
                    } else {
                        // Live fetch returned 0 this tick — keep the budget fresh
                        // against the last known balance if we have one.
                        Double cached = brokerAvailableFunds.get(name);
                        if (cached != null && cached > 0) {
                            applyTimeBasedBudget(name, cached);
                        } else {
                            System.out.println("⚠️ [MarginFetchScheduler] " + name
                                    + " | availableMargin is 0 — not caching (check auth / API status / funds field mapping)");
                        }
                    }
                } catch (Exception e) {
                    System.out.println("❌ [MarginFetchScheduler] Failed to fetch funds for "
                            + name + ": " + e.getMessage());
                }
            }

        } catch (Exception e) {
            log.error("❌ Failed to set daily budgets: {}", e.getMessage());
        }
    }

    /**
     * Apply the time-based momentum multiplier to a broker's available funds
     * and store the resulting budget keyed by broker::strategy.
     */
    private void applyTimeBasedBudget(String brokerName, double availableFunds) {
        SlotMultiplier slot = currentSlotMultiplier();
        double multiplier = slot.multiplier;
        String timeSlot = slot.timeSlot;

        double budget = availableFunds * multiplier;

        // Per-broker+strategy budget: "KITE::Dip-Accumulator-Momentum" → budget
        MAX_CAPITAL_PER_STRATEGY.put(brokerName + "::Dip-Accumulator-Momentum", budget);

        // Also update the legacy global key (Kite only) for backward compat
        if ("KITE".equals(brokerName)) {
            MAX_CAPITAL_PER_STRATEGY.put("Dip-Accumulator-Momentum", budget);
        }

        System.out.println("💰 [MarginFetchScheduler] " + brokerName +
                         " | Funds: ₹" + String.format("%.0f", availableFunds) +
                         " | Slot: " + timeSlot +
                         " | Mult: " + (multiplier * 100) + "%" +
                         " | Budget: ₹" + String.format("%.0f", budget));
    }

    /**
     * Resolve the current time-based momentum slot and multiplier.
     * Pinned to IST so the slot is correct regardless of the JVM's default
     * timezone (e.g. UTC on a cloud host would otherwise always land in the
     * earliest slot and apply the wrong multiplier).
     */
    private SlotMultiplier currentSlotMultiplier() {
        LocalTime now = LocalTime.now(java.time.ZoneId.of("Asia/Kolkata"));
        if (now.isBefore(LocalTime.of(10, 0))) {
            return new SlotMultiplier(5.0, "9:15-10:00");
        } else if (now.isBefore(LocalTime.of(10, 30))) {
            return new SlotMultiplier(4.0, "10:00-10:30");
        } else if (now.isBefore(LocalTime.of(11, 0))) {
            return new SlotMultiplier(3.0, "10:30-11:00");
        } else if (now.isBefore(LocalTime.of(11, 15))) {
            return new SlotMultiplier(2.0, "11:00-11:15");
        } else if (now.isBefore(LocalTime.of(11, 30))) {
            return new SlotMultiplier(1.0, "11:15-11:30");
        } else {
            return new SlotMultiplier(0.2, "11:30+");
        }
    }

    /** Immutable holder for a time-slot's multiplier and its label. */
    private static final class SlotMultiplier {
        final double multiplier;
        final String timeSlot;
        SlotMultiplier(double multiplier, String timeSlot) {
            this.multiplier = multiplier;
            this.timeSlot = timeSlot;
        }
    }

    /** The momentum multiplier currently in effect for the IST time-of-day. */
    public double getCurrentMultiplier() {
        return currentSlotMultiplier().multiplier;
    }

    /** The label of the momentum time slot currently in effect (IST). */
    public String getCurrentTimeSlot() {
        return currentSlotMultiplier().timeSlot;
    }

    /**
     * Legacy: max capital for a strategy (global, Kite-based).
     * Kept for backward compat with callers that don't know the broker yet.
     */
    public double getMaxCapitalPerStrategy(String name) {
        Double maxCapital = MAX_CAPITAL_PER_STRATEGY.get(name);
        if (maxCapital == null) {
            return 100000; // 1L default
        }
        return maxCapital;
    }

    /**
     * Per-broker max capital for a strategy.
     * Falls back to the broker's cached available funds (× time multiplier)
     * if the scheduled budget hasn't been set yet.
     */
    public double getMaxCapitalPerStrategyAndBroker(String strategy, String broker) {
        String b = broker != null ? broker : "KITE";
        String key = b + "::" + strategy;
        Double budget = MAX_CAPITAL_PER_STRATEGY.get(key);
        if (budget != null) {
            return budget;
        }
        // Fallback: use cached broker funds (if any) with a conservative 1× multiplier
        Double funds = brokerAvailableFunds.get(b);
        if (funds != null && funds > 0) {
            return funds;
        }
        // Final fallback: Kite's global budget (so Kite-only setups still work)
        Double kiteBudget = MAX_CAPITAL_PER_STRATEGY.get(strategy);
        return kiteBudget != null ? kiteBudget : 100000;
    }

    /** Get the cached available funds for a specific broker. */
    public double getAvailableFundsForBroker(String broker) {
        String b = broker != null ? broker : "KITE";
        return brokerAvailableFunds.getOrDefault(b, 0.0);
    }

    /**
     * Snapshot of the latest available funds for every known broker
     * (KITE / INDSTOCKS / SHOONYA). Brokers whose funds haven't been fetched
     * yet report 0.0. Used by the dashboard's "Broker Funds & Usage" section.
     */
    public Map<String, Double> getAllBrokerAvailableFunds() {
        Map<String, Double> out = new java.util.LinkedHashMap<>();
        for (String b : new String[]{"KITE", "INDSTOCKS", "SHOONYA"}) {
            out.put(b, brokerAvailableFunds.getOrDefault(b, 0.0));
        }
        return out;
    }

    /**
     * Snapshot of the time-multiplied budget (available funds × current
     * momentum multiplier) for every known broker. Used by the dashboard's
     * "Broker Funds & Usage" section to show deployable capital, which differs
     * from raw available margin whenever the multiplier isn't 1×.
     */
    public Map<String, Double> getAllBrokerBudgets() {
        double multiplier = getCurrentMultiplier();
        Map<String, Double> out = new java.util.LinkedHashMap<>();
        for (String b : new String[]{"KITE", "INDSTOCKS", "SHOONYA"}) {
            out.put(b, brokerAvailableFunds.getOrDefault(b, 0.0) * multiplier);
        }
        return out;
    }
}
