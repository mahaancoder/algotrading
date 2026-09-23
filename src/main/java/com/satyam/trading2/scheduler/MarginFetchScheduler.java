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
                }
            }

            // Fetch funds from all other registered brokers (INDstocks, Shoonya)
            for (Broker broker : brokerRouter.getAllBrokers()) {
                String name = broker.getBrokerName();
                if ("KITE".equals(name)) continue;
                try {
                    BrokerFunds funds = broker.getFunds();
                    if (funds != null && funds.getAvailableMargin() > 0) {
                        brokerAvailableFunds.put(name, funds.getAvailableMargin());
                        // Per-broker budget uses the same time-based multiplier
                        applyTimeBasedBudget(name, funds.getAvailableMargin());
                    }
                } catch (Exception e) {
                    log.warn("[MarginFetchScheduler] Failed to fetch funds for {}: {}", name, e.getMessage());
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
        LocalTime now = LocalTime.now();
        double multiplier;
        String timeSlot;

        if (now.isBefore(LocalTime.of(10, 0))) {
            multiplier = 4.0; timeSlot = "9:15-10:00";
        } else if (now.isBefore(LocalTime.of(10, 30))) {
            multiplier = 2.0; timeSlot = "10:00-10:30";
        } else if (now.isBefore(LocalTime.of(11, 0))) {
            multiplier = 1.0; timeSlot = "10:30-11:00";
        } else if (now.isBefore(LocalTime.of(11, 15))) {
            multiplier = 0.6; timeSlot = "11:00-11:15";
        } else if (now.isBefore(LocalTime.of(11, 30))) {
            multiplier = 0.4; timeSlot = "11:15-11:30";
        } else {
            multiplier = 0.2; timeSlot = "11:30+";
        }

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
}
