package com.satyam.trading2.scheduler;

import com.satyam.trading2.service.OrderServiceV2;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class MarginFetchScheduler {

    private final OrderServiceV2 brokerService;
    private static Map<String, Integer> MAX_CAPITAL_PER_STRATEGY = new HashMap<>();


    @Scheduled(fixedDelay = 30000)
    public void setDailyBudgets() {
        try {
            Double availableFunds = brokerService.getMargin();

            if (availableFunds != null) {
                // Determine multiplier for Momentum strategy based on time
                LocalTime now = LocalTime.now();
                double momentumMultiplier;
                String timeSlot;

                if (now.isBefore(LocalTime.of(10, 0))) {
                    // 9:15 - 10:00 AM
                    momentumMultiplier = 1.0;
                    timeSlot = "9:15-10:00";
                }  else if (now.isBefore(LocalTime.of(10, 30))) {
                    // 10:00 - 10:30 AM (Peak)
                    momentumMultiplier = 0.8;
                    timeSlot = "10:00-10:30";
                } else if (now.isBefore(LocalTime.of(11, 0))) {
                    // 10:30 - 11:00 AM
                    momentumMultiplier = 0.6;
                    timeSlot = "10:30-11:00";
                } else if (now.isBefore(LocalTime.of(11, 15))) {
                    // 11:00 - 11:15 AM
                    momentumMultiplier = 0.4;
                    timeSlot = "11:00-11:15";
                } else if (now.isBefore(LocalTime.of(11, 30))) {
                    // 11:15 - 11:30 AM
                    momentumMultiplier = 0.2;
                    timeSlot = "11:15-11:30";
                } else {
                    // 11:30 AM and later
                    momentumMultiplier = 0.1;
                    timeSlot = "11:30+";
                }

                double momentumCapital = availableFunds * momentumMultiplier;

                // Set budgets with time-based Momentum allocation
                MAX_CAPITAL_PER_STRATEGY.put("Dip-Accumulator-Momentum", (int) momentumCapital);

                // Log every 5 minutes (every 10th call at 30s interval)
                if (System.currentTimeMillis() % 300000 < 30000) {
                    System.out.println("💰 [MarginFetchScheduler] Available Funds: ₹" + String.format("%.0f", availableFunds) +
                                     " | Time Slot: " + timeSlot +
                                     " | Multiplier: " + (momentumMultiplier * 100) + "%" +
                                     " | Momentum Budget: ₹" + String.format("%.0f", momentumCapital));
                }
//                MAX_CAPITAL_PER_STRATEGY.put("Dip-Accumulator-NoRegime", (int) (momentumCapital * 0.3));
//                MAX_CAPITAL_PER_STRATEGY.put("Dip-Accumulator-SmartBounce", (int) (momentumCapital * 0.3));
            }
        } catch (Exception e) {
            System.out.println("❌ Failed to set daily budgets: " + e);
        }
    }

    public double getMaxCapitalPerStrategy(String name) {
        Integer maxCapital = MAX_CAPITAL_PER_STRATEGY.get(name);
        if (maxCapital == null) {
            System.out.println("⚠️ [RiskManager] Max capital not initialized for " + name + ", using default 330000");
            return 100000; // 1L default
        }
        return maxCapital;
    }
}
