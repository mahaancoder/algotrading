package com.satyam.trading2.scheduler;

import com.satyam.trading2.datamodel.Order;
import com.satyam.trading2.service.OrderServiceV2;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Scheduler that runs every 15 minutes to cancel stale BUY orders
 * Cancels BUY orders that have been pending for more than 10 minutes
 */
@Service
@RequiredArgsConstructor
public class StaleBuyOrderCancellationScheduler {

    private final OrderServiceV2 orderServiceV2;

    // Kite API timestamp format: "2024-01-15 09:30:45" or "2024-01-15T09:30:45"
    private static final DateTimeFormatter[] FORMATTERS = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    };

    /**
     * Runs every 15 minutes: 0, 15, 30, 45 minutes past each hour
     * On weekdays (MON-FRI)
     */
    @Scheduled(cron = "0 0/15 * * * MON-FRI")
    public void cancelStaleBuyOrders() {
        System.out.println("🔄 [StaleBuyOrder] Running stale BUY order cancellation check...");

        try {
            // Fetch all orders from broker
            List<Order> allOrders = orderServiceV2.fetchOrders();

            if (allOrders.isEmpty()) {
                System.out.println("⏭️ [StaleBuyOrder] No orders found - skipping");
                return;
            }

            // Filter for open BUY orders
            List<Order> openBuyOrders = allOrders.stream()
                    .filter(o -> "BUY".equalsIgnoreCase(o.getTransactionType()))
                    .filter(Order::isOpen) // OPEN or TRIGGER PENDING status
                    .collect(Collectors.toList());

            System.out.println("📊 [StaleBuyOrder] Found " + openBuyOrders.size() + " open BUY orders");

            if (openBuyOrders.isEmpty()) {
                System.out.println("⏭️ [StaleBuyOrder] No open BUY orders - skipping");
                return;
            }

            LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
            int cancelledCount = 0;
            int skippedCount = 0;
            int failedCount = 0;

            for (Order order : openBuyOrders) {
                try {
                    String timestampStr = order.getOrderTimestamp();
                    
                    if (timestampStr == null || timestampStr.isEmpty()) {
                        System.out.println("⚠️ [StaleBuyOrder] " + order.getTradingSymbol() + 
                                         " (orderId: " + order.getOrderId() + "): No timestamp, skipping");
                        skippedCount++;
                        continue;
                    }

                    // Parse timestamp
                    LocalDateTime orderTime = parseTimestamp(timestampStr);
                    if (orderTime == null) {
                        System.out.println("⚠️ [StaleBuyOrder] " + order.getTradingSymbol() + 
                                         " (orderId: " + order.getOrderId() + "): Failed to parse timestamp '" + 
                                         timestampStr + "', skipping");
                        skippedCount++;
                        continue;
                    }

                    // Calculate minutes since order was placed
                    long minutesSinceOrder = ChronoUnit.MINUTES.between(orderTime, now);

                    // Cancel if order is older than 10 minutes
                    if (minutesSinceOrder >= 10) {
                        orderServiceV2.cancelOrder(order.getOrderId());
                        cancelledCount++;
                        System.out.println("✅ [StaleBuyOrder] Cancelled " + order.getTradingSymbol() + 
                                         " @ ₹" + String.format("%.2f", order.getPrice()) +
                                         " (orderId: " + order.getOrderId() + 
                                         ", age: " + minutesSinceOrder + " min)");
                        
                        // Small delay to avoid rate limiting
                        Thread.sleep(100);
                    } else {
                        System.out.println("⏭️ [StaleBuyOrder] " + order.getTradingSymbol() + 
                                         " (orderId: " + order.getOrderId() + "): Only " + 
                                         minutesSinceOrder + " min old, keeping");
                        skippedCount++;
                    }

                } catch (Exception e) {
                    failedCount++;
                    System.err.println("❌ [StaleBuyOrder] Error processing order " + 
                                     order.getOrderId() + " for " + order.getTradingSymbol() + 
                                     ": " + e.getMessage());
                }
            }

            // Summary
            String summary = String.format(
                "[StaleBuyOrder] Complete - Cancelled: %d, Skipped: %d, Failed: %d",
                cancelledCount, skippedCount, failedCount
            );
            System.out.println("📊 " + summary);

        } catch (Exception e) {
            System.err.println("❌ [StaleBuyOrder] Error in cancelStaleBuyOrders: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Parse Kite API timestamp string to LocalDateTime
     * Supports multiple formats: "yyyy-MM-dd HH:mm:ss" and "yyyy-MM-dd'T'HH:mm:ss"
     *
     * @param timestampStr Timestamp string from Kite API
     * @return LocalDateTime or null if parsing fails
     */
    private LocalDateTime parseTimestamp(String timestampStr) {
        if (timestampStr == null || timestampStr.isEmpty()) {
            return null;
        }

        for (DateTimeFormatter formatter : FORMATTERS) {
            try {
                return LocalDateTime.parse(timestampStr, formatter);
            } catch (Exception ignored) {
                // Try next formatter
            }
        }

        // If all formatters fail, return null
        return null;
    }
}

