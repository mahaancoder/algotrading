package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.datamodel.Nifty500Stocks;
import com.satyam.trading2.order.PendingOrderRepository;
import org.springframework.core.env.Environment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/** Shared strategy guard for the momentum-only trailing exit policy. */
public final class MomentumTrailingPolicy {
    public static final String STRATEGY = "Dip-Accumulator-Momentum";

    private MomentumTrailingPolicy() {
    }

    /**
     * Only classify a live position as momentum-managed when every recorded BUY
     * order ID resolves to a BUY for the same broker, symbol and momentum strategy.
     * Terminal status and fill reconciliation are checked separately before any SELL.
     * Broker snapshots can use the momentum strategy as a legacy placeholder;
     * those unattributed/manual positions retain the legacy exit policy.
     */
    public static boolean isManagedPosition(Position position, PendingOrderRepository orders) {
        if (position == null || orders == null || !isManagedStrategy(position.getStrategy())
                || position.getBuyOrderIds() == null || position.getBuyOrderIds().isEmpty()) {
            return false;
        }
        String broker = position.getBroker() != null ? position.getBroker().trim().toUpperCase(Locale.ROOT)
                : position.getExecutionRoute() != null ? position.getExecutionRoute().name() : "KITE";
        boolean foundBuy = false;
        for (String orderId : position.getBuyOrderIds()) {
            if (orderId == null || orderId.trim().isEmpty()) return false;
            PendingOrder pending = orders.findByBrokerAndBrokerOrderId(broker, orderId).orElse(null);
            if (pending == null) {
                pending = orders.findAll().stream()
                        .filter(java.util.Objects::nonNull)
                        .filter(po -> orderId.equals(po.getBrokerOrderId() != null
                                ? po.getBrokerOrderId() : po.getOrderId()))
                        .filter(po -> broker.equals(po.getBroker() != null
                                ? po.getBroker().trim().toUpperCase(Locale.ROOT)
                                : po.getExecutionRoute() != null ? po.getExecutionRoute().name() : "KITE"))
                        .findFirst().orElse(null);
            }
            // Every recorded BUY must be attributable to this broker, symbol and
            // momentum strategy, and its cumulative fill must be fully reconciled.
            // One valid BUY is not enough if another recorded BUY ID is unknown.
            if (pending == null || pending.getSide() != TradeSide.BUY
                    || !isManagedStrategy(pending.getStrategy())
                    || position.getSymbol() == null
                    || !position.getSymbol().equalsIgnoreCase(pending.getSymbol())) {
                return false;
            }
            foundBuy = true;
        }
        return foundBuy;
    }

    /**
     * A live-policy switch is safe only when all prerequisites are present. If
     * configuration, instrument tick size, BUY attribution or broker SL-LIMIT
     * capability is missing, callers must retain the legacy fixed-target path.
     */
    public static boolean hasUnresolvedMomentumBuy(Position position, PendingOrderRepository orders) {
        if (position == null || orders == null || position.getSymbol() == null
                || !isManagedStrategy(position.getStrategy())) return false;
        String broker = position.getBroker() != null ? position.getBroker().trim().toUpperCase(Locale.ROOT)
                : position.getExecutionRoute() != null ? position.getExecutionRoute().name() : "KITE";
        for (PendingOrder buy : orders.findAll()) {
            if (buy == null || buy.getSide() != TradeSide.BUY
                    || !isManagedStrategy(buy.getStrategy())
                    || buy.getSymbol() == null || !position.getSymbol().equalsIgnoreCase(buy.getSymbol())
                    || (buy.getPositionType() != null && buy.getPositionType() != Position.PositionType.INTRADAY)) {
                continue;
            }
            String pendingBroker = buy.getBroker() != null
                    ? buy.getBroker().trim().toUpperCase(Locale.ROOT)
                    : buy.getExecutionRoute() != null ? buy.getExecutionRoute().name() : "KITE";
            if (!broker.equals(pendingBroker)) continue;
            boolean terminal = buy.getState() == com.satyam.trading2.datamodel.PendingOrderState.COMPLETE
                    || buy.getState() == com.satyam.trading2.datamodel.PendingOrderState.CANCELLED
                    || buy.getState() == com.satyam.trading2.datamodel.PendingOrderState.REJECTED;
            if (!terminal || (buy.getFilledQty() > 0 && !buy.isCompletionProcessed())) return true;
        }
        return false;
    }

    public static boolean isLivePolicyReady(Position position, PendingOrderRepository orders,
                                            Environment environment, boolean brokerSupportsStopLimit,
                                            MomentumTrailingStateRepository stateRepository) {
        boolean liveOptIn = environment != null
                && Boolean.parseBoolean(environment.getProperty("momentum.trailing.enabled", "false"))
                && Boolean.parseBoolean(environment.getProperty("momentum.trailing.live-enabled", "false"));
        if (liveOptIn && position != null && stateRepository != null
                && isManagedStrategy(position.getStrategy())) {
            try {
                synchronized (com.satyam.trading2.order.SellLifecycleLockRegistry.lockFor(position)) {
                    stateRepository.restoreBuyOrderIds(position,
                            java.time.LocalDate.now(java.time.ZoneId.of("Asia/Kolkata")).toString());
                }
            } catch (RuntimeException restoreFailure) {
                // A failed state restore must never let a legacy scheduler infer
                // that a momentum position is safe to target/convert.
                return false;
            }
        }
        return isLivePolicyReady(position, orders, environment, brokerSupportsStopLimit);
    }

    public static boolean isLivePolicyReady(Position position, PendingOrderRepository orders,
                                            Environment environment, boolean brokerSupportsStopLimit) {
        if (position == null || environment == null || !brokerSupportsStopLimit
                || !position.isOpen() || position.getAveragePrice() <= 0 || position.getTotalQuantity() <= 0
                || position.getPositionType() != Position.PositionType.INTRADAY
                || !Boolean.parseBoolean(environment.getProperty("momentum.trailing.enabled", "false"))
                || !Boolean.parseBoolean(environment.getProperty("momentum.trailing.live-enabled", "false"))
                || !isManagedPosition(position, orders)) {
            return false;
        }
        String broker = position.getBroker() != null ? position.getBroker().trim().toUpperCase(Locale.ROOT)
                : position.getExecutionRoute() != null ? position.getExecutionRoute().name() : "KITE";
        String configuredCost = environment.getProperty("momentum.trailing.round-trip-cost-percent." + broker);
        if (configuredCost == null || configuredCost.trim().isEmpty()) return false;
        try {
            double cost = Double.parseDouble(configuredCost.trim());
            if (!Double.isFinite(cost) || cost < 0 || cost > 5) return false;
        } catch (NumberFormatException e) {
            return false;
        }
        if (position.getSymbol() == null) return false;
        com.satyam.trading2.datamodel.Instrument instrument =
                Nifty500Stocks.Nifty500SymbolToInstrument.get(position.getSymbol().toUpperCase(Locale.ROOT));
        if (instrument == null || instrument.getTickSize() <= 0) return false;

        // The initial SL-LIMIT must itself be placeable at/above the configured
        // net break-even floor. Otherwise keep the legacy target path active.
        double average = position.getAveragePrice();
        double tick = instrument.getTickSize();
        double breakEven = average * (1.0 + Double.parseDouble(configuredCost.trim()) / 100.0);
        double trigger = ceilToTick(average * 1.005, tick);
        double limit = ceilToTick(trigger * (1.0 - 0.0005), tick);
        if (limit >= trigger) limit = trigger - tick;
        if (limit + 1.0e-9 < breakEven) return false;

        return isStateStoreWritable(environment);
    }

    public static boolean isStateStoreWritable(Environment environment) {
        if (environment == null) return false;
        String stateFile = environment.getProperty("momentum.trailing.state-file",
                "/home/ec2-user/momentum_trailing_state.json");
        try {
            Path file = Paths.get(stateFile).toAbsolutePath();
            if (Files.exists(file) && !Files.isWritable(file)) return false;
            Path directory = file.getParent();
            while (directory != null && !Files.exists(directory)) directory = directory.getParent();
            return directory != null && Files.isDirectory(directory) && Files.isWritable(directory);
        } catch (Exception e) {
            return false;
        }
    }

    private static double ceilToTick(double price, double tick) {
        return BigDecimal.valueOf(price)
                .divide(BigDecimal.valueOf(tick), 0, RoundingMode.CEILING)
                .multiply(BigDecimal.valueOf(tick))
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
    }

    public static boolean isManagedStrategy(String strategy) {
        if (strategy == null) return false;
        String normalized = strategy.trim();
        if (normalized.endsWith("#MIS")) {
            normalized = normalized.substring(0, normalized.length() - 4);
        }
        return STRATEGY.equalsIgnoreCase(normalized.toUpperCase(Locale.ROOT));
    }
}
