package com.satyam.trading2.broker;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.order.PendingOrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Read-only, broker-specific inventory audit.
 *
 * The audit never places or modifies orders. It compares the local quantity used
 * for SELL authorization with each broker's live sellable-quantity response.
 * Symbols with an unresolved SELL are skipped because the broker may already
 * reflect fills that local position accounting has intentionally not applied yet.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BrokerInventoryAuditService {

    private final BrokerRouter brokerRouter;
    private final PositionManager positionManager;
    private final PendingOrderRepository pendingOrderRepository;

    @Scheduled(fixedDelay = 300000L, initialDelay = 60000L)
    public void auditDuringMarketHours() {
        LocalTime now = LocalTime.now();
        if (now.isBefore(LocalTime.of(9, 20)) || now.isAfter(LocalTime.of(15, 30))) {
            return;
        }

        List<Position> openPositions = positionManager.getAllOpenPositions();
        List<PendingOrder> unresolvedOrders = pendingOrderRepository.findOrdersRequiringReconciliation();
        int checked = 0;
        int mismatched = 0;
        int unavailable = 0;
        int skipped = 0;

        for (Broker broker : brokerRouter.getAllBrokers()) {
            String brokerName = broker.getBrokerName();
            Map<String, InventoryKey> inventoryKeys = new LinkedHashMap<>();
            for (Position position : openPositions) {
                if (position == null || !position.isOpen() || position.isExitProcessed()) {
                    continue;
                }
                String positionBroker = position.getBroker() != null ? position.getBroker() : "KITE";
                if (!brokerName.equalsIgnoreCase(positionBroker) || position.getSymbol() == null
                        || position.getPositionType() == null) {
                    continue;
                }
                InventoryKey key = new InventoryKey(position.getSymbol(), position.getPositionType());
                inventoryKeys.putIfAbsent(key.key(), key);
            }

            for (InventoryKey key : inventoryKeys.values()) {
                if (hasUnresolvedSell(unresolvedOrders, brokerName, key)) {
                    skipped++;
                    continue;
                }

                long localQuantity = openPositions.stream()
                        .filter(Objects::nonNull)
                        .filter(Position::isOpen)
                        .filter(position -> !position.isExitProcessed())
                        .filter(position -> brokerName.equalsIgnoreCase(
                                position.getBroker() != null ? position.getBroker() : "KITE"))
                        .filter(position -> key.symbol().equalsIgnoreCase(position.getSymbol()))
                        .filter(position -> position.getPositionType() == key.positionType())
                        .mapToLong(position -> Math.max(0, position.getTotalQuantity()))
                        .sum();

                Integer brokerQuantity;
                try {
                    brokerQuantity = broker.getNetSellableQuantity(
                            key.symbol(), key.positionType() == Position.PositionType.HOLDING);
                } catch (RuntimeException e) {
                    log.warn("[BrokerInventoryAudit] {} {} {} query threw; inventory cannot be verified: {}",
                            brokerName, key.symbol(), key.positionType(), e.getMessage());
                    unavailable++;
                    continue;
                }

                if (brokerQuantity == null || brokerQuantity < 0) {
                    log.warn("[BrokerInventoryAudit] {} {} {} returned no authoritative sellable quantity; "
                                    + "SELL authorization will fail closed",
                            brokerName, key.symbol(), key.positionType());
                    unavailable++;
                    continue;
                }

                checked++;
                if (localQuantity != brokerQuantity.longValue()) {
                    mismatched++;
                    log.error("[BrokerInventoryAudit] INVENTORY MISMATCH broker={} symbol={} product={} "
                                    + "localSellable={} brokerSellable={}; SELLs are blocked by the router "
                                    + "until quantities reconcile",
                            brokerName, key.symbol(), key.positionType(), localQuantity, brokerQuantity);
                } else {
                    log.info("[BrokerInventoryAudit] MATCH broker={} symbol={} product={} sellableQty={}",
                            brokerName, key.symbol(), key.positionType(), brokerQuantity);
                }
            }
        }

        if (checked > 0 || mismatched > 0 || unavailable > 0 || skipped > 0) {
            log.info("[BrokerInventoryAudit] Summary checked={} mismatched={} unavailable={} "
                            + "skippedUnresolvedSell={}",
                    checked, mismatched, unavailable, skipped);
        }
    }

    private boolean hasUnresolvedSell(List<PendingOrder> orders, String brokerName, InventoryKey key) {
        for (PendingOrder order : orders) {
            if (order == null || order.getSide() != TradeSide.SELL || order.isCompletionProcessed()) {
                continue;
            }
            String orderBroker = order.getBroker() != null ? order.getBroker() : "KITE";
            if (!brokerName.equalsIgnoreCase(orderBroker)
                    || order.getSymbol() == null
                    || !key.symbol().equalsIgnoreCase(order.getSymbol())
                    || order.getPositionType() != key.positionType()) {
                continue;
            }
            // A pending order whose lifecycle is not complete can make the broker's
            // inventory differ from the not-yet-updated local position.
            return true;
        }
        return false;
    }

    private record InventoryKey(String symbol, Position.PositionType positionType) {
        private String key() {
            return symbol.toUpperCase(java.util.Locale.ROOT) + "::" + positionType.name();
        }
    }
}
