package com.satyam.trading2.broker;

import com.satyam.trading2.broker.model.BrokerFunds;
import com.satyam.trading2.broker.model.BrokerOrderResponse;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.broker.model.OrderRequest;
import com.satyam.trading2.order.SellOrderAuthorizationContext;
import com.satyam.trading2.broker.model.OrderType;
import com.satyam.trading2.broker.model.ProductType;
import com.satyam.trading2.broker.model.TransactionType;
import com.satyam.trading2.datamodel.Order;
import com.satyam.trading2.service.OrderServiceV2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Kite execution broker.
 *
 * WRAPPER STAGE (current): delegates broker/execution calls to the existing
 * OrderServiceV2, translating the new broker-neutral OrderRequest into the
 * old String-based placeOrder signature. This keeps the app fully functional
 * while introducing the Broker abstraction. Full absorption of OrderServiceV2
 * internals can happen in a later cleanup pass.
 *
 * IMPORTANT: Kite has a dual role — it is ALSO the only market-data source
 * (WebSocketService ticker, CandleAggregator). That market-data role is NOT
 * part of this class; it stays in the market-data layer. KiteBroker is
 * execution-only.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KiteBroker implements Broker {

    private final OrderServiceV2 orderServiceV2;

    @Override
    public String getBrokerName() {
        return "KITE";
    }

    @Override
    public ExecutionRoute getRoute() {
        return ExecutionRoute.KITE;
    }

    @Override
    public BrokerOrderResponse placeOrder(OrderRequest request) {
        if (request != null && request.getTransactionType() == TransactionType.SELL
                && !SellOrderAuthorizationContext.isAuthorized()) {
            log.error("[KiteBroker] Refusing direct SELL submission outside BrokerOrderRouter; "
                    + "all SELLs must pass broker inventory and reservation checks.");
            return BrokerOrderResponse.builder()
                    .brokerName(getBrokerName())
                    .accepted(false)
                    .rejectionReason("SELL requires BrokerOrderRouter inventory/reservation authorization")
                    .build();
        }
        // Translate broker-neutral enums to the Kite String format
        String side = request.getTransactionType() == TransactionType.BUY ? "BUY" : "SELL";
        String type = mapOrderType(request.getOrderType());
        String product = mapProductType(request.getProductType());
        double price = request.getPrice();
        double trigger = request.getTriggerPrice() != null ? request.getTriggerPrice() : 0;
        String strategy = request.getStrategyId() != null ? request.getStrategyId() : "Unknown";

        String brokerOrderId = orderServiceV2.placeOrder(
                request.getSymbol(), side, type, request.getQuantity(),
                price, trigger, product, strategy);

        boolean accepted = brokerOrderId != null;
        return BrokerOrderResponse.builder()
                .brokerOrderId(brokerOrderId)
                .internalOrderId(null) // set by caller (PendingOrderRepository) if needed
                .brokerName(getBrokerName())
                .accepted(accepted)
                .rejectionReason(accepted ? null : "Kite rejected order (see logs)")
                .rawResponse(brokerOrderId)
                .build();
    }

    @Override
    public void cancelOrder(String brokerOrderId) {
        orderServiceV2.cancelOrder(brokerOrderId);
    }

    @Override
    public void modifyOrder(String brokerOrderId, OrderRequest request) {
        // Kite has no direct modify in OrderServiceV2; cancel + re-place is the existing pattern
        // (updateTargetOrder does cancel+replace internally). For now, delegate to cancel.
        // Full modify support can be added when needed.
        log.warn("[KiteBroker] modifyOrder called for {} — falling back to cancel (no direct modify yet)", brokerOrderId);
        orderServiceV2.cancelOrder(brokerOrderId);
    }

    @Override
    public BrokerOrderStatus getOrderStatus(String brokerOrderId) {
        List<Order> orders = orderServiceV2.fetchOrders();
        Order match = orders.stream()
                .filter(o -> brokerOrderId.equals(o.getOrderId()))
                .findFirst()
                .orElse(null);
        if (match == null) {
            return null;
        }
        return mapKiteStatus(match.getStatus(), match.getFilledQuantity(), match.getQuantity());
    }

    @Override
    public Integer getOrderFilledQuantity(String brokerOrderId) {
        List<Order> orders = orderServiceV2.fetchOrders();
        if (orders == null) return null;
        Order match = orders.stream()
                .filter(java.util.Objects::nonNull)
                .filter(o -> brokerOrderId.equals(o.getOrderId()))
                .findFirst()
                .orElse(null);
        return match == null ? null : match.getFilledQuantity();
    }

    @Override
    public Double getOrderAveragePrice(String brokerOrderId) {
        List<Order> orders = orderServiceV2.fetchOrders();
        if (orders == null) return null;
        Order match = orders.stream()
                .filter(java.util.Objects::nonNull)
                .filter(o -> brokerOrderId.equals(o.getOrderId()))
                .findFirst()
                .orElse(null);
        return match == null || match.getAveragePrice() == null || match.getAveragePrice() <= 0
                ? null : match.getAveragePrice();
    }

    @Override
    public Integer getNetSellableQuantity(String symbol, boolean holding) {
        return orderServiceV2.getNetSellableQuantity(symbol, holding);
    }

    @Override
    public BrokerFunds getFunds() {
        Double margin = orderServiceV2.getMargin();
        if (margin == null) {
            return BrokerFunds.builder()
                    .brokerName(getBrokerName())
                    .availableMargin(0)
                    .usedMargin(0)
                    .freeCash(0)
                    .build();
        }
        return BrokerFunds.builder()
                .brokerName(getBrokerName())
                .availableMargin(margin)
                .usedMargin(0) // OrderServiceV2.getMargin returns net available; used not separated yet
                .freeCash(margin)
                .build();
    }

    @Override
    public void startOrderUpdates() {
        // Kite order updates arrive via HTTP webhook (KiteWebhookController).
        // Nothing to start here currently — webhook endpoint is always on.
        // When per-broker enable/disable is needed, flip a flag here.
        log.info("[KiteBroker] Order updates active (HTTP webhook mode)");
    }

    @Override
    public void stopOrderUpdates() {
        log.info("[KiteBroker] Order updates disabled (HTTP webhook mode) — webhook still receives but will be ignored");
    }

    @Override
    public void reconcileOrders() {
        // Reconciliation logic currently lives in ReconciliationService.
        // During the refactor, BrokerReconciliationService will call this per-broker.
        // For now this is a no-op; full reconciliation moves in Step 8.
        log.info("[KiteBroker] reconcileOrders — delegated to ReconciliationService (will move here in Step 8)");
    }

    // ─────────────────────────────────────────────────────────────────────
    // Mapping helpers: broker-neutral enums ↔ Kite String values
    // ─────────────────────────────────────────────────────────────────────

    private String mapOrderType(OrderType orderType) {
        if (orderType == null) return "LIMIT";
        switch (orderType) {
            case MARKET:           return "MARKET";
            case LIMIT:            return "LIMIT";
            case STOP_LOSS:        return "SL";
            case STOP_LOSS_MARKET: return "SL-M";
            default:               return "LIMIT";
        }
    }

    private String mapProductType(ProductType productType) {
        if (productType == null) return "MIS";
        switch (productType) {
            case INTRADAY: return "MIS";
            case DELIVERY: return "CNC";
            case MARGIN:   return "NRML";
            default:       return "MIS";
        }
    }

    private BrokerOrderStatus mapKiteStatus(String kiteStatus, Integer filledQty, Integer totalQty) {
        if (kiteStatus == null) return null;
        switch (kiteStatus) {
            case "COMPLETE":  return BrokerOrderStatus.COMPLETE;
            case "REJECTED":  return BrokerOrderStatus.REJECTED;
            case "CANCELLED": return BrokerOrderStatus.CANCELLED;
            case "OPEN":
            case "UPDATE":
                int filled = filledQty != null ? filledQty : 0;
                int total = totalQty != null ? totalQty : 0;
                if (filled > 0 && filled < total) {
                    return BrokerOrderStatus.PARTIALLY_FILLED;
                }
                return BrokerOrderStatus.OPEN;
            default:
                return BrokerOrderStatus.OPEN;
        }
    }
}
