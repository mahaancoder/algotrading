package com.satyam.trading2.order;

import com.satyam.trading2.broker.Broker;
import com.satyam.trading2.broker.BrokerRouter;
import com.satyam.trading2.broker.ExecutionRoute;
import com.satyam.trading2.broker.model.BrokerOrderResponse;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.broker.model.OrderRequest;
import com.satyam.trading2.broker.model.OrderType;
import com.satyam.trading2.broker.model.ProductType;
import com.satyam.trading2.broker.model.TransactionType;
import com.satyam.trading2.datamodel.Order;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.service.OrderServiceV2;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * BrokerOrderRouter — shared, broker-aware order placement/cancellation used by
 * the background schedulers (TrailingStop, ConvertOrSell, HoldingsTargetSet).
 *
 * The buy for a Shoonya/INDstocks position is routed to that broker, so the
 * corresponding target/exit sell must go to the SAME broker — not Kite.
 * OrderServiceV2 is Kite-only, so KITE positions keep using it (preserving its
 * update-target dedup logic); non-KITE positions route through BrokerRouter so
 * the sell lands at the owning broker AND its fill event matches the
 * PendingOrder keyed by (broker, brokerOrderId).
 *
 * This mirrors the private helpers in OrderCompletionProcessor so scheduler code
 * does not have to be triplicated.
 */
@Component
@RequiredArgsConstructor
public class BrokerOrderRouter {

    private final OrderServiceV2 orderServiceV2;
    private final BrokerRouter brokerRouter;
    private final PendingOrderRepository pendingOrderRepository;

    /** The ExecutionRoute that owns this position (defaults to KITE). */
    public ExecutionRoute routeOf(Position position) {
        return position.getExecutionRoute() != null ? position.getExecutionRoute() : ExecutionRoute.KITE;
    }

    /** Broker name string for this position (defaults to KITE). */
    public String brokerOf(Position position) {
        return position.getBroker() != null ? position.getBroker() : routeOf(position).name();
    }

    /** True if this position is owned by Kite (uses OrderServiceV2). */
    public boolean isKite(Position position) {
        return routeOf(position) == ExecutionRoute.KITE;
    }

    /** Cancel an order at the broker that owns the position. */
    public void cancelViaBroker(Position position, String orderId) {
        if (orderId == null) return;
        if (isKite(position)) {
            orderServiceV2.cancelOrder(orderId);
        } else {
            brokerRouter.getBrokerWithFallback(routeOf(position)).cancelOrder(orderId);
        }
    }

    /**
     * Current status of an order at the broker that owns the position. Works for
     * KITE too (KiteBroker.getOrderStatus reads from OrderServiceV2). Returns null
     * if the order can't be found or the lookup fails.
     */
    public BrokerOrderStatus getOrderStatus(Position position, String orderId) {
        if (orderId == null) return null;
        try {
            return brokerRouter.getBrokerWithFallback(routeOf(position)).getOrderStatus(orderId);
        } catch (Exception e) {
            System.err.println("⚠️ [BrokerOrderRouter] Failed to fetch status for order "
                    + orderId + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Snapshot-aware status lookup. For KITE positions the status is resolved from
     * a pre-fetched orders snapshot (one GET /orders shared across the whole cycle)
     * instead of re-fetching per order. Non-KITE positions (or a null snapshot)
     * fall back to the per-order broker lookup above.
     */
    public BrokerOrderStatus getOrderStatus(Position position, String orderId, List<Order> kiteOrdersSnapshot) {
        if (orderId == null) return null;
        if (!isKite(position) || kiteOrdersSnapshot == null) {
            return getOrderStatus(position, orderId);
        }
        Order match = kiteOrdersSnapshot.stream()
                .filter(o -> orderId.equals(o.getOrderId()))
                .findFirst()
                .orElse(null);
        if (match == null) return null; // not in order book → treat as gone
        return mapKiteStatus(match.getStatus(), match.getFilledQuantity(), match.getQuantity());
    }

    /** Mirrors KiteBroker.mapKiteStatus so snapshot lookups map identically. */
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

    /**
     * Place/replace a TARGET sell. KITE keeps updateTargetOrder (which cancels the
     * prior target and dedups holdings); non-KITE cancels the prior target then
     * places a fresh LIMIT SELL at the owning broker.
     * @return broker order id, or null on failure.
     */
    public String placeTargetSell(Position position, String symbol, double price, int qty,
                                  String oldTargetOrderId, boolean isHolding, String strategy) {
        if (position == null || qty <= 0 || symbol == null || strategy == null) {
            System.err.println("🛑 [BrokerOrderRouter] Refusing invalid target SELL request.");
            return null;
        }
        synchronized (SellLifecycleLockRegistry.lockFor(position)) {
            if (isKite(position)) {
                // OrderServiceV2 performs the Kite order-book scan, cancellation
                // confirmation and zero-fill check under the same position lock.
                return orderServiceV2.updateTargetOrder(symbol, price, qty, oldTargetOrderId, isHolding, strategy);
            }
            if (oldTargetOrderId != null && !cancelAndConfirmSell(position, oldTargetOrderId, 2500L)) {
                System.err.println("🛑 [BrokerOrderRouter] Refusing replacement SELL for " + symbol
                        + ": previous order " + oldTargetOrderId
                        + " is not confirmed terminal with zero fills.");
                return null;
            }
            return placeLimitSellViaBroker(position, symbol, price, qty, isHolding, strategy);
        }
    }

    /**
     * Cancel an existing SELL and confirm it is terminal with exactly zero fills.
     * A CANCELLED status by itself is insufficient because an order may have
     * partially filled before cancellation. Missing status/fill data fails closed.
     */
    public boolean cancelAndConfirmSell(Position position, String orderId, long timeoutMillis) {
        if (orderId == null) return true;
        if (position == null) return false;

        Order kiteOrder = isKite(position) ? findKiteOrder(orderId) : null;
        BrokerOrderStatus status = isKite(position)
                ? (kiteOrder == null ? null : mapKiteStatus(
                        kiteOrder.getStatus(), kiteOrder.getFilledQuantity(), kiteOrder.getQuantity()))
                : getOrderStatus(position, orderId);
        Integer filled = isKite(position)
                ? (kiteOrder == null ? null : kiteOrder.getFilledQuantity())
                : getFilledQuantity(position, orderId);

        if (status == BrokerOrderStatus.COMPLETE || (filled != null && filled > 0)) {
            return false;
        }
        if ((status == BrokerOrderStatus.CANCELLED || status == BrokerOrderStatus.REJECTED)
                && Integer.valueOf(0).equals(filled)) {
            return true;
        }

        cancelViaBroker(position, orderId);
        long deadline = System.currentTimeMillis() + Math.max(0L, timeoutMillis);
        do {
            try {
                Thread.sleep(250L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            if (isKite(position)) {
                kiteOrder = findKiteOrder(orderId);
                status = kiteOrder == null ? null : mapKiteStatus(
                        kiteOrder.getStatus(), kiteOrder.getFilledQuantity(), kiteOrder.getQuantity());
                filled = kiteOrder == null ? null : kiteOrder.getFilledQuantity();
            } else {
                status = getOrderStatus(position, orderId);
                filled = getFilledQuantity(position, orderId);
            }

            if (status == BrokerOrderStatus.COMPLETE || (filled != null && filled > 0)) {
                return false;
            }
            if ((status == BrokerOrderStatus.CANCELLED || status == BrokerOrderStatus.REJECTED)
                    && Integer.valueOf(0).equals(filled)) {
                return true;
            }
        } while (System.currentTimeMillis() < deadline);
        return false;
    }

    /** Fetch one Kite order-book snapshot and resolve the requested order from it. */
    private Order findKiteOrder(String orderId) {
        List<Order> snapshot = orderServiceV2.fetchOrders();
        if (snapshot == null) return null;
        return snapshot.stream()
                .filter(java.util.Objects::nonNull)
                .filter(o -> orderId.equals(o.getOrderId()))
                .findFirst()
                .orElse(null);
    }

    private Integer getFilledQuantity(Position position, String orderId) {
        try {
            return brokerRouter.getBrokerWithFallback(routeOf(position)).getOrderFilledQuantity(orderId);
        } catch (Exception e) {
            System.err.println("⚠️ [BrokerOrderRouter] Unable to verify fills for " + orderId
                    + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Place a plain LIMIT SELL (no cancel-and-replace). KITE uses OrderServiceV2;
     * non-KITE routes to the owning broker.
     * @return broker order id, or null on failure.
     */
    public String placeLimitSell(Position position, String symbol, double price, int qty,
                                 boolean isHolding, String strategy) {
        if (position == null || symbol == null || qty <= 0 || strategy == null) {
            System.err.println("🛑 [BrokerOrderRouter] Refusing invalid plain SELL request.");
            return null;
        }
        synchronized (SellLifecycleLockRegistry.lockFor(position)) {
            if (isKite(position)) {
                return orderServiceV2.placeOrder(symbol, "SELL", "LIMIT", qty, price, 0,
                        isHolding ? "CNC" : "MIS", strategy);
            }
            return placeLimitSellViaBroker(position, symbol, price, qty, isHolding, strategy);
        }
    }

    private String placeLimitSellViaBroker(Position position, String symbol, double price, int qty,
                                           boolean isHolding, String strategy) {
        ExecutionRoute route = routeOf(position);
        Broker broker = brokerRouter.getBrokerWithFallback(route);
        OrderRequest req = OrderRequest.builder()
                .symbol(symbol)
                .exchange("NSE")
                .transactionType(TransactionType.SELL)
                .orderType(OrderType.LIMIT)
                .quantity(qty)
                .price(price)
                .productType(isHolding ? ProductType.DELIVERY : ProductType.INTRADAY)
                .strategyId(strategy)
                .build();
        System.out.println("🎯 [BrokerOrderRouter] Placing " + (isHolding ? "DELIVERY" : "INTRADAY")
                + " SELL via " + route + " for " + symbol + " qty=" + qty + " @ ₹" + String.format("%.2f", price));
        BrokerOrderResponse resp = broker.placeOrder(req);
        if (resp != null && resp.isAccepted() && resp.getBrokerOrderId() != null) {
            System.out.println("🎯 [BrokerOrderRouter] " + route + " sell placed for " + symbol
                    + " @ ₹" + String.format("%.2f", price) + " (qty=" + qty + ", id=" + resp.getBrokerOrderId() + ")");
            return resp.getBrokerOrderId();
        }
        System.out.println("❌ [BrokerOrderRouter] " + route + " sell REJECTED for " + symbol
                + ": " + (resp != null ? resp.getRejectionReason() : "null response"));
        return null;
    }

    /**
     * Create a SELL PendingOrder tagged with the position's broker + route so the
     * fill event is matched by (broker, brokerOrderId). KITE positions record KITE.
     */
    public PendingOrder createSellPending(Position position, String orderId, String symbol,
                                          String strategy, int qty, double price, boolean isHolding) {
        ExecutionRoute route = routeOf(position);
        String broker = brokerOf(position);
        return pendingOrderRepository.create(orderId, symbol, TradeSide.SELL, strategy,
                qty, price, isHolding, broker, route, null);
    }
}
