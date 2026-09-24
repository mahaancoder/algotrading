package com.satyam.trading2.order;

import com.satyam.trading2.broker.Broker;
import com.satyam.trading2.broker.BrokerRouter;
import com.satyam.trading2.broker.ExecutionRoute;
import com.satyam.trading2.broker.model.BrokerOrderResponse;
import com.satyam.trading2.broker.model.OrderRequest;
import com.satyam.trading2.broker.model.OrderType;
import com.satyam.trading2.broker.model.ProductType;
import com.satyam.trading2.broker.model.TransactionType;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.service.OrderServiceV2;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

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
     * Place/replace a TARGET sell. KITE keeps updateTargetOrder (which cancels the
     * prior target and dedups holdings); non-KITE cancels the prior target then
     * places a fresh LIMIT SELL at the owning broker.
     * @return broker order id, or null on failure.
     */
    public String placeTargetSell(Position position, String symbol, double price, int qty,
                                  String oldTargetOrderId, boolean isHolding, String strategy) {
        if (isKite(position)) {
            return orderServiceV2.updateTargetOrder(symbol, price, qty, oldTargetOrderId, isHolding, strategy);
        }
        cancelViaBroker(position, oldTargetOrderId);
        return placeLimitSellViaBroker(position, symbol, price, qty, isHolding, strategy);
    }

    /**
     * Place a plain LIMIT SELL (no cancel-and-replace). KITE uses OrderServiceV2;
     * non-KITE routes to the owning broker.
     * @return broker order id, or null on failure.
     */
    public String placeLimitSell(Position position, String symbol, double price, int qty,
                                 boolean isHolding, String strategy) {
        if (isKite(position)) {
            return orderServiceV2.placeOrder(symbol, "SELL", "LIMIT", qty, price, 0,
                    isHolding ? "CNC" : "MIS", strategy);
        }
        return placeLimitSellViaBroker(position, symbol, price, qty, isHolding, strategy);
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
