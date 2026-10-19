package com.satyam.trading2.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.satyam.trading2.broker.model.BrokerFunds;
import com.satyam.trading2.broker.model.BrokerOrderResponse;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.broker.model.OrderRequest;
import com.satyam.trading2.broker.model.OrderType;
import com.satyam.trading2.broker.model.ProductType;
import com.satyam.trading2.broker.model.TransactionType;
import com.satyam.trading2.broker.ws.BrokerOrderEventListener;
import com.satyam.trading2.broker.ws.ShoonyaOrderWebSocketClient;
import com.satyam.trading2.config.ShoonyaConfig;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.order.PendingOrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Shoonya (Finvasia) execution broker — Group 2.
 *
 * REST format: POST application/x-www-form-urlencoded with
 *   jData=<JSON payload>&jKey=<accessToken>
 * Shoonya returns HTTP 200 for both accepted and rejected orders — always
 * check the "stat" field ("Ok" / "Not_Ok") before trusting "norenordno".
 *
 * Symbol mapping: our system uses Kite-style symbols (e.g. "RELIANCE").
 * Shoonya uses "<SYMBOL>-EQ" for NSE equity. toShoonyaSymbol() appends "-EQ".
 *
 * Product mapping: DELIVERY→"C" (CNC), INTRADAY→"I" (MIS), MARGIN→"M" (NRML).
 * Order type mapping: LIMIT→"LMT". Shoonya rejects MKT orders.
 *
 * Activated only when broker.shoonya.enabled=true.
 * Shoonya is EXECUTION-ONLY — market data always comes from Kite.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "broker.shoonya.enabled", havingValue = "true")
public class ShoonyaBroker extends AbstractRestBroker {

    private final ShoonyaConfig shoonyaConfig;
    private final BrokerOrderEventProcessor brokerOrderEventProcessor;
    private final PositionManager positionManager;

    // Field-injected (no cycle: PendingOrderRepository has no broker deps) so the
    // reconcile safety net can persist the recovered target SELL for fill matching.
    @Autowired
    private PendingOrderRepository pendingOrderRepository;

    // PositionBook carries no strategy, so a reconciled-in position uses this default.
    private static final String DEFAULT_STRATEGY = "Dip-Accumulator-Momentum";

    // WebSocket client for order updates — created on startOrderUpdates()
    private volatile ShoonyaOrderWebSocketClient wsClient;

    // ─────────────────────────────────────────────────────────────────────
    // Identity
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public String getBrokerName() { return "SHOONYA"; }

    @Override
    public ExecutionRoute getRoute() { return ExecutionRoute.SHOONYA; }

    // ─────────────────────────────────────────────────────────────────────
    // placeOrder — POST /PlaceOrder
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public BrokerOrderResponse placeOrder(OrderRequest request) {
        if (!shoonyaConfig.isAuthenticated()) {
            return BrokerOrderResponse.builder()
                    .brokerName(getBrokerName())
                    .accepted(false)
                    .rejectionReason("Shoonya not authenticated — no access token")
                    .build();
        }

        try {
            // Build the jData payload per Shoonya PlaceOrder spec
            Map<String, String> payload = new HashMap<>();
            payload.put("uid", shoonyaConfig.getUserId());
            payload.put("actid", shoonyaConfig.getActid());
            payload.put("exch", request.getExchange() != null ? request.getExchange() : "NSE");
            payload.put("tsym", toShoonyaSymbol(request.getSymbol()));
            payload.put("qty", String.valueOf(request.getQuantity()));
            payload.put("dscqty", "0");
            // Snap limit price to tick size (same as the Kite path) so Shoonya
            // doesn't reject with "price is not a multiple of tick size".
            boolean isBuy = request.getTransactionType() == TransactionType.BUY;
            String prc = (request.getOrderType() == OrderType.MARKET)
                    ? String.valueOf(request.getPrice())
                    : toTickPrice(request.getSymbol(), request.getPrice(), isBuy);
            payload.put("prc", prc);
            payload.put("prd", toShoonyaProduct(request.getProductType()));
            payload.put("prctyp", toShoonyaOrderType(request.getOrderType()));
            payload.put("trantype", toShoonyaTranType(request.getTransactionType()));
            payload.put("ret", "DAY");
            payload.put("ordersource", "API");
            // remarks = strategy/signalId for reconciliation
            String remarks = request.getStrategyId() != null ? request.getStrategyId() : "";
            if (request.getSignalId() != null) {
                remarks = remarks + "/" + request.getSignalId();
            }
            if (remarks.length() > 100) remarks = remarks.substring(0, 100);
            payload.put("remarks", remarks);

            // SL-LMT orders need a trigger price
            if (request.getOrderType() == OrderType.STOP_LOSS
                    || request.getOrderType() == OrderType.STOP_LOSS_MARKET) {
                if (request.getTriggerPrice() != null) {
                    payload.put("trgprc",
                            toTickPriceNearest(request.getSymbol(), request.getTriggerPrice()));
                }
            }

            String jData = objectMapper.writeValueAsString(payload);
            String body = norenBody(jData);

            JsonNode resp = postForm(shoonyaConfig.getBaseUrl() + "PlaceOrder", body);
            String stat = textOr(resp, "stat", "");
            String raw = resp.toString();

            if ("Ok".equals(stat)) {
                String norenordno = textOr(resp, "norenordno", null);
                log.info("[ShoonyaBroker] Order placed: {} {} {} qty={} → norenordno={}",
                        request.getTransactionType(), request.getSymbol(),
                        request.getPrice(), request.getQuantity(), norenordno);
                return BrokerOrderResponse.builder()
                        .brokerName(getBrokerName())
                        .brokerOrderId(norenordno)
                        .accepted(true)
                        .rawResponse(raw)
                        .build();
            } else {
                String emsg = textOr(resp, "emsg", "Unknown rejection");
                log.warn("[ShoonyaBroker] Order rejected: {} {} → {}", request.getSymbol(),
                        request.getTransactionType(), emsg);
                return BrokerOrderResponse.builder()
                        .brokerName(getBrokerName())
                        .accepted(false)
                        .rejectionReason(emsg)
                        .rawResponse(raw)
                        .build();
            }
        } catch (Exception e) {
            log.error("[ShoonyaBroker] placeOrder exception: {} {}", request.getSymbol(), e.getMessage(), e);
            return BrokerOrderResponse.builder()
                    .brokerName(getBrokerName())
                    .accepted(false)
                    .rejectionReason("Exception: " + e.getMessage())
                    .build();
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // cancelOrder — POST /CancelOrder
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public void cancelOrder(String brokerOrderId) {
        if (!shoonyaConfig.isAuthenticated()) {
            log.warn("[ShoonyaBroker] cancelOrder skipped — not authenticated. orderId={}", brokerOrderId);
            return;
        }
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("uid", shoonyaConfig.getUserId());
            payload.put("norenordno", brokerOrderId);

            String jData = objectMapper.writeValueAsString(payload);
            String body = norenBody(jData);

            JsonNode resp = postForm(shoonyaConfig.getBaseUrl() + "CancelOrder", body);
            String stat = textOr(resp, "stat", "");
            if ("Ok".equals(stat)) {
                log.info("[ShoonyaBroker] Order cancelled: {}", brokerOrderId);
            } else {
                log.warn("[ShoonyaBroker] Cancel failed: {} → {}", brokerOrderId, textOr(resp, "emsg", ""));
            }
        } catch (Exception e) {
            log.error("[ShoonyaBroker] cancelOrder exception: {} {}", brokerOrderId, e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // modifyOrder — POST /ModifyOrder
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public void modifyOrder(String brokerOrderId, OrderRequest request) {
        if (!shoonyaConfig.isAuthenticated()) {
            log.warn("[ShoonyaBroker] modifyOrder skipped — not authenticated. orderId={}", brokerOrderId);
            return;
        }
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("uid", shoonyaConfig.getUserId());
            payload.put("norenordno", brokerOrderId);
            payload.put("exch", request.getExchange() != null ? request.getExchange() : "NSE");
            payload.put("tsym", toShoonyaSymbol(request.getSymbol()));
            payload.put("qty", String.valueOf(request.getQuantity()));
            // Snap modified limit price to tick size (see placeOrder).
            String mprc = (request.getOrderType() == OrderType.MARKET)
                    ? String.valueOf(request.getPrice())
                    : toTickPrice(request.getSymbol(), request.getPrice(),
                            request.getTransactionType() == TransactionType.BUY);
            payload.put("prc", mprc);
            payload.put("prctyp", toShoonyaOrderType(request.getOrderType()));
            payload.put("ret", "DAY");

            String jData = objectMapper.writeValueAsString(payload);
            String body = norenBody(jData);

            JsonNode resp = postForm(shoonyaConfig.getBaseUrl() + "ModifyOrder", body);
            String stat = textOr(resp, "stat", "");
            if ("Ok".equals(stat)) {
                log.info("[ShoonyaBroker] Order modified: {}", brokerOrderId);
            } else {
                log.warn("[ShoonyaBroker] Modify failed: {} → {}", brokerOrderId, textOr(resp, "emsg", ""));
            }
        } catch (Exception e) {
            log.error("[ShoonyaBroker] modifyOrder exception: {} {}", brokerOrderId, e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // getOrderStatus — POST /SingleOrdStatus
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public BrokerOrderStatus getOrderStatus(String brokerOrderId) {
        if (!shoonyaConfig.isAuthenticated()) {
            log.warn("[ShoonyaBroker] getOrderStatus skipped — not authenticated. orderId={}", brokerOrderId);
            return null;
        }
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("uid", shoonyaConfig.getUserId());
            payload.put("norenordno", brokerOrderId);

            String jData = objectMapper.writeValueAsString(payload);
            String body = norenBody(jData);

            JsonNode resp = postForm(shoonyaConfig.getBaseUrl() + "SingleOrdStatus", body);
            String stat = textOr(resp, "stat", "");
            if ("Not_Ok".equals(stat)) {
                log.warn("[ShoonyaBroker] getOrderStatus failed: {} → {}", brokerOrderId, textOr(resp, "emsg", ""));
                return null;
            }
            // Shoonya order status field: "OPEN", "PENDING", "COMPLETE", "REJECTED", "CANCELLED"
            return mapShoonyaStatus(textOr(resp, "status", ""));
        } catch (Exception e) {
            log.error("[ShoonyaBroker] getOrderStatus exception: {} {}", brokerOrderId, e.getMessage(), e);
            return null;
        }
    }

    @Override
    public Integer getOrderFilledQuantity(String brokerOrderId) {
        if (!shoonyaConfig.isAuthenticated()) return null;
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("uid", shoonyaConfig.getUserId());
            payload.put("norenordno", brokerOrderId);
            JsonNode resp = postForm(shoonyaConfig.getBaseUrl() + "SingleOrdStatus",
                    norenBody(objectMapper.writeValueAsString(payload)));
            if ("Not_Ok".equals(textOr(resp, "stat", ""))) return null;
            for (String field : new String[]{"fillshares", "filled_quantity", "filled_qty", "flqty"}) {
                JsonNode value = resp.get(field);
                if (value != null && !value.isNull()) {
                    try {
                        return (int) Math.round(Double.parseDouble(value.asText().trim()));
                    } catch (NumberFormatException ignored) {
                        return null;
                    }
                }
            }
            return null;
        } catch (Exception e) {
            log.warn("[ShoonyaBroker] Unable to verify filled quantity for {}: {}",
                    brokerOrderId, e.getMessage());
            return null;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // getFunds — POST /Limits
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public BrokerFunds getFunds() {
        if (!shoonyaConfig.isAuthenticated()) {
            System.out.println("⚠️ [ShoonyaBroker] getFunds skipped — not authenticated (no jKey set)");
            return BrokerFunds.builder()
                    .brokerName(getBrokerName())
                    .availableMargin(0).usedMargin(0).freeCash(0)
                    .build();
        }
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("uid", shoonyaConfig.getUserId());
            payload.put("actid", shoonyaConfig.getActid());

            String jData = objectMapper.writeValueAsString(payload);
            String body = norenBody(jData);

            JsonNode resp = postForm(shoonyaConfig.getBaseUrl() + "Limits", body, SLOW_REQUEST_TIMEOUT_MS);
            // Diagnostic: print the raw Limits response so a ₹0 dashboard can be
            // traced to an API error vs. a field-name mismatch (does NOT include
            // the jKey — only the balance payload).
            System.out.println("🔍 [ShoonyaBroker] /Limits raw response → " + resp);
            String stat = textOr(resp, "stat", "");
            if ("Not_Ok".equals(stat)) {
                System.out.println("⚠️ [ShoonyaBroker] getFunds failed: " + textOr(resp, "emsg", ""));
                return BrokerFunds.builder()
                        .brokerName(getBrokerName())
                        .availableMargin(0).usedMargin(0).freeCash(0)
                        .build();
            }
            // Noren /Limits balance model: funds can arrive via several credit
            // fields (an account that deposited today shows the amount under
            // 'payin', not 'cash'), so summing only 'cash' wrongly reads ₹0.
            //   credit    = cash + payin + brkcollamt   (available capital in)
            //   debit     = marginused + payout          (consumed / withdrawn)
            //   available = credit - debit               (net deployable)
            double cash = doubleOr(resp, "cash", 0);
            double payin = doubleOr(resp, "payin", 0);
            double collateral = doubleOr(resp, "brkcollamt", 0);
            double payout = doubleOr(resp, "payout", 0);
            double marginUsed = doubleOr(resp, "marginused", 0);

            double credit = cash + payin + collateral;
            double available = credit - marginUsed + payout;
            if (available < 0) available = 0;

            if (credit == 0) {
                System.out.println("⚠️ [ShoonyaBroker] getFunds: no credit (cash/payin/brkcollamt all 0) — "
                        + "check field mapping against response above (stat=" + stat + ")");
            }

            return BrokerFunds.builder()
                    .brokerName(getBrokerName())
                    .availableMargin(available)
                    .usedMargin(marginUsed)
                    .freeCash(available)
                    .build();
        } catch (Exception e) {
            System.out.println("❌ [ShoonyaBroker] getFunds exception: " + e.getMessage());
            log.error("[ShoonyaBroker] getFunds exception: {}", e.getMessage(), e);
            return BrokerFunds.builder()
                    .brokerName(getBrokerName())
                    .availableMargin(0).usedMargin(0).freeCash(0)
                    .build();
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // startOrderUpdates / stopOrderUpdates — WebSocket (stub for now)
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public void startOrderUpdates() {
        if (!shoonyaConfig.isAuthenticated()) {
            log.warn("[ShoonyaBroker] startOrderUpdates skipped — not authenticated");
            return;
        }
        if (wsClient != null && wsClient.isRunning()) {
            log.info("[ShoonyaBroker] Order updates already running");
            return;
        }
        log.info("[ShoonyaBroker] startOrderUpdates — connecting Shoonya order WebSocket to {}",
                shoonyaConfig.getWsUrl());

        // Listener: route each BrokerOrderEvent through the broker-neutral processor
        BrokerOrderEventListener listener = event -> {
            try {
                brokerOrderEventProcessor.process(event);
            } catch (Exception e) {
                log.error("[ShoonyaBroker] Error processing order event: {}", e.getMessage(), e);
            }
        };

        wsClient = new ShoonyaOrderWebSocketClient(shoonyaConfig, listener);
        wsClient.start();
    }

    @Override
    public void stopOrderUpdates() {
        log.info("[ShoonyaBroker] stopOrderUpdates — disconnecting Shoonya order WebSocket");
        if (wsClient != null) {
            wsClient.stop();
            wsClient = null;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // reconcileOrders — POST /PositionBook + /OrderBook
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public void reconcileOrders() {
        if (!shoonyaConfig.isAuthenticated()) {
            log.warn("[ShoonyaBroker] reconcileOrders skipped — not authenticated");
            return;
        }
        log.info("[ShoonyaBroker] reconcileOrders — fetching positions + order book from Shoonya");
        try {
            // 1. Fetch positions (POST /PositionBook)
            Map<String, String> posPayload = new HashMap<>();
            posPayload.put("uid", shoonyaConfig.getUserId());
            posPayload.put("actid", shoonyaConfig.getActid());
            String posJData = objectMapper.writeValueAsString(posPayload);
            String posBody = norenBody(posJData);

            JsonNode posResp = postForm(shoonyaConfig.getBaseUrl() + "PositionBook", posBody);
            if (posResp.isArray()) {
                log.info("[ShoonyaBroker] Reconciliation: {} open positions from Shoonya", posResp.size());
                syncPositionsIntoManager(posResp);
            }

            // 2. Fetch order book (POST /OrderBook)
            Map<String, String> ordPayload = new HashMap<>();
            ordPayload.put("uid", shoonyaConfig.getUserId());
            String ordJData = objectMapper.writeValueAsString(ordPayload);
            String ordBody = norenBody(ordJData);

            JsonNode ordResp = postForm(shoonyaConfig.getBaseUrl() + "OrderBook", ordBody);
            if (ordResp.isArray()) {
                log.info("[ShoonyaBroker] Reconciliation: {} orders from Shoonya order book", ordResp.size());
                // TODO: match each order's norenordno against PendingOrderRepository
                // by (broker=SHOONYA, brokerOrderId) and update status if drifted.
            }

            // 3. Fetch demat holdings (POST /Holdings) — settled CNC stock that does
            //    NOT appear in the intraday PositionBook. Needed so the morning
            //    HoldingsTargetSetScheduler can place targets for overnight Shoonya
            //    holdings.
            Map<String, String> holdPayload = new HashMap<>();
            holdPayload.put("uid", shoonyaConfig.getUserId());
            holdPayload.put("actid", shoonyaConfig.getActid());
            holdPayload.put("prd", "C"); // CNC / delivery holdings
            String holdJData = objectMapper.writeValueAsString(holdPayload);
            String holdBody = norenBody(holdJData);

            JsonNode holdResp = postForm(shoonyaConfig.getBaseUrl() + "Holdings", holdBody, SLOW_REQUEST_TIMEOUT_MS);
            if (holdResp.isArray()) {
                log.info("[ShoonyaBroker] Reconciliation: {} demat holdings from Shoonya", holdResp.size());
                syncHoldingsIntoManager(holdResp);
            } else {
                // Non-array = error/empty payload (e.g. {"stat":"Not_Ok","emsg":...}).
                // Log it so a missing-holdings dashboard can be traced to an API error
                // rather than a silent skip (does NOT include the jKey).
                System.out.println("🔍 [ShoonyaBroker] /Holdings non-array response → " + holdResp);
            }

            // Safety net: re-arm any missing intraday target sells. A buy fill can
            // arrive on the order-update WS before its PendingOrder is saved and be
            // dropped as unmatched, so onBuyCompleted never runs and no target sell
            // is placed. Recover here off the broker order book (source of truth).
            ensureTargetSellsForOpenPositions(ordResp);
        } catch (Exception e) {
            log.error("[ShoonyaBroker] reconcileOrders exception: {}", e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Target-sell safety net
    //
    // For every open SHOONYA intraday position with no resting SELL in the broker
    // order book, place the standard MIS target (avg × time-window multiplier —
    // identical to OrderCompletionProcessor.onBuyCompleted). Any non-terminal SELL
    // for the symbol blocks a re-place so we never oversell.
    // ─────────────────────────────────────────────────────────────────────

    private void ensureTargetSellsForOpenPositions(JsonNode orderBook) {
        try {
            Set<String> symbolsWithOpenSell = new HashSet<>();
            if (orderBook != null && orderBook.isArray()) {
                for (JsonNode o : orderBook) {
                    if (!"S".equalsIgnoreCase(textOr(o, "trantype", ""))) continue;
                    String st = textOr(o, "status", "").toUpperCase();
                    boolean terminal = st.equals("COMPLETE") || st.equals("FILLED")
                            || st.equals("REJECTED") || st.equals("CANCELLED") || st.equals("CANCELED");
                    if (!terminal) {
                        String sym = fromShoonyaSymbol(textOr(o, "tsym", null));
                        if (sym != null) symbolsWithOpenSell.add(sym);
                    }
                }
            }

            for (Position pos : positionManager.getAllIntradayPositions()) {
                if (pos == null || !"SHOONYA".equals(pos.getBroker())) continue;
                if (pos.getTotalQuantity() <= 0) continue;
                if (symbolsWithOpenSell.contains(pos.getSymbol())) {
                    continue; // already has a resting target sell — nothing to recover
                }
                placeRecoveredTargetSell(pos);
            }
        } catch (Exception e) {
            log.warn("[ShoonyaBroker] ensureTargetSells failed: {}", e.getMessage());
        }
    }

    /** Place the MIS target SELL for a position that lost its target, then persist
     *  a broker-tagged SELL PendingOrder so the eventual fill is matched. */
    private void placeRecoveredTargetSell(Position pos) {
        String symbol = pos.getSymbol();
        int qty = pos.getTotalQuantity();
        double target = com.satyam.trading2.helpers.TargetMultiplierHelper
                .calculateTargetPrice(pos.getAveragePrice(), false);

        if (!com.satyam.trading2.helpers.CircuitLimitChecker
                .isSpecificTargetWithinCircuitLimits(symbol, target)) {
            log.warn("[ShoonyaBroker] Recover target SKIPPED for {} — target ₹{} hits upper circuit",
                    symbol, String.format("%.2f", target));
            return;
        }

        OrderRequest req = OrderRequest.builder()
                .symbol(symbol)
                .exchange("NSE")
                .transactionType(TransactionType.SELL)
                .orderType(OrderType.LIMIT)
                .quantity(qty)
                .price(target)
                .productType(ProductType.INTRADAY)
                .strategyId(pos.getStrategy())
                .build();

        log.info("[ShoonyaBroker] Recover target: placing SELL {} qty={} @ ₹{}",
                symbol, qty, String.format("%.2f", target));
        BrokerOrderResponse resp = placeOrder(req);
        if (resp != null && resp.isAccepted() && resp.getBrokerOrderId() != null) {
            String orderId = resp.getBrokerOrderId();
            pos.setTargetOrderId(orderId);
            pos.setTargetPlaced(true);
            positionManager.updatePosition(pos);
            try {
                PendingOrder po = pendingOrderRepository.create(orderId, symbol, TradeSide.SELL,
                        pos.getStrategy(), qty, target, false, "SHOONYA", ExecutionRoute.SHOONYA, null);
                pendingOrderRepository.save(po);
            } catch (Exception e) {
                log.warn("[ShoonyaBroker] Recover target: PendingOrder persist failed for {}: {}",
                        symbol, e.getMessage());
            }
            log.info("[ShoonyaBroker] Recover target: SELL placed for {} id={}", symbol, orderId);
        } else {
            log.warn("[ShoonyaBroker] Recover target: SELL REJECTED for {} ({})",
                    symbol, resp != null ? resp.getRejectionReason() : "null response");
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Mapping helpers: our enums → Shoonya strings
    // ─────────────────────────────────────────────────────────────────────

    /** RELIANCE → RELIANCE-EQ (NSE equity convention). */
    private String toShoonyaSymbol(String kiteSymbol) {
        if (kiteSymbol == null) return null;
        return kiteSymbol.endsWith("-EQ") ? kiteSymbol : kiteSymbol + "-EQ";
    }

    private String toShoonyaProduct(ProductType productType) {
        if (productType == null) return "I"; // default MIS
        switch (productType) {
            case DELIVERY: return "C";  // CNC
            case MARGIN:   return "M";  // NRML
            case INTRADAY:
            default:       return "I";  // MIS
        }
    }

    private String toShoonyaOrderType(OrderType orderType) {
        if (orderType == null) return "LMT";
        switch (orderType) {
            case STOP_LOSS:
            case STOP_LOSS_MARKET:
                return "SL-LMT";
            case LIMIT:
            case MARKET:
            default:
                return "LMT"; // Shoonya rejects MKT — we send LMT always
        }
    }

    private String toShoonyaTranType(TransactionType tranType) {
        return tranType == TransactionType.SELL ? "S" : "B";
    }

    /** Shoonya status string → our BrokerOrderStatus enum. */
    private BrokerOrderStatus mapShoonyaStatus(String status) {
        if (status == null) return null;
        switch (status.toUpperCase()) {
            case "COMPLETE":
            case "FILLED":
                return BrokerOrderStatus.COMPLETE;
            case "REJECTED":
                return BrokerOrderStatus.REJECTED;
            case "CANCELLED":
                return BrokerOrderStatus.CANCELLED;
            case "PARTIALLY_FILLED":
                return BrokerOrderStatus.PARTIALLY_FILLED;
            case "OPEN":
            case "PENDING":
            case "PENDING_ALL":
            default:
                return BrokerOrderStatus.OPEN;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // PositionBook → PositionManager sync (broker = SHOONYA)
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Sync Shoonya's PositionBook into PositionManager, keyed by broker=SHOONYA.
     * Shoonya is the source of truth for qty/avg of its OWN positions. The
     * PositionBook carries no strategy, so an existing SHOONYA position for the
     * symbol+type is updated in place; otherwise a new row is created with the
     * default strategy. A flat (netqty ≤ 0) line removes the tracked position.
     */
    private void syncPositionsIntoManager(JsonNode posResp) {
        for (JsonNode node : posResp) {
            try {
                String tsym = textOr(node, "tsym", null);
                if (tsym == null) continue;

                String symbol = fromShoonyaSymbol(tsym);
                int netQty = parseIntSafe(textOr(node, "netqty", "0"));
                double avg = parseDoubleSafe(textOr(node, "netavgprc", "0"));
                Position.PositionType type = fromShoonyaProduct(textOr(node, "prd", "I"));

                Position existing = findShoonyaPosition(symbol, type);

                if (netQty <= 0) {
                    // Flat/closed at Shoonya — drop our row if we still track it.
                    if (existing != null) {
                        positionManager.removePosition(symbol, existing.getStrategy(), "SHOONYA");
                        log.info("[ShoonyaBroker] Reconcile: removed flat SHOONYA position {} ({})",
                                symbol, existing.getStrategy());
                    }
                    continue;
                }

                if (existing != null) {
                    existing.setTotalQuantity(netQty);
                    existing.setAveragePrice(avg);
                    positionManager.updatePosition(existing);
                } else {
                    Position p = new Position(symbol, netQty, avg, 0, true,
                            DEFAULT_STRATEGY, null, null, null, type);
                    p.setBroker("SHOONYA");
                    p.setExecutionRoute(ExecutionRoute.SHOONYA);
                    positionManager.addPosition(p);
                    log.info("[ShoonyaBroker] Reconcile: added SHOONYA {} {} qty={} avg={}",
                            type, symbol, netQty, avg);
                }
            } catch (Exception e) {
                log.warn("[ShoonyaBroker] Failed to sync a PositionBook row: {}", e.getMessage());
            }
        }
    }

    /**
     * Sync Shoonya's demat Holdings (POST /Holdings) into PositionManager as
     * HOLDING positions keyed by broker=SHOONYA. Unlike PositionBook (intraday
     * day positions), Holdings carries the settled overnight demat stock that the
     * morning HoldingsTargetSetScheduler must place targets for.
     *
     * Sellable qty follows Shoonya's documented holding valuation quantity:
     *   holdqty + brkcolqty + unplgdqty + benqty + max(npoadqty, dpqty) + btstqty
     *   - usedqty          (usedqty = quantity already sold today)
     * A POA-free / DP-settled account reports "0" in holdqty and carries the real
     * quantity in dpqty/npoadqty, so summing only holdqty wrongly drops the holding
     * (which is why such Shoonya holdings never appeared on the dashboard). A
     * dually-listed scrip carries multiple exch_tsym entries; the NSE trading symbol
     * is used since our system is NSE Kite-symbol based. A row with no sellable qty
     * removes the tracked position.
     */
    private void syncHoldingsIntoManager(JsonNode holdResp) {
        for (JsonNode node : holdResp) {
            try {
                if ("Not_Ok".equalsIgnoreCase(textOr(node, "stat", "Ok"))) continue;

                String tsym = nseTradingSymbol(node.get("exch_tsym"));
                if (tsym == null) continue;

                String symbol = fromShoonyaSymbol(tsym);
                int holdQty = parseIntSafe(textOr(node, "holdqty", "0"));
                int dpQty = parseIntSafe(textOr(node, "dpqty", "0"));
                int npoadQty = parseIntSafe(textOr(node, "npoadqty", "0"));
                int benQty = parseIntSafe(textOr(node, "benqty", "0"));
                int unplgdQty = parseIntSafe(textOr(node, "unplgdqty", "0"));
                int brkcolQty = parseIntSafe(textOr(node, "brkcolqty", "0"));
                int btstQty = parseIntSafe(textOr(node, "btstqty", "0"));
                int usedQty = parseIntSafe(textOr(node, "usedqty", "0"));
                // Shoonya documented valuation quantity — captures holdings regardless
                // of which bucket they settle in (holdqty is 0 on POA-free accounts).
                int sellableQty = holdQty + brkcolQty + unplgdQty + benQty
                        + Math.max(npoadQty, dpQty) + btstQty - usedQty;
                double avg = parseDoubleSafe(textOr(node, "upldprc", "0"));

                Position existing = findShoonyaPosition(symbol, Position.PositionType.HOLDING);

                if (sellableQty <= 0) {
                    // Nothing left to sell (fully used today) — drop our row if tracked.
                    if (existing != null) {
                        positionManager.removePosition(symbol, existing.getStrategy(), "SHOONYA");
                        log.info("[ShoonyaBroker] Reconcile: removed zero-qty SHOONYA holding {} ({})",
                                symbol, existing.getStrategy());
                    }
                    continue;
                }

                if (existing != null) {
                    existing.setTotalQuantity(sellableQty);
                    existing.setAveragePrice(avg);
                    positionManager.updatePosition(existing);
                } else {
                    Position p = new Position(symbol, sellableQty, avg, 0, true,
                            DEFAULT_STRATEGY, null, null, null, Position.PositionType.HOLDING);
                    p.setBroker("SHOONYA");
                    p.setExecutionRoute(ExecutionRoute.SHOONYA);
                    positionManager.addPosition(p);
                    log.info("[ShoonyaBroker] Reconcile: added SHOONYA HOLDING {} qty={} avg={}",
                            symbol, sellableQty, avg);
                }
            } catch (Exception e) {
                log.warn("[ShoonyaBroker] Failed to sync a Holdings row: {}", e.getMessage());
            }
        }
    }

    /** Pick the NSE trading symbol from a Shoonya exch_tsym array (fallback: first). */
    private String nseTradingSymbol(JsonNode exchTsym) {
        if (exchTsym == null || !exchTsym.isArray() || exchTsym.size() == 0) return null;
        for (JsonNode e : exchTsym) {
            if ("NSE".equalsIgnoreCase(textOr(e, "exch", ""))) {
                return textOr(e, "tsym", null);
            }
        }
        return textOr(exchTsym.get(0), "tsym", null);
    }

    /** Find a SHOONYA position for this symbol+type across strategies, or null. */
    private Position findShoonyaPosition(String symbol, Position.PositionType type) {
        Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);
        for (Position p : strategyPositions.values()) {
            if (p != null && p.getPositionType() == type
                    && "SHOONYA".equals(p.getBroker() != null ? p.getBroker() : "KITE")) {
                return p;
            }
        }
        return null;
    }

    /** RELIANCE-EQ → RELIANCE (reverse of toShoonyaSymbol). */
    private String fromShoonyaSymbol(String shoonyaSymbol) {
        if (shoonyaSymbol == null) return null;
        return shoonyaSymbol.endsWith("-EQ")
                ? shoonyaSymbol.substring(0, shoonyaSymbol.length() - 3) : shoonyaSymbol;
    }

    /** Shoonya product code → PositionType: C(CNC)→HOLDING, else INTRADAY. */
    private Position.PositionType fromShoonyaProduct(String prd) {
        return "C".equalsIgnoreCase(prd)
                ? Position.PositionType.HOLDING : Position.PositionType.INTRADAY;
    }

    private int parseIntSafe(String s) {
        try { return (int) Math.round(Double.parseDouble(s.trim())); } catch (Exception e) { return 0; }
    }

    private double parseDoubleSafe(String s) {
        try { return Double.parseDouble(s.trim()); } catch (Exception e) { return 0; }
    }

    // ─────────────────────────────────────────────────────────────────────
    // JSON helpers
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Build the Noren form body. The Noren API parses the substring after
     * "jData=" directly as a JSON object, so jData must be the RAW JSON string
     * — URL-encoding it makes the server reject it with
     * "jData is not valid json object". jKey (the token) is alphanumeric and is
     * likewise sent as-is.
     */
    private String norenBody(String jData) {
        return "jData=" + jData + "&jKey=" + shoonyaConfig.getAccessToken();
    }

    private String textOr(JsonNode node, String field, String def) {
        JsonNode f = node.get(field);
        return (f != null && !f.isNull()) ? f.asText() : def;
    }

    private double doubleOr(JsonNode node, String field, double def) {
        JsonNode f = node.get(field);
        if (f == null || f.isNull()) return def;
        try {
            return Double.parseDouble(f.asText());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
