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
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.domain.service.PositionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;


import java.util.HashMap;
import java.util.Map;

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
            double available = credit - marginUsed - payout;
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
        } catch (Exception e) {
            log.error("[ShoonyaBroker] reconcileOrders exception: {}", e.getMessage(), e);
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
