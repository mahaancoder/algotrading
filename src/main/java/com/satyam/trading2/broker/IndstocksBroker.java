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
import com.satyam.trading2.broker.ws.IndstocksOrderWebSocketClient;
import com.satyam.trading2.broker.auth.IndstocksInstrumentsService;
import com.satyam.trading2.config.IndstocksConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * INDstocks execution broker — Group 1.
 *
 * REST format: JSON body + Authorization: <access_token> header (no "Bearer").
 * Base URL: https://api.indstocks.com
 *
 * Endpoints:
 *   POST /order           — place order
 *   POST /order/modify    — modify order
 *   POST /order/cancel    — cancel order
 *   GET  /order           — get single order (sends JSON body!)
 *   GET  /order-book      — order book
 *   GET  /funds           — funds/margin
 *
 * Symbol mapping: INDstocks uses numeric security_id, not trading symbol.
 * The security_id must be resolved from the instruments master before placing
 * orders. symbolToSecurityId map is populated by an instruments fetch at startup.
 * If a symbol isn't in the map, the order is rejected with a clear message.
 *
 * Product mapping: DELIVERY→"CNC", INTRADAY→"INTRADAY", MARGIN→"MARGIN".
 * Order type mapping: LIMIT→"LIMIT", MARKET→"MARKET" (auto-converted to LIMIT at LTP).
 *
 * Activated only when broker.indstocks.enabled=true.
 * INDstocks is EXECUTION-ONLY — market data always comes from Kite.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "broker.indstocks.enabled", havingValue = "true")
public class IndstocksBroker extends AbstractRestBroker {

    private final IndstocksConfig indstocksConfig;
    private final BrokerOrderEventProcessor brokerOrderEventProcessor;

    // Lazy to break the bean-creation cycle: IndstocksInstrumentsService already
    // depends on IndstocksBroker (to register mappings). Only used at runtime for
    // self-healing lazy population, never during construction.
    @Lazy
    @Autowired
    private IndstocksInstrumentsService indstocksInstrumentsService;

    // symbol → INDstocks numeric security_id. Populated from instruments master.
    // E.g. "RELIANCE" → "2885". TODO: fetch from /instruments at startup.
    private final Map<String, String> symbolToSecurityId = new ConcurrentHashMap<>();

    // WebSocket client for order updates — created on startOrderUpdates()
    private volatile IndstocksOrderWebSocketClient wsClient;

    // ─────────────────────────────────────────────────────────────────────
    // Identity
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public String getBrokerName() { return "INDSTOCKS"; }

    @Override
    public ExecutionRoute getRoute() { return ExecutionRoute.INDSTOCKS; }

    /** Register a symbol→securityId mapping (called at startup by instruments fetch). */
    public void registerSecurityId(String symbol, String securityId) {
        symbolToSecurityId.put(symbol, securityId);
    }

    /** Number of symbol→securityId mappings currently loaded. 0 means the
     *  instruments master hasn't been populated (orders will be rejected). */
    public int getMappedSymbolCount() {
        return symbolToSecurityId.size();
    }

    /**
     * Self-healing: if the instruments map is empty but INDstocks is authenticated,
     * try to populate it on-demand. This covers the case where the token was set
     * outside the /auth/indstocks/token flow, or the startup fetch failed/returned 0.
     * @return the map size after the attempt
     */
    private int ensureInstrumentsLoaded() {
        if (!symbolToSecurityId.isEmpty()) {
            return symbolToSecurityId.size();
        }
        if (indstocksInstrumentsService == null || !indstocksConfig.isAuthenticated()) {
            return 0;
        }
        System.out.println("🔄 [IndstocksBroker] Instruments map empty — attempting lazy populate");
        try {
            int mapped = indstocksInstrumentsService.populateInstruments();
            System.out.println("🔄 [IndstocksBroker] Lazy populate mapped " + mapped + " symbols");
        } catch (Exception e) {
            System.out.println("❌ [IndstocksBroker] Lazy populate failed: " + e.getMessage());
        }
        return symbolToSecurityId.size();
    }

    /**
     * Resolve a symbol to its INDstocks security_id, tolerating minor format
     * differences between the caller's symbol and the instruments-master key
     * (SYMBOL_NAME): exact match first, then trimmed/upper-cased, then with a
     * trailing "-EQ" stripped.
     */
    private String resolveSecurityId(String symbol) {
        if (symbol == null) return null;
        String s = symbolToSecurityId.get(symbol);
        if (s != null) return s;
        String norm = symbol.trim().toUpperCase();
        s = symbolToSecurityId.get(norm);
        if (s != null) return s;
        if (norm.endsWith("-EQ")) {
            s = symbolToSecurityId.get(norm.substring(0, norm.length() - 3));
        }
        return s;
    }

    // ─────────────────────────────────────────────────────────────────────
    // placeOrder — POST /order
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public BrokerOrderResponse placeOrder(OrderRequest request) {
        if (!indstocksConfig.isAuthenticated()) {
            return BrokerOrderResponse.builder()
                    .brokerName(getBrokerName())
                    .accepted(false)
                    .rejectionReason("INDstocks not authenticated — no access token")
                    .build();
        }

        String securityId = resolveSecurityId(request.getSymbol());
        if (securityId == null) {
            // Self-heal: try to populate the instruments master on-demand, then retry.
            ensureInstrumentsLoaded();
            securityId = resolveSecurityId(request.getSymbol());
        }
        if (securityId == null) {
            System.out.println("⚠️ [IndstocksBroker] No security_id mapped for symbol "
                    + request.getSymbol() + " (mapped symbols in table: "
                    + symbolToSecurityId.size() + ")");
            return BrokerOrderResponse.builder()
                    .brokerName(getBrokerName())
                    .accepted(false)
                    .rejectionReason("No security_id mapped for " + request.getSymbol()
                            + " — populate instruments master first")
                    .build();
        }

        try {
            // Build the JSON body per INDstocks POST /order spec
            Map<String, Object> payload = new HashMap<>();
            payload.put("txn_type", toIndTxnType(request.getTransactionType()));
            payload.put("exchange", request.getExchange() != null ? request.getExchange() : "NSE");
            payload.put("segment", "EQUITY");
            payload.put("product", toIndProduct(request.getProductType()));
            payload.put("order_type", toIndOrderType(request.getOrderType()));
            payload.put("validity", "DAY");
            payload.put("security_id", securityId);
            payload.put("qty", request.getQuantity());
            payload.put("algo_id", indstocksConfig.getAlgoIdNse());
            if (request.getOrderType() == OrderType.LIMIT || request.getPrice() > 0) {
                payload.put("limit_price", request.getPrice());
            }
            payload.put("is_amo", false);
            // remarks = strategy/signalId for reconciliation (max 100 chars)
            String remarks = request.getStrategyId() != null ? request.getStrategyId() : "";
            if (request.getSignalId() != null) {
                remarks = remarks + "/" + request.getSignalId();
            }
            if (remarks.length() > 100) remarks = remarks.substring(0, 100);
            payload.put("remarks", remarks);

            String jsonBody = objectMapper.writeValueAsString(payload);
            JsonNode resp = postJsonBody(indstocksConfig.getBaseUrl() + "/order",
                    jsonBody, indstocksConfig.getAuthHeader());
            String raw = resp.toString();

            String status = textOr(resp, "status", "");
            if ("success".equals(status)) {
                JsonNode data = resp.get("data");
                String orderId = data != null ? textOr(data, "order_id", null) : null;
                String orderStatus = data != null ? textOr(data, "order_status", "") : "";
                log.info("[IndstocksBroker] Order placed: {} {} {} qty={} → order_id={} status={}",
                        request.getTransactionType(), request.getSymbol(),
                        request.getPrice(), request.getQuantity(), orderId, orderStatus);
                return BrokerOrderResponse.builder()
                        .brokerName(getBrokerName())
                        .brokerOrderId(orderId)
                        .accepted(true)
                        .rawResponse(raw)
                        .build();
            } else {
                String reason = textOr(resp, "message",
                        textOr(resp, "error", "Unknown rejection"));
                log.warn("[IndstocksBroker] Order rejected: {} {} → {}", request.getSymbol(),
                        request.getTransactionType(), reason);
                return BrokerOrderResponse.builder()
                        .brokerName(getBrokerName())
                        .accepted(false)
                        .rejectionReason(reason)
                        .rawResponse(raw)
                        .build();
            }
        } catch (Exception e) {
            log.error("[IndstocksBroker] placeOrder exception: {} {}", request.getSymbol(), e.getMessage(), e);
            return BrokerOrderResponse.builder()
                    .brokerName(getBrokerName())
                    .accepted(false)
                    .rejectionReason("Exception: " + e.getMessage())
                    .build();
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // cancelOrder — POST /order/cancel
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public void cancelOrder(String brokerOrderId) {
        if (!indstocksConfig.isAuthenticated()) {
            log.warn("[IndstocksBroker] cancelOrder skipped — not authenticated. orderId={}", brokerOrderId);
            return;
        }
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("order_id", brokerOrderId);
            payload.put("segment", "EQUITY");

            String jsonBody = objectMapper.writeValueAsString(payload);
            JsonNode resp = postJsonBody(indstocksConfig.getBaseUrl() + "/order/cancel",
                    jsonBody, indstocksConfig.getAuthHeader());
            String status = textOr(resp, "status", "");
            if ("success".equals(status)) {
                log.info("[IndstocksBroker] Order cancelled: {}", brokerOrderId);
            } else {
                log.warn("[IndstocksBroker] Cancel failed: {} → {}", brokerOrderId,
                        textOr(resp, "message", ""));
            }
        } catch (Exception e) {
            log.error("[IndstocksBroker] cancelOrder exception: {} {}", brokerOrderId, e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // modifyOrder — POST /order/modify
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public void modifyOrder(String brokerOrderId, OrderRequest request) {
        if (!indstocksConfig.isAuthenticated()) {
            log.warn("[IndstocksBroker] modifyOrder skipped — not authenticated. orderId={}", brokerOrderId);
            return;
        }
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("order_id", brokerOrderId);
            payload.put("segment", "EQUITY");
            payload.put("qty", request.getQuantity());
            payload.put("limit_price", request.getPrice());

            String jsonBody = objectMapper.writeValueAsString(payload);
            JsonNode resp = postJsonBody(indstocksConfig.getBaseUrl() + "/order/modify",
                    jsonBody, indstocksConfig.getAuthHeader());
            String status = textOr(resp, "status", "");
            if ("success".equals(status)) {
                log.info("[IndstocksBroker] Order modified: {}", brokerOrderId);
            } else {
                log.warn("[IndstocksBroker] Modify failed: {} → {}", brokerOrderId,
                        textOr(resp, "message", ""));
            }
        } catch (Exception e) {
            log.error("[IndstocksBroker] modifyOrder exception: {} {}", brokerOrderId, e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // getOrderStatus — GET /order (with JSON body — unusual but per docs)
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public BrokerOrderStatus getOrderStatus(String brokerOrderId) {
        if (!indstocksConfig.isAuthenticated()) {
            log.warn("[IndstocksBroker] getOrderStatus skipped — not authenticated. orderId={}", brokerOrderId);
            return null;
        }
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("order_id", brokerOrderId);
            payload.put("segment", "EQUITY");

            String jsonBody = objectMapper.writeValueAsString(payload);
            JsonNode resp = getWithBody(indstocksConfig.getBaseUrl() + "/order",
                    jsonBody, indstocksConfig.getAuthHeader());
            String status = textOr(resp, "status", "");
            if (!"success".equals(status)) {
                log.warn("[IndstocksBroker] getOrderStatus failed: {} → {}", brokerOrderId,
                        textOr(resp, "message", ""));
                return null;
            }
            JsonNode data = resp.get("data");
            if (data == null) return null;
            // INDstocks statuses: QUEUED, INITIATED, PENDING, SUCCESS, CANCELLED,
            // EXPIRED, FAILED, PARTIALLY FILLED, etc.
            return mapIndStatus(textOr(data, "status", ""));
        } catch (Exception e) {
            log.error("[IndstocksBroker] getOrderStatus exception: {} {}", brokerOrderId, e.getMessage(), e);
            return null;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // getFunds — GET /funds
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public BrokerFunds getFunds() {
        if (!indstocksConfig.isAuthenticated()) {
            System.out.println("⚠️ [IndstocksBroker] getFunds skipped — not authenticated (no access token set)");
            return BrokerFunds.builder()
                    .brokerName(getBrokerName())
                    .availableMargin(0).usedMargin(0).freeCash(0)
                    .build();
        }
        try {
            JsonNode resp = getJson(indstocksConfig.getBaseUrl() + "/funds",
                    indstocksConfig.getAuthHeader());
            // Diagnostic: print the raw funds response so a ₹0 dashboard can be
            // traced to an API error vs. a field-name mismatch (does NOT include
            // the auth token — only the balance payload).
            System.out.println("🔍 [IndstocksBroker] /funds raw response → " + resp);
            String status = textOr(resp, "status", "");
            if (!"success".equals(status)) {
                System.out.println("⚠️ [IndstocksBroker] getFunds failed: status=" + status
                        + " message=" + textOr(resp, "message", ""));
                return BrokerFunds.builder()
                        .brokerName(getBrokerName())
                        .availableMargin(0).usedMargin(0).freeCash(0)
                        .build();
            }
            JsonNode data = resp.get("data");
            if (data == null) {
                System.out.println("⚠️ [IndstocksBroker] getFunds: status=success but 'data' node missing");
                return BrokerFunds.builder()
                        .brokerName(getBrokerName())
                        .availableMargin(0).usedMargin(0).freeCash(0)
                        .build();
            }
            // sod_balance = start-of-day balance (total available margin)
            // withdrawal_balance = free cash
            // used = sod_balance - withdrawal_balance (approx, ignoring realized P&L)
            double sodBalance = doubleOr(data, "sod_balance", 0);
            double withdrawalBalance = doubleOr(data, "withdrawal_balance", 0);
            double usedMargin = sodBalance - withdrawalBalance;
            if (usedMargin < 0) usedMargin = 0;

            if (sodBalance == 0) {
                System.out.println("⚠️ [IndstocksBroker] getFunds: 'sod_balance' is 0 or absent — "
                        + "check field mapping against data node keys: " + fieldNames(data));
            }

            return BrokerFunds.builder()
                    .brokerName(getBrokerName())
                    .availableMargin(sodBalance)
                    .usedMargin(usedMargin)
                    .freeCash(withdrawalBalance)
                    .build();
        } catch (Exception e) {
            System.out.println("❌ [IndstocksBroker] getFunds exception: " + e.getMessage());
            log.error("[IndstocksBroker] getFunds exception: {}", e.getMessage(), e);
            return BrokerFunds.builder()
                    .brokerName(getBrokerName())
                    .availableMargin(0).usedMargin(0).freeCash(0)
                    .build();
        }
    }

    /** Comma-joined top-level field names of a JSON node (for diagnostics). */
    private static String fieldNames(JsonNode node) {
        if (node == null) return "<null>";
        StringBuilder sb = new StringBuilder();
        node.fieldNames().forEachRemaining(n -> {
            if (sb.length() > 0) sb.append(", ");
            sb.append(n);
        });
        return sb.toString();
    }

    // ─────────────────────────────────────────────────────────────────────
    // startOrderUpdates / stopOrderUpdates — WebSocket (stub for now)
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public void startOrderUpdates() {
        if (!indstocksConfig.isAuthenticated()) {
            log.warn("[IndstocksBroker] startOrderUpdates skipped — not authenticated");
            return;
        }
        if (wsClient != null && wsClient.isRunning()) {
            log.info("[IndstocksBroker] Order updates already running");
            return;
        }
        log.info("[IndstocksBroker] startOrderUpdates — connecting INDstocks order WebSocket to {}",
                indstocksConfig.getWsOrderUrl());

        // Listener: route each BrokerOrderEvent through the broker-neutral processor
        BrokerOrderEventListener listener = event -> {
            try {
                brokerOrderEventProcessor.process(event);
            } catch (Exception e) {
                log.error("[IndstocksBroker] Error processing order event: {}", e.getMessage(), e);
            }
        };

        wsClient = new IndstocksOrderWebSocketClient(indstocksConfig, listener);
        wsClient.start();
    }

    @Override
    public void stopOrderUpdates() {
        log.info("[IndstocksBroker] stopOrderUpdates — disconnecting INDstocks order WebSocket");
        if (wsClient != null) {
            wsClient.stop();
            wsClient = null;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // reconcileOrders — GET /order-book + GET /trade-book
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public void reconcileOrders() {
        if (!indstocksConfig.isAuthenticated()) {
            log.warn("[IndstocksBroker] reconcileOrders skipped — not authenticated");
            return;
        }
        log.info("[IndstocksBroker] reconcileOrders — fetching order book from INDstocks");
        try {
            JsonNode resp = getJson(indstocksConfig.getBaseUrl() + "/order-book",
                    indstocksConfig.getAuthHeader());
            String status = textOr(resp, "status", "");
            if ("success".equals(status)) {
                JsonNode data = resp.get("data");
                int count = data != null && data.isArray() ? data.size() : 0;
                log.info("[IndstocksBroker] Reconciliation: {} orders from INDstocks order book", count);
                // TODO: match each order's id against PendingOrderRepository by
                // (broker=INDSTOCKS, brokerOrderId) and update status if drifted.
                // Statuses: SUCCESS→COMPLETE, CANCELLED→CANCELLED, FAILED/EXPIRED→REJECTED.
            }
        } catch (Exception e) {
            log.error("[IndstocksBroker] reconcileOrders exception: {}", e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Mapping helpers: our enums → INDstocks strings
    // ─────────────────────────────────────────────────────────────────────

    private String toIndTxnType(TransactionType tranType) {
        return tranType == TransactionType.SELL ? "SELL" : "BUY";
    }

    private String toIndProduct(ProductType productType) {
        if (productType == null) return "INTRADAY";
        switch (productType) {
            case DELIVERY: return "CNC";
            case MARGIN:   return "MARGIN";
            case INTRADAY:
            default:       return "INTRADAY";
        }
    }

    private String toIndOrderType(OrderType orderType) {
        if (orderType == null) return "LIMIT";
        switch (orderType) {
            case MARKET:          return "MARKET"; // auto-converted to LIMIT at LTP by INDstocks
            case STOP_LOSS:
            case STOP_LOSS_MARKET:
                return "STOP_LOSS";
            case LIMIT:
            default:
                return "LIMIT";
        }
    }

    /** INDstocks status string → our BrokerOrderStatus enum. */
    private BrokerOrderStatus mapIndStatus(String status) {
        if (status == null) return null;
        switch (status.toUpperCase()) {
            case "SUCCESS":
                return BrokerOrderStatus.COMPLETE;
            case "CANCELLED":
            case "PARTIALLY FILLED - CANCELLED":
                return BrokerOrderStatus.CANCELLED;
            case "FAILED":
            case "ABORTED":
            case "EXPIRED":
            case "PARTIALLY FILLED - EXPIRED":
                return BrokerOrderStatus.REJECTED;
            case "PARTIALLY FILLED":
                return BrokerOrderStatus.PARTIALLY_FILLED;
            case "QUEUED":
            case "INITIATED":
            case "PENDING":
            case "PROCESSING":
            case "O-PENDING":
            case "SL-PENDING":
            default:
                return BrokerOrderStatus.OPEN;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // JSON helpers
    // ─────────────────────────────────────────────────────────────────────

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
