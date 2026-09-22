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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
            payload.put("prc", String.valueOf(request.getPrice()));
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
                    payload.put("trgprc", String.valueOf(request.getTriggerPrice()));
                }
            }

            String jData = objectMapper.writeValueAsString(payload);
            String body = "jData=" + urlEncode(jData) + "&jKey=" + urlEncode(shoonyaConfig.getAccessToken());

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
            String body = "jData=" + urlEncode(jData) + "&jKey=" + urlEncode(shoonyaConfig.getAccessToken());

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
            payload.put("prc", String.valueOf(request.getPrice()));
            payload.put("prctyp", toShoonyaOrderType(request.getOrderType()));
            payload.put("ret", "DAY");

            String jData = objectMapper.writeValueAsString(payload);
            String body = "jData=" + urlEncode(jData) + "&jKey=" + urlEncode(shoonyaConfig.getAccessToken());

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
            String body = "jData=" + urlEncode(jData) + "&jKey=" + urlEncode(shoonyaConfig.getAccessToken());

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
            log.warn("[ShoonyaBroker] getFunds skipped — not authenticated");
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
            String body = "jData=" + urlEncode(jData) + "&jKey=" + urlEncode(shoonyaConfig.getAccessToken());

            JsonNode resp = postForm(shoonyaConfig.getBaseUrl() + "Limits", body);
            String stat = textOr(resp, "stat", "");
            if ("Not_Ok".equals(stat)) {
                log.warn("[ShoonyaBroker] getFunds failed: {}", textOr(resp, "emsg", ""));
                return BrokerFunds.builder()
                        .brokerName(getBrokerName())
                        .availableMargin(0).usedMargin(0).freeCash(0)
                        .build();
            }
            // cash = total margin, marginused = used margin, freeCash = cash - marginused
            double cash = doubleOr(resp, "cash", 0);
            double marginUsed = doubleOr(resp, "marginused", 0);
            double freeCash = cash - marginUsed;

            return BrokerFunds.builder()
                    .brokerName(getBrokerName())
                    .availableMargin(cash)
                    .usedMargin(marginUsed)
                    .freeCash(freeCash)
                    .build();
        } catch (Exception e) {
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
            String posBody = "jData=" + urlEncode(posJData) + "&jKey=" + urlEncode(shoonyaConfig.getAccessToken());

            JsonNode posResp = postForm(shoonyaConfig.getBaseUrl() + "PositionBook", posBody);
            if (posResp.isArray()) {
                log.info("[ShoonyaBroker] Reconciliation: {} open positions from Shoonya", posResp.size());
                // TODO: sync each position with PositionManager keyed by broker=SHOONYA.
                // Fields: tsym (→ strip "-EQ"), netqty, netavgprc, prd (→ ProductType), rpnl.
            }

            // 2. Fetch order book (POST /OrderBook)
            Map<String, String> ordPayload = new HashMap<>();
            ordPayload.put("uid", shoonyaConfig.getUserId());
            String ordJData = objectMapper.writeValueAsString(ordPayload);
            String ordBody = "jData=" + urlEncode(ordJData) + "&jKey=" + urlEncode(shoonyaConfig.getAccessToken());

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
    // JSON helpers
    // ─────────────────────────────────────────────────────────────────────

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
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
