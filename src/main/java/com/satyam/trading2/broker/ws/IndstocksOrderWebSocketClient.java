package com.satyam.trading2.broker.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.satyam.trading2.broker.model.BrokerOrderEvent;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.config.IndstocksConfig;

/**
 * INDstocks order-update WebSocket client.
 *
 * Connect: wss://ws-order-updates.indstocks.com/api/v1/ws/trades
 * Auth: Authorization header on the WS handshake (same token as REST).
 *
 * After open, send: {"action":"subscribe","mode":"order_update"}
 * Then order updates arrive as JSON frames with data.order_status codes:
 *   R=Received, P=Pending, S=Success, F=Failed, C=Cancelled,
 *   RJ=Rejected, PF=Partially filled, PFC=Partially filled-cancelled
 *
 * Note: The INDstocks WS sends frames as a JSON-encoded *string* — i.e.
 * the frame is a quoted string that itself contains JSON. We handle that
 * double-encoding by detecting and unwrapping it.
 */
public class IndstocksOrderWebSocketClient extends BrokerWebSocketClient {

    private final IndstocksConfig config;

    public IndstocksOrderWebSocketClient(IndstocksConfig config, BrokerOrderEventListener listener) {
        super(config.getWsOrderUrl(), listener);
        this.config = config;
    }

    @Override
    protected String getBrokerName() { return "INDSTOCKS"; }

    @Override
    protected boolean isAuthenticated() {
        return config.isAuthenticated();
    }

    /**
     * Add the Authorization header to the WS handshake builder.
     * INDstocks authenticates the WS connection via the same token as REST.
     */
    @Override
    protected void customizeBuilder(java.net.http.WebSocket.Builder builder) {
        builder.header("Authorization", config.getAuthHeader());
    }

    @Override
    protected String buildConnectPayload() {
        // Subscribe to order updates
        return "{\"action\":\"subscribe\",\"mode\":\"order_update\"}";
    }

    @Override
    protected void handleMessage(String text) {
        try {
            // Log EVERY raw frame IND Money sends on this socket, so we can see
            // whether the order-completed event actually arrives here and what
            // shape it has (mode value, field names). Without this, a fill frame
            // with an unexpected `mode` is dropped silently and the target SELL
            // is never placed.
            System.out.println("[INDSTOCKSWS] RAW frame: " + text);

            // INDstocks frames may be double-encoded: a JSON string containing JSON.
            // If the frame starts with a quote, unwrap it first.
            String jsonStr = text;
            if (jsonStr.trim().startsWith("\"")) {
                jsonStr = objectMapper.readTree(jsonStr).asText();
            }

            JsonNode msg = objectMapper.readTree(jsonStr);
            String mode = msg.has("mode") ? msg.get("mode").asText() : "";

            if (!"order_update".equals(mode)) {
                // Not the expected order-update shape — log at INFO (not debug) so
                // a fill event delivered under a different `mode` is visible instead
                // of being dropped silently.
                System.out.println("[INDSTOCKSWS] Ignoring non-order_update frame: mode='" + mode + "' | body=" + jsonStr);
                return;
            }

            JsonNode data = msg.get("data");
            if (data == null) {
                System.out.println("[INDSTOCKSWS] order_update missing data field: " + jsonStr);
                return;
            }

            BrokerOrderEvent event = parseOrderUpdate(data);
            if (event != null && listener != null) {
                System.out.println("[INDSTOCKSWS] Dispatching event → orderId=" + event.getBrokerOrderId()
                        + " status=" + event.getStatus()
                        + " filledQty=" + event.getFilledQuantity()
                        + " avgPrice=" + event.getAveragePrice());
                listener.onOrderEvent(event);
            } else {
                System.out.println("[INDSTOCKSWS] No event dispatched (event=" + (event != null)
                        + ", listener=" + (listener != null) + ")");
            }
        } catch (Exception e) {
            System.out.println("[INDSTOCKSWS] Parse error: " + e.getMessage() + " | raw: " + text);
            e.printStackTrace();
        }
    }

    /**
     * Parse an INDstocks order update data object into BrokerOrderEvent.
     *
     * Fields: order_id, order_status (short code), req_quantity, executed_price,
     * lot, error_message.
     */
    private BrokerOrderEvent parseOrderUpdate(JsonNode data) {
        String orderId = textOr(data, "order_id", null);
        if (orderId == null) {
            System.out.println("[INDSTOCKSWS] order_update missing order_id: " + data);
            return null;
        }

        String statusCode = textOr(data, "order_status", "");
        BrokerOrderStatus mappedStatus = mapIndStatus(statusCode);
        if (mappedStatus == null) {
            System.out.println("[INDSTOCKSWS] Unmapped status code '" + statusCode + "' for " + orderId);
            return null;
        }

        // executed_price and lot are only sent on execution (status S)
        double executedPrice = doubleOr(data, "executed_price", 0);
        int reqQuantity = intOr(data, "req_quantity", 0);
        int lot = intOr(data, "lot", 0);

        // For filled quantity: if lot is present, use lot * (req_quantity / requested_lot)
        // Otherwise just use req_quantity on success. INDstocks sends lot on execution.
        int filledQty = 0;
        if (mappedStatus == BrokerOrderStatus.COMPLETE) {
            filledQty = reqQuantity; // full fill on S
        } else if (mappedStatus == BrokerOrderStatus.PARTIALLY_FILLED) {
            // lot filled so far — approximate
            filledQty = lot > 0 ? lot : 0;
        }

        String errorMessage = textOr(data, "error_message", " ");
        String rejectionReason = (mappedStatus == BrokerOrderStatus.REJECTED
                || mappedStatus == BrokerOrderStatus.CANCELLED)
                && !errorMessage.trim().isEmpty() ? errorMessage : null;

        return BrokerOrderEvent.builder()
                .broker("INDSTOCKS")
                .brokerOrderId(orderId)
                .status(mappedStatus)
                .filledQuantity(filledQty)
                .averagePrice(executedPrice)
                .rejectionReason(rejectionReason)
                .build();
    }

    /**
     * Map INDstocks short status codes → our BrokerOrderStatus.
     * R=Received, P=Pending, S=Success, F=Failed, C=Cancelled,
     * RJ=Rejected, PF=Partially filled, PFC=Partially filled-cancelled
     */
    private BrokerOrderStatus mapIndStatus(String code) {
        if (code == null || code.isEmpty()) return null;
        switch (code.toUpperCase()) {
            case "S":     return BrokerOrderStatus.COMPLETE;
            case "F":
            case "RJ":    return BrokerOrderStatus.REJECTED;
            case "C":
            case "PFC":   return BrokerOrderStatus.CANCELLED;
            case "PF":    return BrokerOrderStatus.PARTIALLY_FILLED;
            case "R":
            case "P":     return BrokerOrderStatus.OPEN;
            default:      return null;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // JSON helpers
    // ─────────────────────────────────────────────────────────────────────

    private String textOr(JsonNode node, String field, String def) {
        JsonNode f = node.get(field);
        return (f != null && !f.isNull()) ? f.asText() : def;
    }

    private int intOr(JsonNode node, String field, int def) {
        JsonNode f = node.get(field);
        if (f == null || f.isNull()) return def;
        try { return Integer.parseInt(f.asText()); } catch (NumberFormatException e) { return def; }
    }

    private double doubleOr(JsonNode node, String field, double def) {
        JsonNode f = node.get(field);
        if (f == null || f.isNull()) return def;
        try { return Double.parseDouble(f.asText()); } catch (NumberFormatException e) { return def; }
    }
}
