package com.satyam.trading2.broker.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.satyam.trading2.broker.model.BrokerOrderEvent;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.config.ShoonyaConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * Shoonya order-update WebSocket client.
 *
 * Connect handshake: send {"t":"a","uid":"...","actid":"...","source":"API",
 * "accesstoken":"..."} on open. Wait for {"t":"ak","s":"Ok"} ack.
 *
 * Order updates arrive automatically as {"t":"om", ...} frames — no separate
 * subscribe needed. Fields: norenordno, status, reporttype, fillshares, avgprc.
 *
 * We map status + reporttype → BrokerOrderStatus and dispatch via the listener.
 */
@Slf4j
public class ShoonyaOrderWebSocketClient extends BrokerWebSocketClient {

    private final ShoonyaConfig config;

    public ShoonyaOrderWebSocketClient(ShoonyaConfig config, BrokerOrderEventListener listener) {
        super(config.getWsUrl(), listener);
        this.config = config;
    }

    @Override
    protected String getBrokerName() { return "SHOONYA"; }

    @Override
    protected boolean isAuthenticated() {
        return config.isAuthenticated();
    }

    @Override
    protected String buildConnectPayload() {
        // Shoonya connect frame: t="a"
        Map<String, String> payload = new HashMap<>();
        payload.put("t", "a");
        payload.put("uid", config.getUserId());
        payload.put("actid", config.getActid());
        payload.put("source", "API");
        payload.put("accesstoken", config.getAccessToken());
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.error("[SHOONYAWS] Failed to serialize connect payload: {}", e.getMessage());
            return null;
        }
    }

    @Override
    protected void handleMessage(String text) {
        try {
            JsonNode msg = objectMapper.readTree(text);
            String type = msg.has("t") ? msg.get("t").asText() : "";

            switch (type) {
                case "ak":
                    // Connect acknowledgment
                    String s = msg.has("s") ? msg.get("s").asText() : "";
                    if ("Ok".equals(s)) {
                        log.info("[SHOONYAWS] Connect acknowledged — order updates will arrive automatically");
                    } else {
                        log.error("[SHOONYAWS] Connect rejected: s={}", s);
                    }
                    return;

                case "om":
                    // Order update — normalize and dispatch
                    BrokerOrderEvent event = parseOrderUpdate(msg);
                    if (event != null && listener != null) {
                        listener.onOrderEvent(event);
                    }
                    return;

                default:
                    // Ignore other message types (ticks, pings, etc.)
                    log.debug("[SHOONYAWS] Ignoring message type: {}", type);
            }
        } catch (Exception e) {
            log.error("[SHOONYAWS] Parse error: {} | raw: {}", e.getMessage(), text);
        }
    }

    /**
     * Parse a Shoonya "om" order-update frame into BrokerOrderEvent.
     *
     * Shoonya fields: norenordno, status, reporttype, fillshares, avgprc, remarks.
     * We drive off `status` (New, Replaced, Complete, Rejected, Cancelled) and
     * use reporttype for additional context.
     */
    private BrokerOrderEvent parseOrderUpdate(JsonNode msg) {
        String norenordno = textOr(msg, "norenordno", null);
        if (norenordno == null) {
            log.warn("[SHOONYAWS] om message missing norenordno: {}", msg);
            return null;
        }

        String status = textOr(msg, "status", "");
        String reporttype = textOr(msg, "reporttype", "");
        BrokerOrderStatus mappedStatus = mapShoonyaStatus(status, reporttype);
        if (mappedStatus == null) {
            log.debug("[SHOONYAWS] Unmapped status '{}' / reporttype '{}' for {}", status, reporttype, norenordno);
            return null;
        }

        int fillShares = intOr(msg, "fillshares", 0);
        double avgPrc = doubleOr(msg, "avgprc", 0);
        String remarks = textOr(msg, "remarks", null);

        return BrokerOrderEvent.builder()
                .broker("SHOONYA")
                .brokerOrderId(norenordno)
                .signalId(remarks)  // we stored strategyId/signalId in remarks on placeOrder
                .status(mappedStatus)
                .filledQuantity(fillShares)
                .averagePrice(avgPrc)
                .rejectionReason("Rejected".equalsIgnoreCase(status) ? textOr(msg, "rejreason", null) : null)
                .build();
    }

    /**
     * Map Shoonya status + reporttype → our BrokerOrderStatus.
     *
     * Per docs, drive transitions off reporttype, but map to our 5 states:
     *   COMPLETE  ← status "COMPLETE" or reporttype "Fill"
     *   REJECTED  ← status "REJECTED" or reporttype "Rejected"
     *   CANCELLED ← status "CANCELLED" or reporttype "Canceled"
     *   PARTIALLY_FILLED ← reporttype "Partial Fill"
     *   OPEN      ← status "New"/"Replaced"/"Pending" or reporttype "New"/"Replaced"/"Pending"
     */
    private BrokerOrderStatus mapShoonyaStatus(String status, String reporttype) {
        String s = status == null ? "" : status.toUpperCase();
        String r = reporttype == null ? "" : reporttype.toUpperCase();

        if (s.contains("COMPLETE") || r.contains("FILL") && !r.contains("PARTIAL")) {
            return BrokerOrderStatus.COMPLETE;
        }
        if (s.contains("REJECT") || r.contains("REJECT")) {
            return BrokerOrderStatus.REJECTED;
        }
        if (s.contains("CANCEL") || r.contains("CANCEL")) {
            return BrokerOrderStatus.CANCELLED;
        }
        if (r.contains("PARTIAL")) {
            return BrokerOrderStatus.PARTIALLY_FILLED;
        }
        if (s.contains("NEW") || s.contains("REPLACED") || s.contains("PENDING")
                || r.contains("NEW") || r.contains("REPLACED") || r.contains("PENDING")) {
            return BrokerOrderStatus.OPEN;
        }
        return null;
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
