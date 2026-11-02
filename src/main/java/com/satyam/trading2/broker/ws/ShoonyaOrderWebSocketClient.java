package com.satyam.trading2.broker.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.satyam.trading2.broker.model.BrokerOrderEvent;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.config.ShoonyaConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Shoonya order-update WebSocket client.
 *
 * Connect handshake: send {"t":"a","uid":"...","actid":"...","source":"API",
 * "accesstoken":"..."} on open. The server acks with {"t":"ak","s":"OK"}
 * (OAuth flow) or {"t":"ck","s":"OK"} (legacy).
 *
 * After the ack we must (1) send a keepalive heartbeat {"t":"h"} every ~3s —
 * without it the Noren server drops the socket abnormally (close 1006) — and
 * (2) subscribe to order updates {"t":"o","actid":"..."}. Order updates then
 * arrive as {"t":"om", ...} frames. Fields: norenordno, status, reporttype,
 * fillshares, avgprc.
 *
 * We map status + reporttype → BrokerOrderStatus and dispatch via the listener.
 */
@Slf4j
public class ShoonyaOrderWebSocketClient extends BrokerWebSocketClient {

    private static final long HEARTBEAT_INTERVAL_SECONDS = 3;

    private final ShoonyaConfig config;
    private final ScheduledExecutorService heartbeatExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "shoonya-ws-heartbeat");
                t.setDaemon(true);
                return t;
            });
    private volatile ScheduledFuture<?> heartbeatTask;

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
                case "ak":  // OAuth-flow connect ack
                case "ck":  // legacy connect ack
                    // Success value is "OK" per Noren docs; accept any case.
                    String s = msg.has("s") ? msg.get("s").asText() : "";
                    if ("OK".equalsIgnoreCase(s)) {
                        log.info("[SHOONYAWS] Connect acknowledged (t={}) — starting heartbeat + subscribing to order updates", type);
                        startHeartbeat();
                        subscribeOrderUpdates();
                    } else {
                        log.error("[SHOONYAWS] Connect rejected (t={} s={}) | raw: {}", type, s, text);
                    }
                    return;

                case "ok":  // order-subscription ack
                    log.info("[SHOONYAWS] Order feed subscribed — updates will arrive as 'om' frames");
                    return;

                case "om":
                    // Order update — normalize and dispatch
                    BrokerOrderEvent event = parseOrderUpdate(msg);
                    if (event != null && listener != null) {
                        listener.onOrderEvent(event);
                    }
                    return;

                default:
                    // Ignore other message types (heartbeat echoes, ticks, etc.)
                    log.debug("[SHOONYAWS] Ignoring message type: {}", type);
            }
        } catch (Exception e) {
            log.error("[SHOONYAWS] Parse error: {} | raw: {}", e.getMessage(), text);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Keepalive heartbeat + order-feed subscription
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Send the order-feed subscription frame ({"t":"o","actid":"..."}).
     * The server replies with an "ok" ack, after which "om" order updates flow.
     */
    private void subscribeOrderUpdates() {
        Map<String, String> sub = new HashMap<>();
        sub.put("t", "o");
        sub.put("actid", config.getActid());
        try {
            sendMessage(objectMapper.writeValueAsString(sub));
        } catch (Exception e) {
            log.error("[SHOONYAWS] Failed to serialize order-subscribe payload: {}", e.getMessage());
        }
    }

    /**
     * Start the {"t":"h"} keepalive. Noren drops the socket abnormally
     * (close 1006) if no heartbeat arrives within a few seconds.
     */
    private synchronized void startHeartbeat() {
        stopHeartbeat();
        heartbeatTask = heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                sendMessage("{\"t\":\"h\"}");
            } catch (Exception e) {
                log.debug("[SHOONYAWS] Heartbeat send failed: {}", e.getMessage());
            }
        }, HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private synchronized void stopHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
    }

    @Override
    protected void onDisconnected() {
        // Stop the heartbeat when the socket drops; it restarts on the next ack.
        stopHeartbeat();
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

        Integer fillShares = nullableIntOr(msg, "fillshares");
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

    private Integer nullableIntOr(JsonNode node, String field) {
        JsonNode f = node.get(field);
        if (f == null || f.isNull()) return null;
        try { return Integer.parseInt(f.asText()); } catch (NumberFormatException e) { return null; }
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
