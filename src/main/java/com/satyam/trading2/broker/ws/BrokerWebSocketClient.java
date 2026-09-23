package com.satyam.trading2.broker.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for broker order-update WebSocket clients.
 *
 * Uses Java 11's built-in java.net.http.WebSocket — no external deps.
 * Subclasses implement:
 *   - buildConnectPayload() — the first message to send after open (handshake/subscribe)
 *   - handleMessage() — parse a text frame and dispatch BrokerOrderEvent via the Listener
 *
 * Lifecycle:
 *   start()  → opens WS, sends connect payload, starts listening
 *   stop()   → sends close frame, marks stopped
 *
 * Reconnection: if the WebSocket closes unexpectedly (not via stop()),
 * onClosed() triggers a reconnect with exponential backoff (capped at 30s).
 */
@Slf4j
public abstract class BrokerWebSocketClient {

    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected final BrokerOrderEventListener listener;

    private final HttpClient httpClient;
    private final String wsUrl;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile WebSocket webSocket;
    private int reconnectDelayMs = 1000;
    private static final int MAX_RECONNECT_DELAY_MS = 30000;

    /**
     * @param wsUrl    the WebSocket endpoint URL
     * @param listener callback for each normalized BrokerOrderEvent
     */
    protected BrokerWebSocketClient(String wsUrl, BrokerOrderEventListener listener) {
        this.wsUrl = wsUrl;
        this.listener = listener;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(3))
                .build();
    }

    // ─────────────────────────────────────────────────────────────────────
    // Abstract — subclasses provide broker-specific handshake + message parsing
    // ─────────────────────────────────────────────────────────────────────

    /** The first JSON message to send after the WS opens (handshake/subscribe). */
    protected abstract String buildConnectPayload();

    /** Parse a text frame; if it's an order update, call listener.onOrderEvent(). */
    protected abstract void handleMessage(String text);

    /** Subclasses return false if not yet authenticated (skip connect). */
    protected abstract boolean isAuthenticated();

    /** Broker name for logging. */
    protected abstract String getBrokerName();

    // ─────────────────────────────────────────────────────────────────────
    // Hooks for subclasses (default no-op)
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Called after the socket opens and the connect payload has been sent.
     * Subclasses can start keepalive timers, etc. Default: no-op.
     */
    protected void onConnected() {
        // no-op by default
    }

    /**
     * Called when the socket closes or errors (before any reconnect).
     * Subclasses can stop keepalive timers, etc. Default: no-op.
     */
    protected void onDisconnected() {
        // no-op by default
    }

    /**
     * Send a text frame on the current WebSocket. Safe to call from subclass
     * keepalive/subscribe logic. No-op if the socket isn't connected.
     */
    protected void sendMessage(String text) {
        WebSocket ws = webSocket;
        if (ws == null || text == null || text.isEmpty()) return;
        try {
            ws.sendText(text, true);
        } catch (Exception e) {
            log.debug("[{}WS] sendMessage failed: {}", getBrokerName(), e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────

    public void start() {
        if (!isAuthenticated()) {
            log.warn("[{}WS] Not authenticated — skipping connect", getBrokerName());
            return;
        }
        running.set(true);
        connect();
    }

    public void stop() {
        running.set(false);
        try {
            onDisconnected();
        } catch (Exception e) {
            log.debug("[{}WS] onDisconnected hook error: {}", getBrokerName(), e.getMessage());
        }
        if (webSocket != null) {
            try {
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown");
            } catch (Exception e) {
                log.debug("[{}WS] Error sending close: {}", getBrokerName(), e.getMessage());
            }
        }
        log.info("[{}WS] Stopped", getBrokerName());
    }

    public boolean isRunning() {
        return running.get() && webSocket != null;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Connect + listener
    // ─────────────────────────────────────────────────────────────────────

    private void connect() {
        if (!running.get()) return;
        try {
            log.info("[{}WS] Connecting to {}", getBrokerName(), wsUrl);
            WebSocket.Builder builder = httpClient.newWebSocketBuilder()
                    .connectTimeout(java.time.Duration.ofSeconds(3));
            customizeBuilder(builder);
            builder.buildAsync(URI.create(wsUrl), new WebSocketListener())
                    .thenAccept(ws -> {
                        webSocket = ws;
                        // Send connect/subscribe payload
                        String payload = buildConnectPayload();
                        if (payload != null && !payload.isEmpty()) {
                            ws.sendText(payload, true);
                            log.info("[{}WS] Sent connect payload", getBrokerName());
                        }
                        try {
                            onConnected();
                        } catch (Exception e) {
                            log.debug("[{}WS] onConnected hook error: {}", getBrokerName(), e.getMessage());
                        }
                    })
                    .exceptionally(ex -> {
                        log.error("[{}WS] Connect failed: {}", getBrokerName(), ex.getMessage());
                        scheduleReconnect();
                        return null;
                    });
        } catch (Exception e) {
            log.error("[{}WS] Connect exception: {}", getBrokerName(), e.getMessage(), e);
            scheduleReconnect();
        }
    }

    /**
     * Hook for subclasses to add headers (e.g. Authorization) to the WS builder.
     * Default implementation does nothing.
     */
    protected void customizeBuilder(WebSocket.Builder builder) {
        // no-op by default
    }

    private void scheduleReconnect() {
        if (!running.get()) return;
        log.info("[{}WS] Reconnecting in {}ms", getBrokerName(), reconnectDelayMs);
        try {
            Thread.sleep(reconnectDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        // Exponential backoff, capped
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, MAX_RECONNECT_DELAY_MS);
        connect();
    }

    private void resetBackoff() {
        reconnectDelayMs = 1000;
    }

    // ─────────────────────────────────────────────────────────────────────
    // WebSocket.Listener implementation
    // ─────────────────────────────────────────────────────────────────────

    private class WebSocketListener implements WebSocket.Listener {
        private final StringBuilder partialBuffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            log.info("[{}WS] Connection opened", getBrokerName());
            resetBackoff();
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partialBuffer.append(data);
            if (last) {
                String message = partialBuffer.toString();
                partialBuffer.setLength(0);
                try {
                    handleMessage(message);
                } catch (Exception e) {
                    log.error("[{}WS] Error handling message: {}", getBrokerName(), e.getMessage(), e);
                }
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
            webSocket.sendPong(message);
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.warn("[{}WS] Closed: code={} reason={}", getBrokerName(), statusCode, reason);
            notifyDisconnected();
            if (running.get()) {
                scheduleReconnect();
            }
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            log.error("[{}WS] Error: {}", getBrokerName(), error.getMessage(), error);
            notifyDisconnected();
            if (running.get()) {
                scheduleReconnect();
            }
        }

        private void notifyDisconnected() {
            try {
                onDisconnected();
            } catch (Exception e) {
                log.debug("[{}WS] onDisconnected hook error: {}", getBrokerName(), e.getMessage());
            }
        }
    }
}
