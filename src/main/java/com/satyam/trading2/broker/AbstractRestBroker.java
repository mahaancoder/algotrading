package com.satyam.trading2.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyam.trading2.datamodel.Instrument;
import com.satyam.trading2.datamodel.Nifty500Stocks;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Shared HTTP plumbing for REST-based brokers.
 *
 * Both INDstocks (JSON body + Authorization header) and Shoonya
 * (x-www-form-urlencoded jData + jKey) use java.net.http.HttpClient.
 * This base class owns the HttpClient instance and provides thin helpers
 * for POST and GET, returning the parsed JsonNode.
 *
 * Subclasses build the request body / headers in their own format.
 */
@Slf4j
public abstract class AbstractRestBroker implements Broker {

    protected final ObjectMapper objectMapper = new ObjectMapper();

    // Default per-request timeout for the latency-sensitive order path.
    protected static final long DEFAULT_REQUEST_TIMEOUT_MS = 3000;
    // Longer per-request timeout for low-frequency polls (e.g. funds/limits),
    // whose pooled connection is usually idle/closed and must pay a fresh TLS
    // handshake within the same overall exchange budget.
    protected static final long SLOW_REQUEST_TIMEOUT_MS = 10000;

    // Connection-pooled HTTP client. The connect timeout is generous (5s) so a
    // cold TLS handshake — which happens on every low-frequency poll after the
    // pooled connection goes idle — doesn't get killed mid-handshake. Warm
    // requests are unaffected (connectTimeout only bounds new connection setup).
    protected final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(5000))
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    // ─────────────────────────────────────────────────────────────────────
    // POST helpers
    // ─────────────────────────────────────────────────────────────────────

    /** POST with a raw string body + custom Content-Type. Returns parsed JSON. */
    protected JsonNode postJson(String url, String body, String contentType, String authHeader)
            throws Exception {
        return postJson(url, body, contentType, authHeader, DEFAULT_REQUEST_TIMEOUT_MS);
    }

    /** POST with a raw string body + custom Content-Type and explicit request timeout. */
    protected JsonNode postJson(String url, String body, String contentType, String authHeader,
                                long requestTimeoutMs) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(requestTimeoutMs))
                .POST(HttpRequest.BodyPublishers.ofString(body));

        if (authHeader != null && !authHeader.isEmpty()) {
            builder.header("Authorization", authHeader);
        }
        builder.header("Content-Type", contentType);

        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        return objectMapper.readTree(response.body());
    }

    /** POST a JSON body with application/json content type. dfd */
    protected JsonNode postJsonBody(String url, String jsonBody, String authHeader) throws Exception {
        return postJson(url, jsonBody, "application/json", authHeader);
    }

    /** POST a form-urlencoded body. */
    protected JsonNode postForm(String url, String formBody) throws Exception {
        return postJson(url, formBody, "application/x-www-form-urlencoded", null);
    }

    /** POST a form-urlencoded body with an explicit request timeout. */
    protected JsonNode postForm(String url, String formBody, long requestTimeoutMs) throws Exception {
        return postJson(url, formBody, "application/x-www-form-urlencoded", null, requestTimeoutMs);
    }

    // ─────────────────────────────────────────────────────────────────────
    // GET helpers
    // ─────────────────────────────────────────────────────────────────────

    /** GET with optional auth header. Returns parsed JSON. */
    protected JsonNode getJson(String url, String authHeader) throws Exception {
        return getJson(url, authHeader, DEFAULT_REQUEST_TIMEOUT_MS);
    }

    /** GET with optional auth header and explicit request timeout. Returns parsed JSON. */
    protected JsonNode getJson(String url, String authHeader, long requestTimeoutMs) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(requestTimeoutMs))
                .GET();

        if (authHeader != null && !authHeader.isEmpty()) {
            builder.header("Authorization", authHeader);
        }

        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        return objectMapper.readTree(response.body());
    }

    // ─────────────────────────────────────────────────────────────────────
    // GET with JSON body (INDstocks /order and /order/trades use this odd pattern)
    // ─────────────────────────────────────────────────────────────────────
    protected JsonNode getWithBody(String url, String jsonBody, String authHeader) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(3000))
                .header("Content-Type", "application/json")
                .method("GET", HttpRequest.BodyPublishers.ofString(jsonBody));

        if (authHeader != null && !authHeader.isEmpty()) {
            builder.header("Authorization", authHeader);
        }

        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        return objectMapper.readTree(response.body());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Tick-size alignment
    //
    // The Kite path sanitizes every limit price to the instrument tick size
    // (DefaultOrderSanitizer: BUY floors, SELL ceils). REST brokers (Shoonya,
    // INDstocks) build the payload directly, so an un-snapped target price like
    // 123.4567 is rejected with "price is not a multiple of tick size". These
    // helpers reuse the same shared instrument master so alignment is identical.
    // ─────────────────────────────────────────────────────────────────────

    /** Instrument tick size for a Kite-style symbol; defaults to 0.05 when unknown. */
    protected static double tickSizeFor(String symbol) {
        Instrument inst = symbol != null ? Nifty500Stocks.Nifty500SymbolToInstrument.get(symbol) : null;
        double tick = (inst != null) ? inst.getTickSize() : 0;
        return tick > 0 ? tick : 0.05;
    }

    /**
     * Snap a limit price to the tick size, matching Kite: BUY floors down, SELL
     * ceils up. Returned as a clean 2-decimal string for the order payload.
     */
    protected String toTickPrice(String symbol, double price, boolean isBuy) {
        double tick = tickSizeFor(symbol);
        BigDecimal steps = BigDecimal.valueOf(price)
                .divide(BigDecimal.valueOf(tick), 0, isBuy ? RoundingMode.FLOOR : RoundingMode.CEILING);
        return steps.multiply(BigDecimal.valueOf(tick)).setScale(2, RoundingMode.HALF_UP).toPlainString();
    }

    /** Tick-aligned price as a double (for JSON numeric fields). */
    protected double toTickPriceValue(String symbol, double price, boolean isBuy) {
        return Double.parseDouble(toTickPrice(symbol, price, isBuy));
    }

    /** Snap a trigger price to the nearest tick multiple. */
    protected String toTickPriceNearest(String symbol, double price) {
        double tick = tickSizeFor(symbol);
        BigDecimal steps = BigDecimal.valueOf(price)
                .divide(BigDecimal.valueOf(tick), 0, RoundingMode.HALF_UP);
        return steps.multiply(BigDecimal.valueOf(tick)).setScale(2, RoundingMode.HALF_UP).toPlainString();
    }
}
