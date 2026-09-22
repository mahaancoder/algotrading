package com.satyam.trading2.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

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

    // Connection-pooled, fast timeouts — same tuning as OrderServiceV2.
    protected final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500))
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    // ─────────────────────────────────────────────────────────────────────
    // POST helpers
    // ─────────────────────────────────────────────────────────────────────

    /** POST with a raw string body + custom Content-Type. Returns parsed JSON. */
    protected JsonNode postJson(String url, String body, String contentType, String authHeader)
            throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(3000))
                .POST(HttpRequest.BodyPublishers.ofString(body));

        if (authHeader != null && !authHeader.isEmpty()) {
            builder.header("Authorization", authHeader);
        }
        builder.header("Content-Type", contentType);

        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        return objectMapper.readTree(response.body());
    }

    /** POST a JSON body with application/json content type. */
    protected JsonNode postJsonBody(String url, String jsonBody, String authHeader) throws Exception {
        return postJson(url, jsonBody, "application/json", authHeader);
    }

    /** POST a form-urlencoded body. */
    protected JsonNode postForm(String url, String formBody) throws Exception {
        return postJson(url, formBody, "application/x-www-form-urlencoded", null);
    }

    // ─────────────────────────────────────────────────────────────────────
    // GET helpers
    // ─────────────────────────────────────────────────────────────────────

    /** GET with optional auth header. Returns parsed JSON. */
    protected JsonNode getJson(String url, String authHeader) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(3000))
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
}
