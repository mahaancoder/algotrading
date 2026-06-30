package com.satyam.trading2.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.satyam.trading2.config.KiteConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * GTT (Good Till Triggered) Order Service
 * ─────────────────────────────────────────────────────────────────────────
 * GTT orders stay active for up to 1 YEAR on Kite.
 * Perfect for positional trades — set stop and target once, forget about it.
 *
 * TWO-LEG GTT (OCO — One Cancels Other):
 *   If price hits stop-loss → sell at market (leg 1 triggers)
 *   If price hits target    → sell at limit  (leg 2 triggers)
 *   When one triggers, the other is automatically cancelled.
 *
 * USE CASE:
 *   You convert an intraday MIS position to positional CNC.
 *   Instead of watching the screen 24/7, you place a GTT:
 *   - Stop: ₹2,720  (below weekly support)
 *   - Target: ₹3,100 (next major resistance)
 *   Bot or price action will trigger whichever comes first.
 *   Your laptop doesn't need to be on!
 *
 * API: POST /gtt/triggers
 */
@Service
public class GttService {

    private static final Logger log = LoggerFactory.getLogger(GttService.class);

    private final KiteConfig kiteConfig;
    private final HttpClient   http    = HttpClient.newHttpClient();
    private final ObjectMapper mapper  = new ObjectMapper();

    public GttService(KiteConfig kiteConfig) {
        this.kiteConfig = kiteConfig;
    }

    /**
     * Places a Two-Leg GTT (OCO) order:
     *   Leg 1: SELL at market if price drops to stopPrice (stop-loss protection)
     *   Leg 2: SELL at limit if price rises to targetPrice (take profit)
     *
     * Kite automatically cancels the other leg when one triggers.
     *
     * @param tradingSymbol  e.g., "RELIANCE"
     * @param exchange       e.g., "NSE"
     * @param quantity       shares held
     * @param lastPrice      current LTP (Kite needs this as reference)
     * @param stopPrice      stop-loss trigger price
     * @param targetPrice    target trigger price
     * @return gtt_id if placed, null if failed
     */
    public String placeTwoLegGtt(String tradingSymbol, String exchange, int quantity,
                                  double lastPrice, double stopPrice, double targetPrice) {
        try {
            // GTT uses JSON body unlike order API which uses form-encoded
            String body = String.format(
                "{\n" +
                "  \"trigger_type\": \"two-leg\",\n" +
                "  \"tradingsymbol\": \"%s\",\n" +
                "  \"exchange\": \"%s\",\n" +
                "  \"trigger_values\": [%.2f, %.2f],\n" +
                "  \"last_price\": %.2f,\n" +
                "  \"orders\": [\n" +
                "    {\n" +
                "      \"transaction_type\": \"SELL\",\n" +
                "      \"quantity\": %d,\n" +
                "      \"order_type\": \"LIMIT\",\n" +
                "      \"product\": \"CNC\",\n" +
                "      \"price\": %.2f\n" +
                "    },\n" +
                "    {\n" +
                "      \"transaction_type\": \"SELL\",\n" +
                "      \"quantity\": %d,\n" +
                "      \"order_type\": \"LIMIT\",\n" +
                "      \"product\": \"CNC\",\n" +
                "      \"price\": %.2f\n" +
                "    }\n" +
                "  ]\n" +
                "}",
                tradingSymbol, exchange,
                stopPrice, targetPrice,
                lastPrice,
                quantity, stopPrice * 0.99,   // stop leg: sell slightly below trigger
                quantity, targetPrice          // target leg: sell at target
            );

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/gtt/triggers"))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            JsonNode json = mapper.readTree(resp.body());

            if ("success".equals(json.get("status").asText())) {
                String gttId = json.get("data").get("trigger_id").asText();
                log.info("[GTT] Two-leg GTT placed: {} | SL=₹{} Target=₹{} | GTT_ID={}",
                        tradingSymbol, stopPrice, targetPrice, gttId);
                return gttId;
            } else {
                log.error("[GTT] Failed: {}", json.get("message") != null
                        ? json.get("message").asText() : resp.body());
            }
        } catch (Exception e) {
            log.error("[GTT] Exception: {}", e.getMessage(), e);
        }
        return null;
    }

    /**
     * Places a single-leg GTT stop-loss (simpler — just a stop, no target leg).
     * Use when you want to only protect downside and trail target manually.
     */
    public String placeSingleLegStopGtt(String tradingSymbol, String exchange, int quantity,
                                          double lastPrice, double stopPrice) {
        try {
            String body = String.format(
                "{\n" +
                "  \"trigger_type\": \"single\",\n" +
                "  \"tradingsymbol\": \"%s\",\n" +
                "  \"exchange\": \"%s\",\n" +
                "  \"trigger_values\": [%.2f],\n" +
                "  \"last_price\": %.2f,\n" +
                "  \"orders\": [{\n" +
                "    \"transaction_type\": \"SELL\",\n" +
                "    \"quantity\": %d,\n" +
                "    \"order_type\": \"MARKET\",\n" +
                "    \"product\": \"CNC\",\n" +
                "    \"price\": %.2f\n" +
                "  }]\n" +
                "}",
                tradingSymbol, exchange, stopPrice, lastPrice, quantity, stopPrice * 0.995
            );

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/gtt/triggers"))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            JsonNode json = mapper.readTree(resp.body());

            if ("success".equals(json.get("status").asText())) {
                String gttId = json.get("data").get("trigger_id").asText();
                log.info("[GTT] Single-leg stop GTT placed: {} | SL=₹{} | GTT_ID={}",
                        tradingSymbol, stopPrice, gttId);
                return gttId;
            }
        } catch (Exception e) {
            log.error("[GTT] Single-leg exception: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Deletes a GTT order (call when converting back to intraday or exiting manually).
     */
    public boolean deleteGtt(String gttId) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/gtt/triggers/" + gttId))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .DELETE().build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            JsonNode json = mapper.readTree(resp.body());
            boolean ok = "success".equals(json.get("status").asText());
            if (ok) log.info("[GTT] Deleted GTT: {}", gttId);
            return ok;
        } catch (Exception e) {
            log.error("[GTT] Delete failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Retrieves all active GTT orders for monitoring.
     */
    public JsonNode getAllGtts() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/gtt/triggers"))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            return mapper.readTree(resp.body()).get("data");
        } catch (Exception e) {
            log.error("[GTT] List failed: {}", e.getMessage());
            return null;
        }
    }
}
