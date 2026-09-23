package com.satyam.trading2.broker.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyam.trading2.config.IndstocksConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * INDstocks token generation service.
 *
 * Calls POST /generate/token with x-api-key (Client ID) header + JSON body
 * containing mpin and the current TOTP code. On success, sets the access
 * token on IndstocksConfig so all subsequent REST/WS calls are authenticated.
 *
 * Per docs: only one TOTP-generated token is live at a time; each call
 * invalidates the previous. Token lasts 24 hours. So this should be called
 * once per session, not before every request.
 *
 * Only created when broker.indstocks.enabled=true.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "broker.indstocks.enabled", havingValue = "true")
public class IndstocksTokenService {

    private final IndstocksConfig indstocksConfig;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(1000))
            .build();

    /**
     * Generate an access token via TOTP and set it on the config.
     * @return true if token was generated successfully
     */
    public boolean generateToken() {
        if (indstocksConfig.getApiKey() == null || indstocksConfig.getApiKey().isEmpty()) {
            log.warn("[IndstocksTokenService] No api.key configured — skipping TOTP token generation. "
                    + "Set the token manually via the dashboard if needed.");
            return false;
        }
        if (indstocksConfig.getMpin() == null || indstocksConfig.getMpin().isEmpty()) {
            log.warn("[IndstocksTokenService] No mpin configured — skipping TOTP token generation");
            return false;
        }
        if (indstocksConfig.getTotpSecret() == null || indstocksConfig.getTotpSecret().isEmpty()) {
            log.warn("[IndstocksTokenService] No totp.secret configured — skipping TOTP token generation");
            return false;
        }

        try {
            String totp = TotpGenerator.generateCode(indstocksConfig.getTotpSecret());

            Map<String, String> payload = new HashMap<>();
            payload.put("mpin", indstocksConfig.getMpin());
            payload.put("totp", totp);
            String jsonBody = objectMapper.writeValueAsString(payload);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(indstocksConfig.getBaseUrl() + "/generate/token"))
                    .timeout(Duration.ofMillis(5000))
                    .header("x-api-key", indstocksConfig.getApiKey())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode json = objectMapper.readTree(response.body());
            String status = json.has("status") ? json.get("status").asText() : "";

            if ("success".equals(status)) {
                JsonNode data = json.get("data");
                String token = data != null && data.has("token") ? data.get("token").asText() : null;
                if (token != null && !token.isEmpty()) {
                    indstocksConfig.setAccessToken(token);
                    log.info("[IndstocksTokenService] Access token generated successfully via TOTP");
                    return true;
                }
            }
            log.error("[IndstocksTokenService] Token generation failed: status={} body={}",
                    status, response.body());
            return false;
        } catch (Exception e) {
            log.error("[IndstocksTokenService] Token generation exception: {}", e.getMessage(), e);
            return false;
        }
    }
}
