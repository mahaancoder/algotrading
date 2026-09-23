package com.satyam.trading2.broker.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyam.trading2.config.ShoonyaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * Shoonya OAuth token exchange service.
 *
 * Shoonya uses a redirect-based OAuth flow:
 *   1. User visits the authorize URL in a browser:
 *      https://api.shoonya.com/OAuthlogin/authorize/oauth?client_id=<clientId>
 *   2. After login, Shoonya redirects back with ?code=<auth_code>
 *   3. Compute checksum = SHA256(clientId + secretCode + authCode)
 *   4. Call POST /NorenWClientAPI/GenAcsTok with { uid, code, appkey=checksum }
 *   5. Response contains access_token (the jKey used in all subsequent REST/WS calls)
 *
 * Steps 1-2 are interactive (browser login). This service handles step 3-5:
 * given the auth_code captured from the redirect, it exchanges it for the
 * access token and sets it on ShoonyaConfig.
 *
 * Only created when broker.shoonya.enabled=true.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "broker.shoonya.enabled", havingValue = "true")
public class ShoonyaOAuthService {

    private final ShoonyaConfig shoonyaConfig;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Human-readable reason for the last exchangeAuthCode() failure, surfaced to the UI.
    private volatile String lastError;

    /** @return the reason the last exchangeAuthCode() call failed, or null if none. */
    public String getLastError() {
        return lastError;
    }

    /**
     * Build the OAuth authorize URL the user should visit in a browser.
     */
    public String getAuthorizeUrl() {
        return "https://api.shoonya.com/OAuthlogin/authorize/oauth?client_id="
                + shoonyaConfig.getClientId();
    }

    /**
     * Exchange an OAuth auth_code (captured from the redirect) for an access token.
     * Sets the token on ShoonyaConfig on success.
     *
     * @param authCode the "code" query param from the OAuth redirect
     * @return true if the token was obtained and set successfully
     */
    public boolean exchangeAuthCode(String authCode) {
        lastError = null;
        if (authCode == null || authCode.isEmpty()) {
            lastError = "authCode is null/empty";
            log.warn("[ShoonyaOAuthService] authCode is null/empty — cannot exchange");
            return false;
        }
        if (shoonyaConfig.getClientId() == null || shoonyaConfig.getClientId().isEmpty()) {
            lastError = "No client.id configured";
            log.warn("[ShoonyaOAuthService] No client.id configured — cannot exchange");
            return false;
        }
        if (shoonyaConfig.getSecretCode() == null || shoonyaConfig.getSecretCode().isEmpty()) {
            lastError = "No secret.code configured";
            log.warn("[ShoonyaOAuthService] No secret.code configured — cannot exchange");
            return false;
        }

        try {
            // checksum = SHA256(clientId + secretCode + authCode)
            String checksumInput = shoonyaConfig.getClientId()
                    + shoonyaConfig.getSecretCode()
                    + authCode;
            String checksum = sha256Hex(checksumInput);

            // Finvasia's reference NorenRestApiOAuth.getAccessToken sends the
            // checksum under the "checksum" field (not "appkey"); using "appkey"
            // makes the server report "code or checksum is Missing".
            Map<String, String> payload = new HashMap<>();
            payload.put("uid", shoonyaConfig.getUserId());
            payload.put("code", authCode);
            payload.put("checksum", checksum);

            // Shoonya's NorenWClientAPI expects the raw JSON as the jData form value
            // (NOT URL-encoded). URL-encoding it makes the server reject it with
            // "Invalid Input : jData is not valid json object".
            String jData = objectMapper.writeValueAsString(payload);
            String body = "jData=" + jData;

            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                    .uri(URI.create(shoonyaConfig.getBaseUrl() + "GenAcsTok"))
                    .timeout(java.time.Duration.ofMillis(5000))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
                    .build();

            java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                    .connectTimeout(java.time.Duration.ofMillis(5000))
                    .build();

            java.net.http.HttpResponse<String> response =
                    client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
            JsonNode json = objectMapper.readTree(response.body());

            String stat = json.has("stat") ? json.get("stat").asText() : "";

            // The GenAcsTok success response is token-bearing and may not carry
            // stat=Ok; the token comes back as susertoken or access_token. Treat
            // any non-empty token as success regardless of stat.
            String accessToken = null;
            if (json.has("susertoken") && !json.get("susertoken").asText().isEmpty()) {
                accessToken = json.get("susertoken").asText();
            } else if (json.has("access_token") && !json.get("access_token").asText().isEmpty()) {
                accessToken = json.get("access_token").asText();
            }
            if (accessToken != null && !accessToken.isEmpty()) {
                shoonyaConfig.setAccessToken(accessToken);
                log.info("[ShoonyaOAuthService] Access token obtained successfully via OAuth");
                return true;
            }
            String emsg = json.has("emsg") ? json.get("emsg").asText() : "unknown";
            lastError = "stat=" + (stat.isEmpty() ? "?" : stat) + ", emsg=" + emsg;
            log.error("[ShoonyaOAuthService] Token exchange failed: stat={} emsg={}", stat, emsg);
            return false;
        } catch (Exception e) {
            lastError = e.getClass().getSimpleName()
                    + (e.getMessage() != null ? ": " + e.getMessage() : "");
            log.error("[ShoonyaOAuthService] Token exchange exception: {}", e.getMessage(), e);
            return false;
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 failed", e);
        }
    }
}
