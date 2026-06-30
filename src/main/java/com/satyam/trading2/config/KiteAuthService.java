package com.satyam.trading2.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Handles Kite Connect authentication:
 * 1. Generates the Kite login URL
 * 2. Receives request_token from redirect
 * 3. Exchanges it for access_token (with SHA-256 checksum)
 */
@Service
public class KiteAuthService {

    private static final Logger log = LoggerFactory.getLogger(KiteAuthService.class);

    private final KiteConfig kiteConfig;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public KiteAuthService(KiteConfig kiteConfig) {
        this.kiteConfig   = kiteConfig;
        this.httpClient   = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Returns the Kite login URL to redirect the user to.
     * After login, Kite redirects back to your redirect_url with ?request_token=xxx
     */
    public String getLoginUrl() {
        return "https://kite.zerodha.com/connect/login?v=3&api_key=" + kiteConfig.getApiKey();
    }

    /**
     * Exchanges the one-time request_token for a persistent access_token.
     *
     * Kite requires a SHA-256 checksum = SHA256(api_key + request_token + api_secret)
     * to prevent replay attacks.
     *
     * @param requestToken  The one-time token received from Kite's redirect callback
     * @return true if token exchange was successful
     */
    public boolean exchangeToken(String requestToken) {
        try {
            String checksum = sha256(kiteConfig.getApiKey() + requestToken + kiteConfig.getApiSecret());

            // Build form body (Kite API uses form-encoded POST)
            String body = "api_key=" + kiteConfig.getApiKey()
                    + "&request_token=" + requestToken
                    + "&checksum=" + checksum;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/session/token"))
                    .header("X-Kite-Version", "3")
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            log.debug("Token exchange response: {}", response.body());

            JsonNode json = objectMapper.readTree(response.body());

            if ("success".equals(json.get("status").asText())) {
                String accessToken = json.get("data").get("access_token").asText();
                kiteConfig.setAccessToken(accessToken);
                log.info("✅ Authentication successful! Access token obtained.");
                return true;
            } else {
                log.error("❌ Token exchange failed: {}", response.body());
                return false;
            }

        } catch (Exception e) {
            log.error("❌ Exception during token exchange: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Logout and invalidate the access token.
     */
    public void logout() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl()
                            + "/session/token?api_key=" + kiteConfig.getApiKey()
                            + "&access_token=" + kiteConfig.getAccessToken()))
                    .header("X-Kite-Version", "3")
                    .DELETE()
                    .build();

            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            kiteConfig.setAccessToken(null);
            log.info("Logged out successfully.");

        } catch (Exception e) {
            log.error("Logout error: {}", e.getMessage());
        }
    }

    /**
     * Computes SHA-256 hash and returns as lowercase hex string.
     * Used to create the checksum for token exchange.
     */
    private String sha256(String input) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
