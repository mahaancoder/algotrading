package com.satyam.trading2.config;

import lombok.Data;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Holds Kite API configuration and the current session's access_token.
 * access_token is set after successful OAuth login and reused for all API calls.
 */
@Component
@Data
public class KiteConfig {

    @Value("${kite.api.key}")
    private String apiKey;

    @Value("${kite.api.secret}")
    private String apiSecret;

    @Value("${kite.base.url}")
    private String baseUrl;

    @Value("${kite.redirect.url}")
    private String redirectUrl;

    // Set at runtime after OAuth login — not stored in properties for security
    @Setter
    private String accessToken;

    public boolean isAuthenticated() {
        return accessToken != null && !accessToken.isEmpty();
    }
    /**
     * Returns the Authorization header value required by every Kite API call.
     * Format: "token api_key:access_token"
     */
    public String getAuthHeader() {
        return "token " + apiKey + ":" + accessToken;
    }
}
