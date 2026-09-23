package com.satyam.trading2.config;

import lombok.Data;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * INDstocks API configuration.
 * Only created when broker.indstocks.enabled=true.
 *
 * Auth: access_token in the Authorization header (no "Bearer" prefix).
 * Token is generated via dashboard or POST /generate/token (TOTP).
 * Static IP whitelisting is required for order placement endpoints.
 *
 * REST base: https://api.indstocks.com
 */
@Component
@ConditionalOnProperty(name = "broker.indstocks.enabled", havingValue = "true")
@Data
public class IndstocksConfig {

    @Value("${broker.indstocks.base.url:https://api.indstocks.com}")
    private String baseUrl;

    @Value("${broker.indstocks.ws.order.url:wss://ws-order-updates.indstocks.com/api/v1/ws/trades}")
    private String wsOrderUrl;

    @Value("${broker.indstocks.algo.id.nse:99999}")
    private String algoIdNse; // "99999" for NSE per docs

    @Value("${broker.indstocks.algo.id.bse:9999999999999999}")
    private String algoIdBse; // "9999999999999999" for BSE per docs

    // For TOTP-based token generation (optional — if not using dashboard tokens)
    @Value("${broker.indstocks.api.key:}")
    private String apiKey; // x-api-key (Client ID) for /generate/token

    @Value("${broker.indstocks.mpin:}")
    private String mpin;

    @Value("${broker.indstocks.totp.secret:}")
    private String totpSecret; // base32 secret for TOTP generation

    // Set at runtime after token generation or dashboard paste — not in properties.
    @Setter
    private String accessToken;

    public boolean isAuthenticated() {
        return accessToken != null && !accessToken.isEmpty();
    }

    /** Auth header value: just the token (INDstocks has no "Bearer" prefix). */
    public String getAuthHeader() {
        return accessToken;
    }
}
