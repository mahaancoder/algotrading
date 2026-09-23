package com.satyam.trading2.config;

import lombok.Data;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Shoonya (Finvasia) API configuration.
 * Only created when broker.shoonya.enabled=true.
 *
 * Auth: OAuth flow — the access token (jKey) is obtained via the OAuth authorize
 * redirect + checksum exchange, then set at runtime. See Shoonya "Manual Login (OAuth)".
 *
 * REST base: https://api.shoonya.com/NorenWClientAPI/
 * WS base:   wss://api.shoonya.com/NorenWSAPI/
 */
@Component
@ConditionalOnProperty(name = "broker.shoonya.enabled", havingValue = "true")
@Data
public class ShoonyaConfig {

    @Value("${broker.shoonya.base.url:https://api.shoonya.com/NorenWClientAPI/}")
    private String baseUrl;

    @Value("${broker.shoonya.ws.url:wss://api.shoonya.com/NorenWSAPI/}")
    private String wsUrl;

    @Value("${broker.shoonya.user.id}")
    private String userId;

    @Value("${broker.shoonya.account.id:}")
    private String accountId; // defaults to userId if not set

    @Value("${broker.shoonya.client.id:}")
    private String clientId; // OAuth client_id

    @Value("${broker.shoonya.secret.code:}")
    private String secretCode; // OAuth secret_code

    // Set at runtime after OAuth login — not stored in properties for security.
    @Setter
    private String accessToken; // jKey

    public boolean isAuthenticated() {
        return accessToken != null && !accessToken.isEmpty();
    }

    /** Account ID defaults to user ID per Shoonya convention. */
    public String getActid() {
        return accountId != null && !accountId.isEmpty() ? accountId : userId;
    }
}
