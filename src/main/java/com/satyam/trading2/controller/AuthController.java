package com.satyam.trading2.controller;

import com.satyam.trading2.broker.IndstocksBroker;
import com.satyam.trading2.broker.ShoonyaBroker;
import com.satyam.trading2.broker.auth.IndstocksInstrumentsService;
import com.satyam.trading2.broker.auth.IndstocksTokenService;
import com.satyam.trading2.broker.auth.ShoonyaOAuthService;
import com.satyam.trading2.config.IndstocksConfig;
import com.satyam.trading2.config.KiteAuthService;
import com.satyam.trading2.config.KiteConfig;
import com.satyam.trading2.config.ShoonyaConfig;
import com.satyam.trading2.service.TradingOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Controller
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    private final KiteAuthService authService;
    private final KiteConfig kiteConfig;

    // Conditional beans — only present when broker.*.enabled=true
    @Autowired
    private Optional<ShoonyaOAuthService> shoonyaOAuthService;
    @Autowired
    private Optional<IndstocksTokenService> indstocksTokenService;
    @Autowired
    private Optional<IndstocksInstrumentsService> indstocksInstrumentsService;
    @Autowired
    private Optional<ShoonyaBroker> shoonyaBroker;
    @Autowired
    private Optional<IndstocksBroker> indstocksBroker;
    @Autowired
    private Optional<ShoonyaConfig> shoonyaConfig;
    @Autowired
    private Optional<IndstocksConfig> indstocksConfig;

    @Autowired
    private TradingOrchestrator tradingOrchestrator;

    // Runs the heavy TradingOrchestrator.start() off the HTTP request thread so
    // the Kite OAuth callback can redirect to the dashboard immediately instead
    // of blocking until startup (instrument fetch, holdings sync, ticker) finishes.
    private final ExecutorService startupExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("orchestrator-startup");
        t.setDaemon(true);
        return t;
    });

    public AuthController(KiteAuthService authService, KiteConfig kiteConfig) {
        this.authService = authService;
        this.kiteConfig = kiteConfig;
    }

    // ─────────────────────────────────────────────────────────────────────
    // LOGIN PAGE — shows all brokers with their auth status
    // ─────────────────────────────────────────────────────────────────────

    @GetMapping("/auth/login")
    public String loginPage() {
        return "login";
    }

    // ─────────────────────────────────────────────────────────────────────
    // KITE — redirect to Kite OAuth, then callback
    // ─────────────────────────────────────────────────────────────────────

    @GetMapping("/auth/kite")
    public String kiteLogin() {
        return "redirect:" + authService.getLoginUrl();
    }

    @GetMapping("/auth/callback")
    public String kiteCallback(
            @RequestParam(value = "request_token", required = false) String requestToken,
            @RequestParam(value = "status", required = false) String status,
            Map<String, Object> model) {

        if ("success".equals(status) && requestToken != null) {
            boolean success = authService.exchangeToken(requestToken);
            if (success) {
                // Start trading asynchronously so the redirect happens right away.
                // The access token is already stored by exchangeToken() above, so
                // there is no need to block the callback on the full startup.
                startupExecutor.submit(() -> {
                    try {
                        tradingOrchestrator.start();
                    } catch (Exception e) {
                        log.error("[AuthController] TradingOrchestrator.start() failed: {}", e.getMessage(), e);
                    }
                });
                return "redirect:/auth/login?kite=success";
            }
        }
        return "redirect:/auth/login?kite=failed";
    }

    @PostMapping("/auth/kite/logout")
    public String kiteLogout() {
        authService.logout();
        return "redirect:/auth/login";
    }

    // ─────────────────────────────────────────────────────────────────────
    // SHOONYA — redirect to OAuth authorize URL, then callback with code
    // ─────────────────────────────────────────────────────────────────────

    @GetMapping("/auth/shoonya")
    public String shoonyaLogin() {
        if (shoonyaOAuthService.isEmpty()) {
            log.warn("[AuthController] Shoonya not enabled — cannot login");
            return "redirect:/auth/login?shoonya=disabled";
        }
        // Redirect to Shoonya OAuth authorize URL.
        // After login, Shoonya redirects back to /auth/login?code=<auth_code>
        String url = shoonyaOAuthService.get().getAuthorizeUrl()
                + "&redirect_uri=" + java.net.URLEncoder.encode(
                        "http://localhost:8080/auth/login", java.nio.charset.StandardCharsets.UTF_8);
        return "redirect:" + url;
    }

    // Shoonya redirects the browser here (its registered redirect URI) with ?code=...
    // Perform the token exchange and redirect back to the login page so the user
    // lands on the login screen with the broker showing authenticated (like Kite),
    // instead of seeing raw JSON.
    @GetMapping("/auth/shoonya/callback")
    public String shoonyaCallback(@RequestParam("code") String authCode) {
        if (shoonyaOAuthService.isEmpty()) {
            log.warn("[AuthController] Shoonya not enabled — cannot exchange code");
            return "redirect:/auth/login?shoonya=disabled";
        }
        boolean success = shoonyaOAuthService.get().exchangeAuthCode(authCode);
        if (success) {
            // Start order updates WebSocket
            shoonyaBroker.ifPresent(b -> {
                try {
                    b.startOrderUpdates();
                    log.info("[AuthController] Shoonya order updates started");
                } catch (Exception e) {
                    log.error("[AuthController] Failed to start Shoonya order updates: {}", e.getMessage());
                }
            });
            return "redirect:/auth/login?shoonya=success";
        }
        String reason = shoonyaOAuthService.get().getLastError();
        String encoded = java.net.URLEncoder.encode(
                reason != null && !reason.isEmpty() ? reason : "Token exchange failed",
                java.nio.charset.StandardCharsets.UTF_8);
        return "redirect:/auth/login?shoonya=failed&reason=" + encoded;
    }

    @PostMapping("/auth/shoonya/logout")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> shoonyaLogout() {
        shoonyaBroker.ifPresent(b -> b.stopOrderUpdates());
        shoonyaConfig.ifPresent(c -> c.setAccessToken(null));
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        return ResponseEntity.ok(response);
    }

    // ─────────────────────────────────────────────────────────────────────
    // INDSTOCKS — TOTP auto-login (no redirect needed)
    // ─────────────────────────────────────────────────────────────────────

    @PostMapping("/auth/indstocks/token")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> indstocksToken() {
        Map<String, Object> response = new HashMap<>();
        if (indstocksTokenService.isEmpty()) {
            response.put("success", false);
            response.put("message", "INDstocks not enabled");
            return ResponseEntity.badRequest().body(response);
        }
        boolean success = indstocksTokenService.get().generateToken();
        if (success) {
            // Populate instruments master + start order updates
            indstocksInstrumentsService.ifPresent(svc -> {
                try {
                    int mapped = svc.populateInstruments();
                    log.info("[AuthController] INDstocks instruments loaded: {} symbols", mapped);
                } catch (Exception e) {
                    log.error("[AuthController] Failed to load INDstocks instruments: {}", e.getMessage());
                }
            });
            indstocksBroker.ifPresent(b -> {
                try {
                    b.startOrderUpdates();
                    log.info("[AuthController] INDstocks order updates started");
                } catch (Exception e) {
                    log.error("[AuthController] Failed to start INDstocks order updates: {}", e.getMessage());
                }
            });
            response.put("success", true);
            response.put("message", "INDstocks authenticated, instruments loaded, order updates started");
        } else {
            String reason = indstocksTokenService.get().getLastError();
            response.put("success", false);
            response.put("message", "TOTP token generation failed: "
                    + (reason != null && !reason.isEmpty() ? reason
                        : "check api.key, mpin, totp.secret in config"));
        }
        return ResponseEntity.ok(response);
    }

    @PostMapping("/auth/indstocks/logout")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> indstocksLogout() {
        indstocksBroker.ifPresent(b -> b.stopOrderUpdates());
        indstocksConfig.ifPresent(c -> c.setAccessToken(null));
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        return ResponseEntity.ok(response);
    }

    // ─────────────────────────────────────────────────────────────────────
    // AUTH STATUS — JSON endpoint for the login page to poll
    // ─────────────────────────────────────────────────────────────────────

    @GetMapping("/auth/status")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> authStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("kite", kiteConfig.isAuthenticated());
        status.put("shoonya", shoonyaConfig.map(ShoonyaConfig::isAuthenticated).orElse(false));
        status.put("shoonyaEnabled", shoonyaConfig.isPresent());
        status.put("indstocks", indstocksConfig.map(IndstocksConfig::isAuthenticated).orElse(false));
        status.put("indstocksEnabled", indstocksConfig.isPresent());
        return ResponseEntity.ok(status);
    }
}
