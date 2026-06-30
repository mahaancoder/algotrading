package com.satyam.trading2.controller;

import com.satyam.trading2.config.KiteAuthService;
import com.satyam.trading2.service.TradingOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class AuthController {

    @Autowired
    private TradingOrchestrator tradingOrchestrator;
    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    private final KiteAuthService authService;

    public AuthController(KiteAuthService authService) {
        this.authService = authService;
    }

    @GetMapping("/auth/login")
    public String login() {
        return "redirect:" + authService.getLoginUrl();
    }

    @GetMapping("/auth/callback")
    public String callback(
            @RequestParam(value = "request_token", required = false) String requestToken,
            @RequestParam(value = "status", required = false) String status,
            Model model) {

        if ("success".equals(status) && requestToken != null) {
            boolean success = authService.exchangeToken(requestToken);
            model.addAttribute("message",
                    success ? "Login successful! Bot is now active." : "Token exchange failed. Please try again.");
            model.addAttribute("success", success);
            tradingOrchestrator.start(); // 🔥 START HERE
        } else {
            model.addAttribute("message", "Login failed or was cancelled.");
            model.addAttribute("success", false);
        }
        return "redirect:/";
    }

    @GetMapping("/auth/logout")
    public String logout() {
        authService.logout();
        return "redirect:/";
    }
}
