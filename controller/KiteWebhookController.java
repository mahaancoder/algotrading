package com.satyam.trading2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyam.trading2.datamodel.KiteWebhookPayload;
import com.satyam.trading2.service.KiteWebhookService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/webhooks/kite")
@RequiredArgsConstructor
public class KiteWebhookController {

    private final ObjectMapper objectMapper;

    private final KiteWebhookService webhookService;

    @PostMapping("/orders")
    public ResponseEntity<Void> handleOrderWebhook(HttpServletRequest request) {
        try {
            String rawBody = request.getReader().lines().collect(Collectors.joining());
            KiteWebhookPayload payload = objectMapper.readValue(rawBody, KiteWebhookPayload.class);
//            System.out.println("webhook received : "+ payload.toString());
            webhookService.process(rawBody, payload);
            return ResponseEntity.ok().build();

        } catch (Exception e) {
            return ResponseEntity.ok().build();
        }
    }
}