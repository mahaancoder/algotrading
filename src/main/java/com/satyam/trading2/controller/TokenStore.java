package com.satyam.trading2.controller;

import org.springframework.stereotype.Component;

@Component
public class TokenStore {

    private String accessToken;

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
        System.out.println("✅ Access Token Stored: " + accessToken);
    }
}