package com.satyam.trading2.service;

public class OrderFailureClassifier {

    public static boolean isRetryable(String errorMessage) {

        if (errorMessage == null) return false;

        errorMessage = errorMessage.toLowerCase();

        return errorMessage.contains("max orders")
            || errorMessage.contains("rate limit")
            || errorMessage.contains("too many requests");
    }
}