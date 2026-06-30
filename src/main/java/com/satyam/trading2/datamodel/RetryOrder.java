package com.satyam.trading2.datamodel;

import lombok.Data;

import java.time.Instant;

@Data
public class RetryOrder {

    private OrderRequest orderRequest;

    private int retryCount;

    private Instant nextRetryTime;

    private String lastError;

    private Instant createdAt;
}