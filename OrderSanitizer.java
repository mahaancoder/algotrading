package com.satyam.trading2.domain.service;

import com.satyam.trading2.datamodel.OrderRequest;

public interface OrderSanitizer {

    OrderRequest sanitize(OrderRequest request);
}