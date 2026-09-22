package com.satyam.trading2.datamodel;

import lombok.Data;

@Data
public class OrderDetails {

    private String orderId;

    private String status;

    private int filledQuantity;

    private double averagePrice;

    private String statusMessage;
}