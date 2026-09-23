package com.satyam.trading2.datamodel;

import lombok.Data;


public enum BrokerOrderStatus {
    OPEN,
    PARTIALLY_FILLED,
    COMPLETE,
    CANCELLED,
    REJECTED
}
