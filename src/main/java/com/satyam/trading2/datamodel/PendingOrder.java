package com.satyam.trading2.datamodel;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class PendingOrder {

    private String orderId;
    private String symbol;
    private String strategy;
    private PendingOrderState state;
    private TradeSide side;
    private long createdTime;
    private int requestQty;
    private double avgPrice;
    private Position.PositionType positionType;

    private long updatedTime;
    // broker data
    private int filledQty;
    private String rejectionReason;
    public boolean completionProcessed;

    // Analytics: Track how this order was generated (for BUY orders)
    private EntryType entryType;

    public void markCompletionProcessed() {
        if(completionProcessed) return;
        this.completionProcessed = true;
    }


    public void markCompleted(double avgPrice, int filledQty){
        if(this.state == PendingOrderState.COMPLETE) return;
        if(this.state == PendingOrderState.CANCELLED || this.state == PendingOrderState.REJECTED) throw new IllegalStateException("Cannot mark a cancelled or rejected order as completed");
        if(filledQty != this.requestQty) throw new IllegalStateException("Cannot mark a order as completed if not fully filled. Filled: " + filledQty + ", Requested: " + this.requestQty);

        this.avgPrice = avgPrice;
        this.filledQty = filledQty;
        this.state = PendingOrderState.COMPLETE;
        this.updatedTime = System.currentTimeMillis();
    }

    public boolean isCompleted(){return this.state == PendingOrderState.COMPLETE;}

    public boolean isOpen() {
        return this.state == PendingOrderState.PENDING
        || this.state == PendingOrderState.PARTIALLY_FILLED;}

    public boolean isClosed() {
        return (this.state == PendingOrderState.COMPLETE )|| (this.state == PendingOrderState.CANCELLED) || (this.state == PendingOrderState.REJECTED);
    }

    public void markRejected() {
       if(this.isClosed()) return;
       this.state = PendingOrderState.REJECTED;
       this.updatedTime = System.currentTimeMillis();
    }

    public void markCancelled() {
        if(this.isClosed()) return;
        this.state = PendingOrderState.CANCELLED;
        this.updatedTime = System.currentTimeMillis();
    }

    public void markPartiallyFilled(int filledQty, double avgPrice) {
        if(this.isClosed()) return;
        if ( filledQty >= this.requestQty || filledQty <= 0) {
            return;
        }
        this.filledQty = filledQty;
        this.avgPrice = avgPrice;
        this.state = PendingOrderState.PARTIALLY_FILLED;
        this.updatedTime = System.currentTimeMillis();
    }
}