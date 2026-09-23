package com.satyam.trading2.datamodel;

import com.satyam.trading2.broker.ExecutionRoute;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.UUID;

@Data
@RequiredArgsConstructor
public class PendingOrder {

    // ===== Identity =====
    // internalOrderId: our own UUID — unique across all brokers, used as the
    // repository map key. Never collides even if two brokers return the same id.
    private String internalOrderId;

    // orderId: the broker-assigned order id. Kept for backward compatibility
    // with existing callers; canonically the same value as brokerOrderId.
    // New code should prefer brokerOrderId + broker for lookups.
    private String orderId;

    // brokerOrderId: broker-assigned order id (canonical). Unique only within
    // that broker — always pair with `broker` for a globally unique key.
    private String brokerOrderId;

    // ===== Broker dimension (added in multi-broker refactor) =====
    private String broker;               // "KITE", "INDSTOCKS", "SHOONYA"
    private ExecutionRoute executionRoute;
    private String signalId;             // TradeSignal id for end-to-end tracing

    // ===== Order details =====
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

    // ─────────────────────────────────────────────────────────────────────
    // Backward-compat: keep orderId and brokerOrderId in sync.
    // Existing callers use getOrderId()/setOrderId(); the broker dimension is
    // new. Setting either field mirrors to the other so both old and new code
    // see the same value.
    // ─────────────────────────────────────────────────────────────────────
    public void setOrderId(String orderId) {
        this.orderId = orderId;
        if (this.brokerOrderId == null) {
            this.brokerOrderId = orderId;
        }
    }

    public void setBrokerOrderId(String brokerOrderId) {
        this.brokerOrderId = brokerOrderId;
        if (this.orderId == null) {
            this.orderId = brokerOrderId;
        }
    }

    /** Generate a fresh internal order id (UUID) for a new PendingOrder. */
    public static String generateInternalOrderId() {
        return UUID.randomUUID().toString();
    }
}