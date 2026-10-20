package com.satyam.trading2.order;

import com.satyam.trading2.datamodel.BrokerOrderStatus;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.service.DipAccumulatorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderLifecycleManagerTest {

    @Mock
    private OrderCompletionProcessor orderCompletionProcessor;
    @Mock
    private PendingOrderRepository pendingOrderRepository;
    @Mock
    private RiskManager riskManager;
    @Mock
    private DipAccumulatorService dipAccumulatorService;

    @InjectMocks
    private OrderLifecycleManager manager;

    @Test
    void cancelledSellWithUnknownFillQuantityKeepsReservationOpen() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-123");
        pending.setBrokerOrderId("sell-123");
        pending.setBroker("KITE");
        pending.setSymbol("RELIANCE");
        pending.setStrategy("strategy-a");
        pending.setPositionType(Position.PositionType.INTRADAY);
        pending.setSide(TradeSide.SELL);
        pending.setState(PendingOrderState.PENDING);
        pending.setRequestQty(10);
        pending.setAvgPrice(100.0);

        when(pendingOrderRepository.findByBrokerOrderId("sell-123"))
                .thenReturn(Optional.of(pending));

        manager.handle(com.satyam.trading2.datamodel.BrokerWebhookEvent.builder()
                .brokerOrderId("sell-123")
                .symbol("RELIANCE")
                .side(TradeSide.SELL)
                .status(BrokerOrderStatus.CANCELLED)
                .filledQuantity(null)
                .build());

        assertEquals(PendingOrderState.PENDING, pending.getState());
        verify(pendingOrderRepository, never()).save(any(PendingOrder.class));
    }

    @Test
    void cancelledSellWithExplicitZeroFillsCanReleaseReservation() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-456");
        pending.setBrokerOrderId("sell-456");
        pending.setBroker("KITE");
        pending.setSymbol("TCS");
        pending.setStrategy("strategy-a");
        pending.setPositionType(Position.PositionType.INTRADAY);
        pending.setSide(TradeSide.SELL);
        pending.setState(PendingOrderState.PENDING);
        pending.setRequestQty(10);
        pending.setAvgPrice(100.0);

        when(pendingOrderRepository.findByBrokerOrderId("sell-456"))
                .thenReturn(Optional.of(pending));

        manager.handle(com.satyam.trading2.datamodel.BrokerWebhookEvent.builder()
                .brokerOrderId("sell-456")
                .symbol("TCS")
                .side(TradeSide.SELL)
                .status(BrokerOrderStatus.CANCELLED)
                .filledQuantity(0)
                .build());

        assertEquals(PendingOrderState.CANCELLED, pending.getState());
        verify(pendingOrderRepository).save(pending);
    }
}
