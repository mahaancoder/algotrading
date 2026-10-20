package com.satyam.trading2.broker;

import com.satyam.trading2.broker.model.BrokerOrderEvent;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.order.PendingOrderRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PendingOrderRecoveryServiceTest {

    @Mock
    private BrokerRouter brokerRouter;
    @Mock
    private PendingOrderRepository pendingOrderRepository;
    @Mock
    private BrokerOrderEventProcessor brokerOrderEventProcessor;
    @Mock
    private Broker broker;

    @InjectMocks
    private PendingOrderRecoveryService service;

    @Test
    void startupRecoveryReplaysTerminalPartialSellFill() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-123");
        pending.setBrokerOrderId("sell-123");
        pending.setBroker("KITE");
        pending.setExecutionRoute(ExecutionRoute.KITE);
        pending.setSymbol("RELIANCE");
        pending.setSide(TradeSide.SELL);
        pending.setPositionType(Position.PositionType.HOLDING);
        pending.setState(PendingOrderState.PARTIALLY_FILLED);
        pending.setRequestQty(10);
        pending.setFilledQty(2);
        pending.setAvgPrice(101.0);
        pending.setCreatedTime(System.currentTimeMillis() - 60_000L);

        when(pendingOrderRepository.findOrdersRequiringReconciliation()).thenReturn(List.of(pending));
        when(brokerRouter.getBroker(ExecutionRoute.KITE)).thenReturn(broker);
        when(broker.getBrokerName()).thenReturn("KITE");
        when(broker.getOrderStatus("sell-123")).thenReturn(BrokerOrderStatus.CANCELLED);
        when(broker.getOrderFilledQuantity("sell-123")).thenReturn(4);
        when(broker.getOrderAveragePrice("sell-123")).thenReturn(102.5);

        service.recoverAfterStartup();

        ArgumentCaptor<BrokerOrderEvent> event = ArgumentCaptor.forClass(BrokerOrderEvent.class);
        verify(brokerOrderEventProcessor).process(event.capture());
        assertEquals("KITE", event.getValue().getBroker());
        assertEquals("sell-123", event.getValue().getBrokerOrderId());
        assertEquals(BrokerOrderStatus.CANCELLED, event.getValue().getStatus());
        assertEquals(4, event.getValue().getFilledQuantity());
        assertEquals(102.5, event.getValue().getAveragePrice());
    }

    @Test
    void missingBrokerFillQuantityNeverDispatchesSyntheticZeroFill() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-456");
        pending.setBrokerOrderId("sell-456");
        pending.setBroker("KITE");
        pending.setExecutionRoute(ExecutionRoute.KITE);
        pending.setSymbol("TCS");
        pending.setSide(TradeSide.SELL);
        pending.setRequestQty(5);
        pending.setCreatedTime(System.currentTimeMillis() - 60_000L);

        when(pendingOrderRepository.findOrdersRequiringReconciliation()).thenReturn(Collections.singletonList(pending));
        when(brokerRouter.getBroker(ExecutionRoute.KITE)).thenReturn(broker);
        when(broker.getBrokerName()).thenReturn("KITE");
        when(broker.getOrderStatus("sell-456")).thenReturn(BrokerOrderStatus.CANCELLED);
        when(broker.getOrderFilledQuantity("sell-456")).thenReturn(null);

        service.recoverAfterStartup();

        verify(brokerOrderEventProcessor, never()).process(org.mockito.ArgumentMatchers.any());
    }
    @Test
    void startupRecoveryRetriesCompleteOrderWhoseCallbackWasNotProcessed() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-789");
        pending.setBrokerOrderId("sell-789");
        pending.setBroker("KITE");
        pending.setExecutionRoute(ExecutionRoute.KITE);
        pending.setSymbol("INFY");
        pending.setSide(TradeSide.SELL);
        pending.setPositionType(Position.PositionType.HOLDING);
        pending.setState(PendingOrderState.COMPLETE);
        pending.setRequestQty(5);
        pending.setFilledQty(5);
        pending.setAvgPrice(101.0);
        pending.setCompletionProcessed(false);
        pending.setCreatedTime(System.currentTimeMillis() - 60_000L);

        when(pendingOrderRepository.findOrdersRequiringReconciliation()).thenReturn(List.of(pending));
        when(brokerRouter.getBroker(ExecutionRoute.KITE)).thenReturn(broker);
        when(broker.getBrokerName()).thenReturn("KITE");
        when(broker.getOrderStatus("sell-789")).thenReturn(BrokerOrderStatus.COMPLETE);
        when(broker.getOrderFilledQuantity("sell-789")).thenReturn(5);
        when(broker.getOrderAveragePrice("sell-789")).thenReturn(102.0);

        service.recoverAfterStartup();

        ArgumentCaptor<BrokerOrderEvent> event = ArgumentCaptor.forClass(BrokerOrderEvent.class);
        verify(brokerOrderEventProcessor).process(event.capture());
        assertEquals(BrokerOrderStatus.COMPLETE, event.getValue().getStatus());
        assertEquals(5, event.getValue().getFilledQuantity());
        assertEquals(102.0, event.getValue().getAveragePrice());
    }

}
