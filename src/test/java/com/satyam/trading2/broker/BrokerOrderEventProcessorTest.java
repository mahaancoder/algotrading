package com.satyam.trading2.broker;

import com.satyam.trading2.broker.model.BrokerOrderEvent;
import com.satyam.trading2.broker.model.BrokerOrderStatus;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.order.OrderCompletionProcessor;
import com.satyam.trading2.order.PendingOrderRepository;
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
class BrokerOrderEventProcessorTest {

    @Mock
    private OrderCompletionProcessor orderCompletionProcessor;
    @Mock
    private PendingOrderRepository pendingOrderRepository;
    @Mock
    private RiskManager riskManager;
    @Mock
    private DipAccumulatorService dipAccumulatorService;

    @InjectMocks
    private BrokerOrderEventProcessor processor;

    @Test
    void completedSellThatWasNotAppliedToPositionKeepsReservation() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-787");
        pending.setBrokerOrderId("sell-787");
        pending.setBroker("KITE");
        pending.setSymbol("INFY");
        pending.setStrategy("strategy-a");
        pending.setPositionType(Position.PositionType.INTRADAY);
        pending.setSide(TradeSide.SELL);
        pending.setState(PendingOrderState.PENDING);
        pending.setRequestQty(3);
        pending.setAvgPrice(1500.0);

        when(pendingOrderRepository.findByBrokerAndBrokerOrderId("KITE", "sell-787"))
                .thenReturn(Optional.of(pending));
        // The production bean uses @Lazy field injection to break a Spring cycle;
        // Mockito's @InjectMocks does not always populate that lazy field.
        org.springframework.test.util.ReflectionTestUtils.setField(
                processor, "orderCompletionProcessor", orderCompletionProcessor);

        processor.process(BrokerOrderEvent.builder()
                .broker("KITE")
                .brokerOrderId("sell-787")
                .status(BrokerOrderStatus.COMPLETE)
                .filledQuantity(3)
                .averagePrice(1501.0)
                .build());

        assertEquals(PendingOrderState.PENDING, pending.getState(),
                "A callback that did not update the position must not release the SELL reservation");
        verify(pendingOrderRepository, org.mockito.Mockito.times(2)).save(pending);
    }

    @Test
    void cancelledSellWithPartialFillsRemainsReservedUntilReconciliation() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-789");
        pending.setBrokerOrderId("sell-789");
        pending.setBroker("KITE");
        pending.setSymbol("RELIANCE");
        pending.setStrategy("strategy-a");
        pending.setPositionType(Position.PositionType.INTRADAY);
        pending.setSide(TradeSide.SELL);
        pending.setState(PendingOrderState.PENDING);
        pending.setRequestQty(10);
        pending.setAvgPrice(100.0);

        when(pendingOrderRepository.findByBrokerAndBrokerOrderId("KITE", "sell-789"))
                .thenReturn(Optional.of(pending));
        org.springframework.test.util.ReflectionTestUtils.setField(
                processor, "orderCompletionProcessor", orderCompletionProcessor);

        processor.process(BrokerOrderEvent.builder()
                .broker("KITE")
                .brokerOrderId("sell-789")
                .status(BrokerOrderStatus.CANCELLED)
                .filledQuantity(4)
                .averagePrice(101.0)
                .build());

        assertEquals(PendingOrderState.PARTIALLY_FILLED, pending.getState());
        assertEquals(4, pending.getFilledQty(),
                "A terminal event must persist cumulative fills even if no partial event arrived first");
        verify(pendingOrderRepository).save(pending);
        verify(orderCompletionProcessor).onSellCompleted(pending);
    }

    @Test
    void rejectedSellWithCumulativePartialFillPersistsFillAndRetainsReservation() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-791");
        pending.setBrokerOrderId("sell-791");
        pending.setBroker("KITE");
        pending.setSymbol("INFY");
        pending.setStrategy("strategy-a");
        pending.setPositionType(Position.PositionType.INTRADAY);
        pending.setSide(TradeSide.SELL);
        pending.setState(PendingOrderState.PENDING);
        pending.setRequestQty(10);
        pending.setAvgPrice(100.0);

        when(pendingOrderRepository.findByBrokerAndBrokerOrderId("KITE", "sell-791"))
                .thenReturn(Optional.of(pending));
        org.springframework.test.util.ReflectionTestUtils.setField(
                processor, "orderCompletionProcessor", orderCompletionProcessor);

        processor.process(BrokerOrderEvent.builder()
                .broker("KITE")
                .brokerOrderId("sell-791")
                .status(BrokerOrderStatus.REJECTED)
                .filledQuantity(3)
                .averagePrice(101.0)
                .build());

        assertEquals(PendingOrderState.PARTIALLY_FILLED, pending.getState());
        assertEquals(3, pending.getFilledQty());
        assertEquals(101.0, pending.getAvgPrice());
        verify(pendingOrderRepository).save(pending);
        verify(orderCompletionProcessor).onSellCompleted(pending);
    }

    @Test
    void cancelledSellWithUnknownFillsRemainsReservedUntilReconciliation() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-788");
        pending.setBrokerOrderId("sell-788");
        pending.setBroker("SHOONYA");
        pending.setSymbol("INFY");
        pending.setStrategy("strategy-b");
        pending.setPositionType(Position.PositionType.INTRADAY);
        pending.setSide(TradeSide.SELL);
        pending.setState(PendingOrderState.PENDING);
        pending.setRequestQty(5);
        pending.setAvgPrice(1500.0);

        when(pendingOrderRepository.findByBrokerAndBrokerOrderId("SHOONYA", "sell-788"))
                .thenReturn(Optional.of(pending));

        processor.process(BrokerOrderEvent.builder()
                .broker("SHOONYA")
                .brokerOrderId("sell-788")
                .status(BrokerOrderStatus.CANCELLED)
                .filledQuantity(null)
                .build());

        assertEquals(PendingOrderState.PENDING, pending.getState());
        verify(pendingOrderRepository, never()).save(any(PendingOrder.class));
    }

    @Test
    void cancelledSellWithZeroFillsCanBeRetired() throws Exception {
        PendingOrder pending = new PendingOrder();
        pending.setOrderId("sell-790");
        pending.setBrokerOrderId("sell-790");
        pending.setBroker("KITE");
        pending.setSymbol("TCS");
        pending.setStrategy("strategy-a");
        pending.setPositionType(Position.PositionType.INTRADAY);
        pending.setSide(TradeSide.SELL);
        pending.setState(PendingOrderState.PENDING);
        pending.setRequestQty(10);
        pending.setAvgPrice(100.0);

        when(pendingOrderRepository.findByBrokerAndBrokerOrderId("KITE", "sell-790"))
                .thenReturn(Optional.of(pending));

        processor.process(BrokerOrderEvent.builder()
                .broker("KITE")
                .brokerOrderId("sell-790")
                .status(BrokerOrderStatus.CANCELLED)
                .filledQuantity(0)
                .build());

        assertEquals(PendingOrderState.CANCELLED, pending.getState());
        verify(pendingOrderRepository).save(pending);
    }
}
