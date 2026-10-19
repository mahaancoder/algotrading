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

        processor.process(BrokerOrderEvent.builder()
                .broker("KITE")
                .brokerOrderId("sell-789")
                .status(BrokerOrderStatus.CANCELLED)
                .filledQuantity(4)
                .averagePrice(101.0)
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
