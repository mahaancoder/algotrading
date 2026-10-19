package com.satyam.trading2.order;

import com.satyam.trading2.broker.BrokerRouter;
import com.satyam.trading2.broker.ExecutionRoute;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.service.OrderServiceV2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BrokerOrderRouterTest {

    @Mock
    private OrderServiceV2 orderServiceV2;
    @Mock
    private BrokerRouter brokerRouter;
    @Mock
    private PendingOrderRepository pendingOrderRepository;
    @Mock
    private PositionManager positionManager;

    @InjectMocks
    private BrokerOrderRouter router;

    private Position position;

    @BeforeEach
    void setUp() {
        position = new Position("RELIANCE", 100, 100.0, 101.0, true,
                "strategy-a", null, null, null, Position.PositionType.INTRADAY);
        position.setBroker("KITE");
        position.setExecutionRoute(ExecutionRoute.KITE);
        when(positionManager.getAllOpenPositions()).thenReturn(List.of(position));
        when(positionManager.getPositionsForSymbol("RELIANCE"))
                .thenReturn(Map.of("KITE::strategy-a", position));
        lenient().when(pendingOrderRepository.findOpenOrders()).thenReturn(Collections.emptyList());
    }

    @Test
    void rejectsSellQuantityGreaterThanConfirmedLongQuantity() {
        assertNull(router.placeLimitSell(position, "RELIANCE", 101.0, 101, false, "strategy-a"));
        verify(orderServiceV2, never()).placeRoutedSell(any(), any(), org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyDouble(), org.mockito.ArgumentMatchers.anyBoolean(), any());
    }

    @Test
    void rejectsSellThatWouldExceedLongQuantityAfterOtherReservations() {
        PendingOrder existingSell = new PendingOrder();
        existingSell.setOrderId("sell-60");
        existingSell.setBrokerOrderId("sell-60");
        existingSell.setBroker("KITE");
        existingSell.setSymbol("RELIANCE");
        existingSell.setSide(TradeSide.SELL);
        existingSell.setState(PendingOrderState.PENDING);
        existingSell.setRequestQty(60);
        existingSell.setFilledQty(0);
        when(pendingOrderRepository.findOpenOrders()).thenReturn(List.of(existingSell));

        assertNull(router.placeLimitSell(position, "RELIANCE", 101.0, 50, false, "strategy-a"));
        verify(orderServiceV2, never()).placeOrder(any(), any(), any(), org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyDouble(), org.mockito.ArgumentMatchers.anyDouble(), any(), any());
    }

    @Test
    void allowsSellWithinUnreservedLongQuantity() {
        when(orderServiceV2.placeRoutedSell(position, "RELIANCE", 100, 101.0, false,
                "strategy-a")).thenReturn("sell-100");

        assertEquals("sell-100",
                router.placeLimitSell(position, "RELIANCE", 101.0, 100, false, "strategy-a"));
        verify(orderServiceV2).placeRoutedSell(position, "RELIANCE", 100, 101.0, false,
                "strategy-a");
    }
    @Test
    void refusesAnotherSellWhenPositionReferencesUnresolvedTargetOrder() {
        position.setTargetOrderId("unknown-target");
        when(pendingOrderRepository.findByBrokerAndBrokerOrderId("KITE", "unknown-target"))
                .thenReturn(java.util.Optional.empty());

        assertNull(router.placeLimitSell(position, "RELIANCE", 101.0, 1, false, "strategy-a"));
        verify(orderServiceV2, never()).placeRoutedSell(any(), any(),
                org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyDouble(),
                org.mockito.ArgumentMatchers.anyBoolean(), any());
    }

    @Test
    void ignoresStaleReferenceToCancelledZeroFillSell() {
        position.setTargetOrderId("cancelled-target");
        PendingOrder cancelled = new PendingOrder();
        cancelled.setOrderId("cancelled-target");
        cancelled.setBroker("KITE");
        cancelled.setSymbol("RELIANCE");
        cancelled.setSide(TradeSide.SELL);
        cancelled.setState(PendingOrderState.CANCELLED);
        cancelled.setRequestQty(100);
        cancelled.setFilledQty(0);
        when(pendingOrderRepository.findByBrokerAndBrokerOrderId("KITE", "cancelled-target"))
                .thenReturn(java.util.Optional.of(cancelled));
        when(orderServiceV2.placeRoutedSell(position, "RELIANCE", 100, 101.0, false,
                "strategy-a")).thenReturn("sell-new");

        assertEquals("sell-new",
                router.placeLimitSell(position, "RELIANCE", 101.0, 100, false, "strategy-a"));
    }

}
