package com.satyam.trading2.domain.service;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.service.OrderServiceV2;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static com.satyam.trading2.datamodel.TradeSignal.SignalType.EXIT_LONG;

@Slf4j
@Service
@RequiredArgsConstructor
public class HoldingService {

    private final PositionManager positionManager;
    private final OrderExecutor orderExecutor;
    private final OrderServiceV2 orderServiceV2;
    private final PendingOrderRepository pendingOrderRepository;



}

