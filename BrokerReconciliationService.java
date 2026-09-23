package com.satyam.trading2.broker;

import com.satyam.trading2.service.ReconciliationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.List;

/**
 * BrokerReconciliationService — periodic safety-net reconciliation across
 * all brokers. Replaces ReconciliationService as the scheduled entry point.
 *
 * Per the architecture: "REST reconciliation runs periodically as a safety net."
 * The primary order-status mechanism is the broker WebSocket
 * (BrokerOrderEventProcessor). This service catches anything missed by the
 * WebSocket: dropped events, missed fills, stale orders, position mismatches.
 *
 * WRAPPER STAGE (current): delegates the Kite reconciliation to the existing
 * ReconciliationService (which has the detailed syncBrokerPositions +
 * ensureTargetOrders logic). As INDstocks/Shoonya are added, their
 * broker.reconcileOrders() implementations will carry their own reconciliation.
 * The old ReconciliationService logic will eventually move into KiteBroker.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BrokerReconciliationService {

    private final BrokerRouter brokerRouter;
    private final ReconciliationService reconciliationService; // Kite legacy

    /**
     * Reconcile ALL registered brokers. Runs every 15 minutes during trading hours.
     */
    @Scheduled(fixedDelay = 900000)
    public void reconcileAll() {
        LocalTime now = LocalTime.now();
        if (now.isBefore(LocalTime.of(9, 20)) || now.isAfter(LocalTime.of(15, 0))) {
            return;
        }

        log.info("🔄 [BrokerReconciliationService] Starting reconciliation cycle for all brokers");

        List<Broker> brokers = brokerRouter.getAllBrokers();
        for (Broker broker : brokers) {
            try {
                reconcile(broker.getRoute());
            } catch (Exception e) {
                log.error("❌ [BrokerReconciliationService] Reconciliation failed for {}: {}",
                        broker.getBrokerName(), e.getMessage(), e);
            }
        }

        log.info("✅ [BrokerReconciliationService] Reconciliation cycle complete");
    }

    /**
     * Reconcile a single broker by its route.
     * For KITE: delegates to the existing ReconciliationService.
     * For INDSTOCKS/SHOONYA: calls broker.reconcileOrders().
     */
    public void reconcile(ExecutionRoute route) {
        Broker broker = brokerRouter.getBroker(route);
        if (broker == null) {
            log.warn("[BrokerReconciliationService] No broker for route {} — skipping", route);
            return;
        }

        log.info("🔄 [BrokerReconciliationService] Reconciling {} ({})", broker.getBrokerName(), route);

        if (route == ExecutionRoute.KITE) {
            // Delegate to existing ReconciliationService — full sync + target logic.
            reconciliationService.reconcile();
        } else {
            broker.reconcileOrders();
        }
    }

    /** Manual trigger for a specific broker (dashboard / admin). */
    public void reconcileBroker(ExecutionRoute route) {
        log.info("[BrokerReconciliationService] Manual reconciliation for {}", route);
        reconcile(route);
    }
}
