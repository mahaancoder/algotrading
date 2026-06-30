package com.satyam.trading2.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyam.trading2.application.dto.PositionDto;
import com.satyam.trading2.application.dto.PositionsSnapshotDto;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeEvent;
import com.satyam.trading2.datamodel.TradeRecord;
import com.satyam.trading2.domain.service.PnLCalculator;
import com.satyam.trading2.domain.service.PositionManager;
import com.satyam.trading2.helpers.LatestPriceHelper;
import com.satyam.trading2.risk.RiskManager;
import com.satyam.trading2.scheduler.MarginFetchScheduler;
import com.satyam.trading2.service.TradeJournalService;
import com.satyam.trading2.strategy.StrategyEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SnapshotService handles sending full state snapshots to WebSocket clients.
 * This is used when a client first connects to get the complete current state.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotService {

    private final PositionManager positionManager;
    private final PnLCalculator pnlCalculator;
    private final TradeJournalService tradeJournalService;
    private final BroadcastService broadcastService;
    private final ObjectMapper objectMapper;
    private final StrategyEngine strategyEngine;
    private final RiskManager riskManager;
    private final MarginFetchScheduler marginFetchScheduler;
    private final com.satyam.trading2.service.EntryTypeAnalyticsService entryTypeAnalytics;
    private final com.satyam.trading2.service.KillSwitchService killSwitchService;

    
    /**
     * Send complete snapshot to a specific WebSocket session
     */
    public void sendFullSnapshot(WebSocketSession session) {
        try {
            log.info("📸 Sending full snapshot to session: {}", session.getId());

            Map<String, Double> latestPriceMap = LatestPriceHelper.getLatestPriceMap();

            // 1. OPEN POSITIONS (INTRADAY ONLY)
            sendIntradayPositions(session, latestPriceMap);
            
            // 2. CLOSED TRADES
            sendClosedTrades(session);
            
            // 3. TOTAL REALIZED P&L
            sendTotalPnL(session);
            
            // 4. TODAY'S P&L
            sendTodaysPnL(session);
            
            // 5. STRATEGY P&L
            sendStrategyPnLs(session);
            
            // 6. ALL TRADE EVENTS
            sendTradeEvents(session);
            
            // 7. HOLDINGS
            sendHoldings(session, latestPriceMap);

            // 8. CAPITAL USED (for Capital Remaining calculation)
            sendCapitalUsed(session);

            // 9. ENTRY TYPE ANALYTICS
            sendEntryTypeAnalytics(session);

            // 10. KILL SWITCH STATUS
            sendKillSwitchStatus(session);

            log.info("✅ Snapshot sent successfully to session: {}", session.getId());
        } catch (Exception e) {
            log.error("Failed to send snapshot to session: {}", session.getId(), e);
        }
    }
    
    /**
     * Send intraday positions as a single batch message
     */
    private void sendIntradayPositions(WebSocketSession session, Map<String, Double> latestPriceMap)
            throws Exception {
        List<PositionDto> positions = new ArrayList<>();

        for (Position p : positionManager.getAllIntradayPositions()) {
            if (!p.isOpen()) continue;

            double last = latestPriceMap.getOrDefault(p.getSymbol(), p.getAveragePrice());
            PositionDto dto = PositionDto.fromIntradayPosition(p, last);
            positions.add(dto);
        }

        // Send all positions in a single message
        if (!positions.isEmpty()) {
            PositionsSnapshotDto snapshot = PositionsSnapshotDto.intradaySnapshot(positions);
            session.sendMessage(new TextMessage(broadcastService.toJson(snapshot)));
            log.debug("Sent {} intraday positions in batch", positions.size());
        }
    }
    
    /**
     * Send closed trades
     */
    private void sendClosedTrades(WebSocketSession session) throws Exception {
        List<TradeRecord> trades = tradeJournalService.getAll();
        log.info("📤 Sending {} closed trades to client", trades.size());
        for (TradeRecord t : trades) {
            String entryType = t.getEntryType() != null ? t.getEntryType().name() : null;
            String entryTypeField = entryType != null ? ", \"entryType\":\"" + entryType + "\"" : "";

            String json = String.format(
                "{ \"type\":\"TRADE\", \"symbol\":\"%s\", \"strategy\":\"%s\", " +
                "\"entry\":%s, \"exit\":%s, \"qty\":%d, \"pnl\":%.2f, \"time\":%d%s }",
                t.getInstrument(), t.getStrategyName(),
                t.getEntryPrice(), t.getExitPrice(),
                t.getQuantity(), t.getPnl(), t.getTimestamp(), entryTypeField
            );
            session.sendMessage(new TextMessage(json));
        }
    }
    
    /**
     * Send total P&L
     */
    private void sendTotalPnL(WebSocketSession session) throws Exception {
        double totalRealizedPnL = tradeJournalService.getAll().stream()
            .mapToDouble(TradeRecord::getPnl)
            .sum();

        log.info("📤 Sending Total Realized P&L: ₹{}", totalRealizedPnL);
        String json = String.format("{ \"type\":\"PNL\", \"value\":%.2f }", totalRealizedPnL);
        session.sendMessage(new TextMessage(json));
    }

    /**
     * Send today's P&L
     */
    private void sendTodaysPnL(WebSocketSession session) throws Exception {
        double todaysPnL = pnlCalculator.getTodaysPnL();
        log.info("📤 Sending Today's P&L: ₹{} (resets at 9:15 AM daily)", todaysPnL);
        String json = String.format("{ \"type\":\"TODAYS_PNL\", \"value\":%.2f }", todaysPnL);
        session.sendMessage(new TextMessage(json));
    }
    
    /**
     * Send strategy P&Ls
     */
    private void sendStrategyPnLs(WebSocketSession session) throws Exception {
        Map<String, Double> strategyPnLs = pnlCalculator.getAllStrategyPnLs();
        log.info("📤 Sending Strategy P&Ls: {}", strategyPnLs);
        for (Map.Entry<String, Double> e : strategyPnLs.entrySet()) {
            String json = String.format(
                "{ \"type\":\"STRATEGY_PNL\", \"strategy\":\"%s\", \"pnl\":%.2f }",
                e.getKey(), e.getValue()
            );
            session.sendMessage(new TextMessage(json));
        }
    }
    
    /**
     * Send all trade events
     */
    private void sendTradeEvents(WebSocketSession session) throws Exception {
        List<TradeEvent> events = tradeJournalService.getAllEvents();
        log.info("📤 Sending {} trade events (BUY/SELL) to client", events.size());
        for (TradeEvent e : events) {
            String entryType = e.getEntryType() != null ? e.getEntryType().name() : null;
            String entryTypeField = entryType != null ? ", \"entryType\":\"" + entryType + "\"" : "";

            String json = String.format(
                "{ \"type\":\"EVENT\", \"symbol\":\"%s\", \"strategy\":\"%s\", " +
                "\"etype\":\"%s\", \"price\":%s, \"qty\":%d, \"pnl\":%s, \"time\":%d%s }",
                e.getSymbol(), e.getStrategy(), e.getType(),
                e.getPrice(), e.getQty(), e.getPnl(), e.getTime(), entryTypeField
            );
            session.sendMessage(new TextMessage(json));
        }
    }
    
    /**
     * Send holdings as a single batch message
     */
    private void sendHoldings(WebSocketSession session, Map<String, Double> latestPriceMap)
            throws Exception {
        List<PositionDto> holdings = new ArrayList<>();

        for (Position p : positionManager.getAllHoldings()) {
            if (!p.isOpen()) continue;

            double last = latestPriceMap.getOrDefault(p.getSymbol(), p.getAveragePrice());
            PositionDto dto = PositionDto.fromHolding(p, last);
            holdings.add(dto);
        }

        // Send all holdings in a single message
        if (!holdings.isEmpty()) {
            PositionsSnapshotDto snapshot = PositionsSnapshotDto.holdingsSnapshot(holdings);
            session.sendMessage(new TextMessage(broadcastService.toJson(snapshot)));
            log.debug("Sent {} holdings in batch", holdings.size());
        }
    }

    /**
     * Send capital used information
     * Calculates total capital deployed in INTRADAY positions and HOLDINGS
     * Also sends PER-STRATEGY capital usage with MAX limits from RiskManager
     * Holdings capital is shown separately, not included in strategy capital
     */
    private void sendCapitalUsed(WebSocketSession session) throws Exception {
        // Calculate intraday capital
        double intradayCapital = positionManager.getAllIntradayPositions().stream()
            .filter(Position::isOpen)
            .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
            .sum();

        // Calculate holdings capital
        double holdingsCapital = positionManager.getAllHoldings().stream()
            .filter(Position::isOpen)
            .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
            .sum();

        // Initialize ALL registered strategies with 0 capital
        Map<String, Double> strategyCapital = new HashMap<>();
        for (String strategyName : strategyEngine.getAllStrategyNames()) {
            strategyCapital.put(strategyName, 0.0);
        }

        // Calculate PER-STRATEGY capital usage from INTRADAY positions only (exclude holdings)
        positionManager.getAllIntradayPositions().stream()
            .filter(Position::isOpen)
            .forEach(p -> {
                String strategy = p.getStrategy();
                double capital = p.getAveragePrice() * p.getTotalQuantity();
                strategyCapital.merge(strategy, capital, Double::sum);
            });

        // Build strategy capital JSON array with max capital limits from RiskManager
        StringBuilder strategyJson = new StringBuilder("[");
        boolean first = true;
        for (Map.Entry<String, Double> entry : strategyCapital.entrySet()) {
            if (!first) strategyJson.append(",");

            // Get max capital for this strategy from RiskManager
            double maxCapital = marginFetchScheduler.getMaxCapitalPerStrategy(entry.getKey());

            strategyJson.append(String.format("{\"strategy\":\"%s\",\"capital\":%.2f,\"maxCapital\":%.2f}",
                entry.getKey(), entry.getValue(), maxCapital));
            first = false;
        }
        strategyJson.append("]");

        // Send capital used message
        String json = String.format(
            "{ \"type\":\"CAPITAL_USED\", \"intraday\":%s, \"holdings\":%s, \"strategies\":%s }",
            intradayCapital, holdingsCapital, strategyJson.toString()
        );

        session.sendMessage(new TextMessage(json));
        log.debug("Sent capital used: intraday={}, holdings={}, strategies={}",
            intradayCapital, holdingsCapital, strategyCapital);
    }

    /**
     * Send entry type analytics
     */
    private void sendEntryTypeAnalytics(WebSocketSession session) throws Exception {
        com.satyam.trading2.application.dto.EntryTypeAnalyticsDto analyticsDto =
            com.satyam.trading2.application.dto.EntryTypeAnalyticsDto.from(
                entryTypeAnalytics.getAnalyticsSummary()
            );

        String json = broadcastService.toJson(analyticsDto);
        session.sendMessage(new TextMessage(json));
        log.info("📤 Sent entry type analytics");
    }

    /**
     * Send current kill switch status
     */
    private void sendKillSwitchStatus(WebSocketSession session) throws Exception {
        boolean isActive = killSwitchService.isActive();
        String json = String.format("{ \"type\":\"KILL_SWITCH\", \"message\":\"%s\" }",
                                    isActive ? "active" : "inactive");
        session.sendMessage(new TextMessage(json));
        log.info("📤 Sent kill switch status: {}", isActive ? "ACTIVE" : "INACTIVE");
    }
}

