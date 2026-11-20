package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.EntryType;
import com.satyam.trading2.datamodel.MarketContext;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.StockChange;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.datamodel.TradeSignal;
import com.satyam.trading2.execution.OrderExecutionService;
import com.satyam.trading2.infrastructure.messaging.BroadcastService;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.domain.service.PositionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.satyam.trading2.datamodel.TradeSignal.SignalType.BUY;

@Slf4j
@Service
public class MomentumTradingCoordinator {
    public static final String STRATEGY = "Dip-Accumulator-Momentum";
    private static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");

    private final StockSelectionService selection;
    private final MomentumTradingStateService state;
    private final OrderExecutionService execution;
    private final MarketContextBuilder contexts;
    private final PendingOrderRepository orders;
    private final PositionManager positions;
    private final BroadcastService broadcasts;
    private final ExecutorService momentumOrderExecutor;

    /**
     * Serializes only the short preflight/reservation phase. It is never held
     * while waiting for a broker API response.
     */
    private final Object batchLock = new Object();

    public MomentumTradingCoordinator(StockSelectionService selection,
            MomentumTradingStateService state,
            OrderExecutionService execution,
            MarketContextBuilder contexts,
            PendingOrderRepository orders,
            PositionManager positions,
            BroadcastService broadcasts,
            @Qualifier("momentumOrderExecutor") ExecutorService momentumOrderExecutor) {
        this.selection = selection;
        this.state = state;
        this.execution = execution;
        this.contexts = contexts;
        this.orders = orders;
        this.positions = positions;
        this.broadcasts = broadcasts;
        this.momentumOrderExecutor = momentumOrderExecutor;
    }

    public Map<String, Object> executeScheduled(String slot, double budget) {
        synchronized (batchLock) {
            LocalTime at = LocalTime.parse(slot);
            LocalTime now = LocalTime.now(ZONE);
            if (now.isAfter(at.plusSeconds(45))) {
                return result("SKIPPED_LATE", 0, 0);
            }
            if (!state.markScheduleExecuted(state.getTradingDate() + "@" + slot)) {
                return result("ALREADY_EXECUTED", 0, 0);
            }
        }

        selection.updateSelection();
        return executeBatch(currentCandidates(), budget, false);
    }

    public Map<String, Object> executeNewCandidates() {
        selection.updateSelection();
        List<Candidate> list = currentCandidates().stream()
                .filter(c -> !state.hasSeenCandidate(c.symbol))
                .collect(Collectors.toList());
        return executeBatch(list, budgetForNow(), true);
    }

    public Map<String, Object> executeCurrentSelection() {
        selection.updateSelection();
        return executeBatch(currentCandidates(), budgetForNow(), true);
    }

    /**
     * Preflight is deliberately sequential and short: it snapshots pending BUYs,
     * checks current positions/prices and reserves each symbol before any worker
     * can submit an order. Independent symbols then build context and submit BUYs
     * concurrently on the dedicated bounded executor.
     */
    private Map<String, Object> executeBatch(List<Candidate> candidates,
            double budget, boolean markSeen) {

        List<ExecutionTask> tasks = new ArrayList<>();
        Set<String> considered = new LinkedHashSet<>();
        int skipped = 0;

        synchronized (batchLock) {
            LocalTime now = LocalTime.now(ZONE);
            DayOfWeek day = LocalDate.now(ZONE).getDayOfWeek();
            if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY
                    || now.isBefore(LocalTime.of(9, 15))
                    || !now.isBefore(LocalTime.of(15, 0))) {
                return result("OUTSIDE_BUY_WINDOW", 0, 0);
            }

            // One repository scan per batch rather than one scan per candidate.
            Set<String> pendingBuySymbols = new HashSet<>();
            List<com.satyam.trading2.datamodel.PendingOrder> openOrders =
                    orders.findOpenOrders();
            if (openOrders != null) {
                for (com.satyam.trading2.datamodel.PendingOrder order : openOrders) {
                    if (order != null && order.getSide() == TradeSide.BUY
                            && order.getSymbol() != null) {
                        pendingBuySymbols.add(order.getSymbol().toUpperCase(Locale.ROOT));
                    }
                }
            }

            Set<String> blacklist = state.getBlacklist();
            Set<String> reserved = state.getReserved();
            Set<String> selectedToday = state.getSelectedToday();

            for (Candidate candidate : candidates) {
                if (candidate == null || candidate.symbol == null) {
                    skipped++;
                    continue;
                }

                String symbol = candidate.symbol.trim().toUpperCase(Locale.ROOT);
                if (!considered.add(symbol)) {
                    skipped++;
                    continue;
                }

                if (blacklist.contains(symbol) || reserved.contains(symbol)
                        || selectedToday.contains(symbol)) {
                    skipped++;
                    continue;
                }
                if (hasOpenPosition(symbol)) {
                    state.markSelectedToday(symbol);
                    skipped++;
                    continue;
                }
                if (pendingBuySymbols.contains(symbol)) {
                    state.markSelectedToday(symbol);
                    skipped++;
                    continue;
                }

                double ltp = selection.getLatestPrice(symbol);
                if (!Double.isFinite(ltp) || ltp <= 0) {
                    skipped++;
                    continue;
                }

                int quantity = (int) Math.floor(budget / ltp);
                if (quantity <= 0) {
                    skipped++;
                    continue;
                }

                // This is the final atomic guard against another batch/thread
                // reserving the same symbol after the snapshots above.
                if (!state.tryReserveSymbol(symbol)) {
                    skipped++;
                    continue;
                }

                tasks.add(new ExecutionTask(candidate, symbol, ltp, quantity));
            }

            if (markSeen) {
                state.markCandidatesSeen(considered);
            }
        }

        List<CompletableFuture<ExecutionResult>> futures = new ArrayList<>(tasks.size());
        for (ExecutionTask task : tasks) {
            try {
                futures.add(CompletableFuture.supplyAsync(
                        () -> executeCandidate(task), momentumOrderExecutor));
            } catch (RuntimeException submissionFailure) {
                // The task did not enter the executor, so no broker request was
                // attempted and this reservation is safe to release.
                state.releaseReservationAfterNoOrder(task.symbol);
                skipped++;
                log.error("[Momentum] Could not schedule BUY for {}; reservation released",
                        task.symbol, submissionFailure);
            }
        }

        int submitted = 0;
        for (CompletableFuture<ExecutionResult> future : futures) {
            try {
                ExecutionResult executionResult = future.join();
                if (executionResult.submitted) {
                    submitted++;
                } else {
                    skipped++;
                }
            } catch (CompletionException unexpectedFailure) {
                // An unexpected worker failure may have happened after a broker
                // request began. Keep the reservation; reconciliation must decide.
                skipped++;
                log.error("[Momentum] Worker failed with uncertain submission outcome; "
                        + "reservation retained", unexpectedFailure.getCause());
            }
        }

        broadcastSnapshot();
        return result("COMPLETED", submitted, skipped);
    }

    private ExecutionResult executeCandidate(ExecutionTask task) {
        MarketContext context;
        try {
            context = contexts.build(task.symbol, null);
        } catch (Exception contextFailure) {
            // Context construction is local work and happens before broker access.
            state.releaseReservationAfterNoOrder(task.symbol);
            log.warn("[Momentum] Market context build failed for {}; reservation released",
                    task.symbol, contextFailure);
            return ExecutionResult.SKIPPED;
        }

        if (context == null) {
            state.releaseReservationAfterNoOrder(task.symbol);
            return ExecutionResult.SKIPPED;
        }

        TradeSignal signal = new TradeSignal(BUY, task.symbol, task.ltp, 0.0,
                task.ltp * 1.01, task.quantity, task.candidate.reason, STRATEGY);
        signal.setEntryType(task.candidate.entryType);

        try {
            String orderId = execution.execute(signal, context);
            if (orderId == null) {
                state.releaseReservationAfterNoOrder(task.symbol);
                return ExecutionResult.SKIPPED;
            }

            log.info("[Momentum] BUY submitted {} qty={} LTP={} orderId={} thread={}",
                    task.symbol, task.quantity, task.ltp, orderId,
                    Thread.currentThread().getName());
            return ExecutionResult.SUBMITTED;
        } catch (Exception brokerFailure) {
            // The broker may have accepted the request before the client timed
            // out. Do not release this reservation until order reconciliation.
            log.error("[Momentum] BUY execution failed for {}; reservation retained",
                    task.symbol, brokerFailure);
            return ExecutionResult.SKIPPED;
        }
    }

    private boolean hasOpenPosition(String symbol) {
        Map<String, Position> found = positions.getPositionsForSymbol(symbol);
        return found != null && found.values().stream()
                .filter(Objects::nonNull)
                .anyMatch(Position::isOpen);
    }

    private List<Candidate> currentCandidates() {
        Map<String, Candidate> map = new LinkedHashMap<>();
        if (state.isGainersEnabled()) {
            for (StockChange stock : selection.getCurrentGainers()) {
                map.put(stock.getSymbol(), new Candidate(stock.getSymbol(),
                        EntryType.TOP_GAINER, "Scheduled top gainer entry"));
            }
        }
        if (state.isLosersEnabled()) {
            for (StockChange stock : selection.getCurrentLosers()) {
                map.putIfAbsent(stock.getSymbol(), new Candidate(stock.getSymbol(),
                        EntryType.TOP_LOSER, "Scheduled top loser entry"));
            }
        }
        if (state.isGainersEnabled()) {
            for (String symbol : state.getWhitelist()) {
                if (!state.isBlacklisted(symbol) && !state.isReserved(symbol)
                        && !state.isSelectedToday(symbol)) {
                    map.putIfAbsent(symbol, new Candidate(symbol,
                            EntryType.TOP_GAINER, "Manual whitelist entry"));
                }
            }
        }
        return new ArrayList<>(map.values());
    }

    private double budgetForNow() {
        LocalTime now = LocalTime.now(ZONE);
        if (now.isBefore(LocalTime.of(9, 16))) return 50000;
        if (now.isBefore(LocalTime.of(10, 0))) return 25000;
        if (now.isBefore(LocalTime.of(12, 0))) return 15000;
        return 10000;
    }

    public void addBlacklist(String symbol) {
        state.addBlacklist(symbol);
        selection.updateSelection();
        broadcastSnapshot();
    }

    public void addWhitelist(String symbol) {
        state.addWhitelist(symbol);
        selection.updateSelection();
        broadcastSnapshot();
    }

    public void removeWhitelist(String symbol) {
        state.removeWhitelist(symbol);
        selection.updateSelection();
        broadcastSnapshot();
    }

    public void setMomentumThresholdPercent(double value) {
        state.setMomentumThresholdPercent(value);
        selection.updateSelection();
        broadcastSnapshot();
    }

    public void setGainersEnabled(boolean enabled) {
        state.setGainersEnabled(enabled);
        selection.updateSelection();
        broadcastSnapshot();
    }

    public void setLosersEnabled(boolean enabled) {
        state.setLosersEnabled(enabled);
        selection.updateSelection();
        broadcastSnapshot();
    }

    @Scheduled(fixedDelay = 30000)
    public void broadcastSelectionSnapshot() {
        broadcastSnapshot();
    }

    public Map<String, Object> getDashboardSnapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("type", "MOMENTUM_STATE");
        snapshot.put("tradingDate", state.getTradingDate());
        snapshot.put("gainersEnabled", state.isGainersEnabled());
        snapshot.put("losersEnabled", state.isLosersEnabled());
        snapshot.put("momentumThresholdPercent", state.getMomentumThresholdPercent());
        snapshot.put("gainers", mapStocks(selection.getCurrentGainers()));
        snapshot.put("losers", mapStocks(selection.getCurrentLosers()));
        snapshot.put("previousCloseCount", selection.getPreviousCloseCount());
        snapshot.put("livePriceCount", StockSelectionService.getLivePriceCount());
        snapshot.put("lastSelectionRefreshEpochMs", selection.getLastSelectionRefreshEpochMs());
        snapshot.put("blacklist", state.getBlacklist());
        snapshot.put("whitelist", state.getWhitelist());
        snapshot.put("reserved", state.getReserved());
        snapshot.put("selectedToday", state.getSelectedToday());
        snapshot.put("executedSchedules", state.getExecutedSchedules());
        return snapshot;
    }

    private List<Map<String, Object>> mapStocks(List<StockChange> stocks) {
        List<Map<String, Object>> rows = new ArrayList<>(stocks.size());
        Set<String> whitelist = state.getWhitelist();
        for (StockChange stock : stocks) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("symbol", stock.getSymbol());
            row.put("changePercent", stock.getChange());
            row.put("ltp", selection.getLatestPrice(stock.getSymbol()));
            row.put("traded", state.isBlacklisted(stock.getSymbol()));
            row.put("pending", state.isReserved(stock.getSymbol()));
            row.put("selectedToday", state.isSelectedToday(stock.getSymbol()));
            row.put("whitelisted", whitelist.contains(stock.getSymbol()));
            rows.add(row);
        }
        return rows;
    }

    private void broadcastSnapshot() {
        try {
            broadcasts.broadcastMomentumState(getDashboardSnapshot());
        } catch (Exception e) {
            log.warn("[Momentum] Dashboard broadcast failed: {}", e.getMessage());
        }
    }

    private Map<String, Object> result(String status, int submitted, int skipped) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", status);
        result.put("submitted", submitted);
        result.put("skipped", skipped);
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }

    private static class Candidate {
        final String symbol;
        final EntryType entryType;
        final String reason;

        Candidate(String symbol, EntryType entryType, String reason) {
            this.symbol = symbol;
            this.entryType = entryType;
            this.reason = reason;
        }
    }

    private static class ExecutionTask {
        final Candidate candidate;
        final String symbol;
        final double ltp;
        final int quantity;

        ExecutionTask(Candidate candidate, String symbol, double ltp, int quantity) {
            this.candidate = candidate;
            this.symbol = symbol;
            this.ltp = ltp;
            this.quantity = quantity;
        }
    }

    private static class ExecutionResult {
        static final ExecutionResult SUBMITTED = new ExecutionResult(true);
        static final ExecutionResult SKIPPED = new ExecutionResult(false);
        final boolean submitted;

        private ExecutionResult(boolean submitted) {
            this.submitted = submitted;
        }
    }
}
