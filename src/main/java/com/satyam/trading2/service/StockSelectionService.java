package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.StockChange;
import com.satyam.trading2.risk.Exits;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
public class StockSelectionService {
    private static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");
    private static final int TOP_K = 10;

    private final MomentumTradingStateService stateService;
    private final Map<String, Double> prevCloseMap = new ConcurrentHashMap<>();
    private static final Map<String, Double> ltpMap = new ConcurrentHashMap<>();

    // Immutable snapshots are replaced atomically after each calculation.
    private volatile Set<String> selectedGainers = Collections.emptySet();
    private volatile Set<String> selectedLosers = Collections.emptySet();
    private volatile List<StockChange> currentGainers = Collections.emptyList();
    private volatile List<StockChange> currentLosers = Collections.emptyList();
    private volatile LocalDate selectionDate = LocalDate.now(ZONE);
    private volatile long lastSelectionRefreshEpochMs;

    @Scheduled(fixedDelay = 60_000L)
    public void refreshSelection() {
        DayOfWeek day = LocalDate.now(ZONE).getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) return;

        LocalTime now = LocalTime.now(ZONE);
        if (now.isBefore(LocalTime.of(9, 5)) || now.isAfter(LocalTime.of(15, 0))) return;
        updateSelection();
    }

    /**
     * One pass over the market universe with bounded Top-K heaps.
     *
     * Ranking is intentionally performed before exclusions: this preserves the
     * existing strategy behavior where an excluded raw top-10 symbol is not
     * replaced by the next eligible rank.
     */
    public synchronized void updateSelection() {
        LocalDate today = LocalDate.now(ZONE);
        if (!today.equals(selectionDate)) {
            prevCloseMap.clear();
            ltpMap.clear();
            selectedGainers = Collections.emptySet();
            selectedLosers = Collections.emptySet();
            currentGainers = Collections.emptyList();
            currentLosers = Collections.emptyList();
            selectionDate = today;
        }

        boolean gainersEnabled = stateService.isGainersEnabled();
        boolean losersEnabled = stateService.isLosersEnabled();
        double thresholdPercent = stateService.getMomentumThresholdPercent();

        Comparator<RankedStock> gainerBestFirst = (left, right) -> {
            int byChange = Double.compare(right.stock.getChange(), left.stock.getChange());
            return byChange != 0 ? byChange : Integer.compare(left.index, right.index);
        };
        Comparator<RankedStock> loserBestFirst = (left, right) -> {
            int byChange = Double.compare(left.stock.getChange(), right.stock.getChange());
            return byChange != 0 ? byChange : Integer.compare(left.index, right.index);
        };

        PriorityQueue<RankedStock> topGainers =
                new PriorityQueue<>(TOP_K + 1, gainerBestFirst.reversed());
        PriorityQueue<RankedStock> topLosers =
                new PriorityQueue<>(TOP_K + 1, loserBestFirst.reversed());

        int index = 0;
        for (Map.Entry<String, Double> entry : prevCloseMap.entrySet()) {
            String symbol = entry.getKey();
            Double previousCloseValue = entry.getValue();
            double previousClose = previousCloseValue == null ? 0.0 : previousCloseValue;
            double ltp = ltpMap.getOrDefault(symbol, 0.0);

            if (!Double.isFinite(previousClose) || previousClose <= 0.0
                    || !Double.isFinite(ltp) || ltp <= 0.0) {
                index++;
                continue;
            }

            double change = ((ltp - previousClose) / previousClose) * 100.0;
            if (!Double.isFinite(change)) {
                index++;
                continue;
            }

            StockChange stock = new StockChange(symbol, change);
            RankedStock ranked = new RankedStock(stock, index);

            if (gainersEnabled && change >= thresholdPercent) {
                offerTop(topGainers, ranked, gainerBestFirst);
            }
            if (losersEnabled && change <= -thresholdPercent) {
                offerTop(topLosers, ranked, loserBestFirst);
            }
            index++;
        }

        // Snapshot mutable state once, rather than asking the state service for
        // a fresh defensive copy for every candidate.
        Set<String> excluded = new HashSet<>(stateService.getBlacklist());
        excluded.addAll(stateService.getReserved());
        excluded.addAll(stateService.getSelectedToday());

        List<StockChange> gainers = gainersEnabled
                ? publishEligible(topGainers, gainerBestFirst, excluded)
                : Collections.emptyList();
        List<StockChange> losers = losersEnabled
                ? publishEligible(topLosers, loserBestFirst, excluded)
                : Collections.emptyList();

        // Publish complete immutable snapshots; readers never observe a set
        // being cleared and repopulated while a trading decision is in flight.
        currentGainers = Collections.unmodifiableList(new ArrayList<>(gainers));
        currentLosers = Collections.unmodifiableList(new ArrayList<>(losers));
        selectedGainers = immutableSymbols(gainers);
        selectedLosers = immutableSymbols(losers);
        lastSelectionRefreshEpochMs = System.currentTimeMillis();
    }

    private void offerTop(PriorityQueue<RankedStock> queue, RankedStock candidate,
            Comparator<RankedStock> bestFirst) {
        if (queue.size() < TOP_K) {
            queue.offer(candidate);
            return;
        }

        RankedStock currentWorst = queue.peek();
        if (bestFirst.compare(candidate, currentWorst) < 0) {
            queue.poll();
            queue.offer(candidate);
        }
    }

    private List<StockChange> publishEligible(PriorityQueue<RankedStock> queue,
            Comparator<RankedStock> bestFirst, Set<String> excluded) {
        List<RankedStock> ranked = new ArrayList<>(queue);
        ranked.sort(bestFirst);

        List<StockChange> result = new ArrayList<>(ranked.size());
        for (RankedStock item : ranked) {
            StockChange stock = item.stock;
            String symbol = stock.getSymbol();
            if (!excluded.contains(symbol) && !Exits.isSymbolExitedToday(symbol)) {
                result.add(stock);
            }
        }
        return result;
    }

    private Set<String> immutableSymbols(List<StockChange> stocks) {
        Set<String> symbols = new HashSet<>(Math.max(16, stocks.size() * 2));
        for (StockChange stock : stocks) {
            symbols.add(stock.getSymbol());
        }
        return Collections.unmodifiableSet(symbols);
    }

    public boolean isTopLoser(String symbol) {
        return symbol != null && selectedLosers.contains(symbol);
    }

    public boolean isTopGainer(String symbol) {
        return symbol != null && selectedGainers.contains(symbol);
    }

    public List<StockChange> getCurrentGainers() {
        return currentGainers;
    }

    public List<StockChange> getCurrentLosers() {
        return currentLosers;
    }

    public void setPreviousClose(String symbol, double price) {
        if (symbol != null && Double.isFinite(price) && price > 0) {
            prevCloseMap.put(symbol, price);
        }
    }

    public static void updateLtp(String symbol, double ltp) {
        if (symbol != null && Double.isFinite(ltp) && ltp > 0) {
            ltpMap.put(symbol, ltp);
        }
    }

    public double getLatestPrice(String symbol) {
        return ltpMap.getOrDefault(symbol, 0.0);
    }

    public int getPreviousCloseCount() {
        return prevCloseMap.size();
    }

    public static int getLivePriceCount() {
        return ltpMap.size();
    }

    public long getLastSelectionRefreshEpochMs() {
        return lastSelectionRefreshEpochMs;
    }

    private static final class RankedStock {
        private final StockChange stock;
        private final int index;

        private RankedStock(StockChange stock, int index) {
            this.stock = stock;
            this.index = index;
        }
    }
}
