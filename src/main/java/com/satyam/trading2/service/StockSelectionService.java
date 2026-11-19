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
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class StockSelectionService {
    private static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");
    private final MomentumTradingStateService stateService;
    private final Map<String, Double> prevCloseMap = new ConcurrentHashMap<>();
    private static final Map<String, Double> ltpMap = new ConcurrentHashMap<>();
    private final Set<String> selectedGainers = ConcurrentHashMap.newKeySet();
    private final Set<String> selectedLosers = ConcurrentHashMap.newKeySet();
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

    public synchronized void updateSelection() {
        LocalDate today = LocalDate.now(ZONE);
        if (!today.equals(selectionDate)) {
            prevCloseMap.clear();
            ltpMap.clear();
            selectedGainers.clear();
            selectedLosers.clear();
            currentGainers = Collections.emptyList();
            currentLosers = Collections.emptyList();
            selectionDate = today;
        }

        Map<String, StockChange> all = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : prevCloseMap.entrySet()) {
            double previousClose = entry.getValue() == null ? 0 : entry.getValue();
            double ltp = ltpMap.getOrDefault(entry.getKey(), 0.0);
            if (!Double.isFinite(previousClose) || previousClose <= 0
                    || !Double.isFinite(ltp) || ltp <= 0) {
                continue;
            }
            all.put(entry.getKey(),
                    new StockChange(entry.getKey(), ((ltp - previousClose) / previousClose) * 100.0));
        }

        // Rank the complete market universe first. Exclusions are applied only
        // after taking the raw top 10, so blacklisted/reserved/previously-selected
        // symbols reduce the published list instead of being replaced by lower ranks.
        Set<String> excluded = new HashSet<>(stateService.getBlacklist());
        excluded.addAll(stateService.getReserved());
        excluded.addAll(stateService.getSelectedToday());

        List<StockChange> gainers = Collections.emptyList();
        if (stateService.isGainersEnabled()) {
            List<StockChange> rawTopGainers = all.values().stream()
                    .filter(stock -> stock.getChange() > 2.0)
                    .sorted(Comparator.comparingDouble(StockChange::getChange).reversed())
                    .limit(10)
                    .collect(Collectors.toList());

            gainers = rawTopGainers.stream()
                    .filter(stock -> !excluded.contains(stock.getSymbol()))
                    .filter(stock -> !Exits.isSymbolExitedToday(stock.getSymbol()))
                    .collect(Collectors.toList());
        }

        List<StockChange> losers = Collections.emptyList();
        if (stateService.isLosersEnabled()) {
            List<StockChange> rawTopLosers = all.values().stream()
                    .filter(stock -> stock.getChange() < -2.0)
                    .sorted(Comparator.comparingDouble(StockChange::getChange))
                    .limit(10)
                    .collect(Collectors.toList());

            losers = rawTopLosers.stream()
                    .filter(stock -> !excluded.contains(stock.getSymbol()))
                    .filter(stock -> !Exits.isSymbolExitedToday(stock.getSymbol()))
                    .collect(Collectors.toList());
        }

        lastSelectionRefreshEpochMs = System.currentTimeMillis();
        currentGainers = Collections.unmodifiableList(new ArrayList<>(gainers));
        currentLosers = Collections.unmodifiableList(new ArrayList<>(losers));
        selectedGainers.clear();
        selectedLosers.clear();
        currentGainers.forEach(stock -> selectedGainers.add(stock.getSymbol()));
        currentLosers.forEach(stock -> selectedLosers.add(stock.getSymbol()));
    }

    public boolean isTopLoser(String symbol) {
        return selectedLosers.contains(symbol);
    }

    public boolean isTopGainer(String symbol) {
        return selectedGainers.contains(symbol);
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
}
