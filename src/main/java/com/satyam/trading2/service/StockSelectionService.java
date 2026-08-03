package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.StockChange;
import com.satyam.trading2.risk.Exits;
import kotlin.NoWhenBranchMatchedException;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
public class StockSelectionService {

    private final EMAService emaService;
    private final CandleAggregator candleAggregator;
    private final RelativeStrengthService relativeStrengthService;
    private final StockFilterService stockFilterService;

    private final Map<String, Double> prevCloseMap = new ConcurrentHashMap<>();; // already you have
    private static final Map<String, Double> ltpMap = new ConcurrentHashMap<>();       // live prices

    private final Set<String> selectedGainers = ConcurrentHashMap.newKeySet();
    private final Set<String> selectedLosers = ConcurrentHashMap.newKeySet();

    @Scheduled(fixedDelay = 30000)
    public void refreshSelection() {
        // Check if today is a weekday (Monday to Friday)
        DayOfWeek today = LocalDate.now().getDayOfWeek();
        if (today == DayOfWeek.SATURDAY || today == DayOfWeek.SUNDAY) {
            return;
        }

        LocalTime now = LocalTime.now();
        LocalTime startTime = LocalTime.of(9, 5);
        LocalTime endTime = LocalTime.of(15, 15);

        if (now.isBefore(startTime) || now.isAfter(endTime)) {
            return;
        }
        updateSelection();
    }

    public void updateSelection() {
        LocalTime now = LocalTime.now();
        List<StockChange> list = new ArrayList<>();

        int validStocks = 0;
        int missingLtp = 0;
        int invalidPrevClose = 0;

        for (String symbol : prevCloseMap.keySet()) {
            double prev = prevCloseMap.get(symbol);
            double ltp = ltpMap.getOrDefault(symbol, 0.0);

            if (prev <= 0) {
                invalidPrevClose++;
                continue;
            }
            if (ltp <= 0) {
                missingLtp++;
                continue;
            }

            validStocks++;
            double change = ((ltp - prev) / prev) * 100;
            list.add(new StockChange(symbol, change));
        }

        // ===== SORT ONCE: ASCENDING (losers first, gainers last) =====
        list.sort(Comparator.comparingDouble(StockChange::getChange));

        // ===== TOP 20 LOSERS (biggest negative changes) =====
        Set<String> newLosers = ConcurrentHashMap.newKeySet();
        List<StockChange> topLosers = new ArrayList<>();
        list.stream()
                .filter(s -> s.getChange() <= -2)           // Only stocks down 2% or more
                .filter(s -> !Exits.isSymbolExitedToday(s.getSymbol()))  // Skip symbols exited today
                .filter(s -> {
                    double ltp = ltpMap.getOrDefault(s.getSymbol(), 0.0);
                    return stockFilterService.passesLoserFilter(s.getSymbol(), ltp, s.getChange());
                })
                .limit(20)                                   // Top 20 biggest losers
                .forEach(s -> {
                    newLosers.add(s.getSymbol());
                    topLosers.add(s);
                });

        // ===== TOP 20 GAINERS (biggest positive changes) =====
        Set<String> newGainers = ConcurrentHashMap.newKeySet();
        List<StockChange> topGainers = new ArrayList<>();
        int count = 0;
        for (int i = list.size() - 1; i >= 0 && count < 20; i--) {
            StockChange stock = list.get(i);
            if (stock.getChange() < 1) {
                break;
            }
            if (stock.getChange() >= 1) {
                if (!Exits.isSymbolExitedToday(stock.getSymbol())) {
                    double ltp = ltpMap.getOrDefault(stock.getSymbol(), 0.0);
                    if (stockFilterService.passesGainerFilter(stock.getSymbol(), ltp, stock.getChange())) {
                        newGainers.add(stock.getSymbol());
                        topGainers.add(stock);
                        count++;
                    }
                }
            }
        }

        // ===== ATOMIC SWAP: Replace old sets with new ones =====
        selectedLosers.clear();
        selectedLosers.addAll(newLosers);
        selectedGainers.clear();
        selectedGainers.addAll(newGainers);

        // ===== DETAILED LOGGING =====
        if (!selectedLosers.isEmpty() || !selectedGainers.isEmpty()) {
            System.out.println("📊 STOCK SELECTION UPDATE - " + new java.util.Date());

            if (!topLosers.isEmpty()) {
                System.out.println("\n🔻 TOP " + topLosers.size() + " LOSERS (sorted by biggest loss):");
                System.out.println("-".repeat(100));
                for (int i = 0; i < topLosers.size(); i++) {
                    StockChange stock = topLosers.get(i);
                    System.out.printf("  %2d. %-15s %6.2f%%", (i + 1), stock.getSymbol(), stock.getChange());
                    System.out.println();
                }
            } else {
                System.out.println("\n🔻 TOP LOSERS: None");
            }

            if (!topGainers.isEmpty()) {
                System.out.println("\n🔺 TOP " + topGainers.size() + " GAINERS (sorted by biggest gain):");
                System.out.println("-".repeat(100));
                for (int i = 0; i < topGainers.size(); i++) {
                    StockChange stock = topGainers.get(i);
                    System.out.printf("  %2d. %-15s %6.2f%%", (i + 1), stock.getSymbol(), stock.getChange());
                    System.out.println();
                }
            } else {
                System.out.println("\n🔺 TOP GAINERS: None");
            }
        }
    }

    public boolean isTopLoser(String symbol) {
        return selectedLosers.contains(symbol);
    }

    public boolean isTopGainer(String symbol) {
        return selectedGainers.contains(symbol);
    }

    public void setPreviousClose(String symbol, double price) {
        prevCloseMap.put(symbol, price);
    }

    public static void updateLtp(String symbol, double ltp) {
        ltpMap.put(symbol, ltp);
    }
}
