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
    private final Map<String,Double> prevCloseMap = new ConcurrentHashMap<>();
    private static final Map<String,Double> ltpMap = new ConcurrentHashMap<>();
    private final Set<String> selectedGainers = ConcurrentHashMap.newKeySet();
    private final Set<String> selectedLosers = ConcurrentHashMap.newKeySet();
    private volatile List<StockChange> currentGainers = Collections.emptyList();
    private volatile List<StockChange> currentLosers = Collections.emptyList();
    private volatile LocalDate selectionDate = LocalDate.now(ZONE);
    private volatile long lastSelectionRefreshEpochMs;

    @Scheduled(fixedDelay = 60_000L)
    public void refreshSelection() {
        DayOfWeek day=LocalDate.now(ZONE).getDayOfWeek();
        if(day==DayOfWeek.SATURDAY||day==DayOfWeek.SUNDAY)return;
        LocalTime now=LocalTime.now(ZONE);
        if(now.isBefore(LocalTime.of(9,5))||now.isAfter(LocalTime.of(15,0)))return;
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
        Map<String,StockChange> all=new LinkedHashMap<>();
        for(Map.Entry<String,Double> e:prevCloseMap.entrySet()) {
            double prev=e.getValue()==null?0:e.getValue();
            double ltp=ltpMap.getOrDefault(e.getKey(),0.0);
            if(!Double.isFinite(prev)||prev<=0||!Double.isFinite(ltp)||ltp<=0)continue;
            all.put(e.getKey(),new StockChange(e.getKey(),((ltp-prev)/prev)*100.0));
        }
        Set<String> excluded=new HashSet<>(stateService.getBlacklist());
        excluded.addAll(stateService.getReserved());
        excluded.addAll(stateService.getSelectedToday());

        List<StockChange> gainers=new ArrayList<>(),losers=new ArrayList<>();
        if(stateService.isGainersEnabled()) {
            gainers=all.values().stream().filter(s->s.getChange() > 2.0)
                .filter(s->!excluded.contains(s.getSymbol()))
                .filter(s->!Exits.isSymbolExitedToday(s.getSymbol()))
                .sorted(Comparator.comparingDouble(StockChange::getChange).reversed())
                .limit(10).collect(Collectors.toList());
            for(String symbol:stateService.getWhitelist()) {
                if(excluded.contains(symbol)||Exits.isSymbolExitedToday(symbol))continue;
                if(gainers.stream().noneMatch(s->s.getSymbol().equals(symbol))
                        && ltpMap.getOrDefault(symbol,0.0)>0) {
                    gainers.add(all.getOrDefault(symbol,new StockChange(symbol,0.0)));
                }
            }
        }
        if(stateService.isLosersEnabled()) {
            losers=all.values().stream().filter(s->s.getChange() < -2.0)
                .filter(s->!excluded.contains(s.getSymbol()))
                .filter(s->!Exits.isSymbolExitedToday(s.getSymbol()))
                .sorted(Comparator.comparingDouble(StockChange::getChange))
                .limit(10).collect(Collectors.toList());
        }
        lastSelectionRefreshEpochMs = System.currentTimeMillis();
        currentGainers=Collections.unmodifiableList(new ArrayList<>(gainers));
        currentLosers=Collections.unmodifiableList(new ArrayList<>(losers));
        selectedGainers.clear(); selectedLosers.clear();
        currentGainers.forEach(s->selectedGainers.add(s.getSymbol()));
        currentLosers.forEach(s->selectedLosers.add(s.getSymbol()));
    }

    public boolean isTopLoser(String symbol){return selectedLosers.contains(symbol);}
    public boolean isTopGainer(String symbol){return selectedGainers.contains(symbol);}
    public List<StockChange> getCurrentGainers(){return currentGainers;}
    public List<StockChange> getCurrentLosers(){return currentLosers;}
    public void setPreviousClose(String symbol,double price){if(symbol!=null&&Double.isFinite(price)&&price>0)prevCloseMap.put(symbol,price);}
    public static void updateLtp(String symbol,double ltp){if(symbol!=null&&Double.isFinite(ltp)&&ltp>0)ltpMap.put(symbol,ltp);}
    public double getLatestPrice(String symbol){return ltpMap.getOrDefault(symbol,0.0);}
    public int getPreviousCloseCount(){return prevCloseMap.size();}
    public static int getLivePriceCount(){return ltpMap.size();}
    public long getLastSelectionRefreshEpochMs(){return lastSelectionRefreshEpochMs;}
}
