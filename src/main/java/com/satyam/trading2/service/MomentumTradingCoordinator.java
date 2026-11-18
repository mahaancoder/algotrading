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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
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
    private final Object batchLock = new Object();

    public MomentumTradingCoordinator(StockSelectionService selection, MomentumTradingStateService state,
            OrderExecutionService execution, MarketContextBuilder contexts, PendingOrderRepository orders,
            PositionManager positions, BroadcastService broadcasts) {
        this.selection=selection; this.state=state; this.execution=execution; this.contexts=contexts;
        this.orders=orders; this.positions=positions; this.broadcasts=broadcasts;
    }

    public Map<String,Object> executeScheduled(String slot, double budget) {
        synchronized (batchLock) {
            LocalTime at=LocalTime.parse(slot), now=LocalTime.now(ZONE);
            if (now.isAfter(at.plusSeconds(45))) return result("SKIPPED_LATE",0,0);
            if (!state.markScheduleExecuted(state.getTradingDate()+"@"+slot)) return result("ALREADY_EXECUTED",0,0);
            selection.updateSelection();
            return executeBatch(currentCandidates(),budget,false);
        }
    }
    public Map<String,Object> executeNewCandidates() {
        selection.updateSelection();
        List<Candidate> list=currentCandidates().stream().filter(c -> !state.hasSeenCandidate(c.symbol)).collect(Collectors.toList());
        return executeBatch(list,budgetForNow(),true);
    }
    public Map<String,Object> executeCurrentSelection() {
        selection.updateSelection();
        return executeBatch(currentCandidates(),budgetForNow(),true);
    }

    private Map<String,Object> executeBatch(List<Candidate> candidates,double budget,boolean markSeen) {
        synchronized(batchLock) {
            LocalTime now=LocalTime.now(ZONE);
            DayOfWeek day=LocalDate.now(ZONE).getDayOfWeek();
            if(day==DayOfWeek.SATURDAY || day==DayOfWeek.SUNDAY || now.isBefore(LocalTime.of(9,15))
                    || !now.isBefore(LocalTime.of(15,0))) return result("OUTSIDE_BUY_WINDOW",0,0);
            int submitted=0, skipped=0;
            Set<String> considered=new LinkedHashSet<>();
            for(Candidate c:candidates) {
                String symbol=c.symbol; considered.add(symbol);
                if(state.isBlacklisted(symbol)||state.isReserved(symbol)||state.isSelectedToday(symbol)){skipped++;continue;}
                if(hasOpenPosition(symbol)){state.markSelectedToday(symbol);skipped++;continue;}
                if(hasPendingBuy(symbol)){state.markSelectedToday(symbol);skipped++;continue;}
                double ltp=selection.getLatestPrice(symbol);
                if(!Double.isFinite(ltp)||ltp<=0){skipped++;continue;}
                int qty=(int)Math.floor(budget/ltp);
                if(qty<=0){skipped++;continue;}
                MarketContext ctx=contexts.build(symbol,null);
                if(ctx==null){skipped++;continue;}
                if(!state.tryReserveSymbol(symbol)){skipped++;continue;}
                TradeSignal signal=new TradeSignal(BUY,symbol,ltp,0.0,ltp*1.01,qty,c.reason,STRATEGY);
                signal.setEntryType(c.entryType);
                try {
                    String orderId=execution.execute(signal,ctx);
                    if(orderId==null){state.releaseReservationAfterNoOrder(symbol);skipped++;}
                    else {submitted++;log.info("[Momentum] BUY submitted {} qty={} LTP={} orderId={}",symbol,qty,ltp,orderId);}
                } catch(Exception e) {
                    // Unknown broker submission outcome: retain reservation until reconciliation.
                    log.error("[Momentum] BUY execution failed for {}; reservation retained",symbol,e);
                    skipped++;
                }
            }
            if(markSeen) state.markCandidatesSeen(considered);
            broadcastSnapshot();
            return result("COMPLETED",submitted,skipped);
        }
    }

    private boolean hasOpenPosition(String symbol) {
        Map<String,Position> found=positions.getPositionsForSymbol(symbol);
        return found!=null && found.values().stream().filter(Objects::nonNull).anyMatch(Position::isOpen);
    }
    private boolean hasPendingBuy(String symbol) {
        return orders.findOpenOrders().stream().filter(Objects::nonNull)
                .anyMatch(o->o.getSide()==TradeSide.BUY && symbol.equalsIgnoreCase(o.getSymbol()));
    }
    private List<Candidate> currentCandidates() {
        Map<String,Candidate> map=new LinkedHashMap<>();
        if(state.isGainersEnabled()) for(StockChange s:selection.getCurrentGainers())
            map.put(s.getSymbol(),new Candidate(s.getSymbol(),EntryType.TOP_GAINER,"Scheduled top gainer entry"));
        if(state.isLosersEnabled()) for(StockChange s:selection.getCurrentLosers())
            map.putIfAbsent(s.getSymbol(),new Candidate(s.getSymbol(),EntryType.TOP_LOSER,"Scheduled top loser entry"));
        if(state.isGainersEnabled()) for(String s:state.getWhitelist())
            if(!state.isBlacklisted(s)&&!state.isReserved(s)&&!state.isSelectedToday(s))
                map.putIfAbsent(s,new Candidate(s,EntryType.TOP_GAINER,"Manual whitelist entry"));
        return new ArrayList<>(map.values());
    }
    private double budgetForNow() {
        LocalTime n=LocalTime.now(ZONE);
        if(n.isBefore(LocalTime.of(9,16))) return 50000;
        if(n.isBefore(LocalTime.of(10,0))) return 25000;
        if(n.isBefore(LocalTime.of(12,0))) return 15000;
        return 10000;
    }

    public void addBlacklist(String s){state.addBlacklist(s);selection.updateSelection();broadcastSnapshot();}
    public void addWhitelist(String s){state.addWhitelist(s);selection.updateSelection();broadcastSnapshot();}
    public void removeWhitelist(String s){state.removeWhitelist(s);selection.updateSelection();broadcastSnapshot();}
    public void setGainersEnabled(boolean v){state.setGainersEnabled(v);selection.updateSelection();broadcastSnapshot();}
    public void setLosersEnabled(boolean v){state.setLosersEnabled(v);selection.updateSelection();broadcastSnapshot();}

    @Scheduled(fixedDelay=30000)
    public void broadcastSelectionSnapshot(){broadcastSnapshot();}

    public Map<String,Object> getDashboardSnapshot() {
        Map<String,Object> m=new LinkedHashMap<>();
        m.put("type","MOMENTUM_STATE"); m.put("tradingDate",state.getTradingDate());
        m.put("gainersEnabled",state.isGainersEnabled()); m.put("losersEnabled",state.isLosersEnabled());
        m.put("gainers",mapStocks(selection.getCurrentGainers())); m.put("losers",mapStocks(selection.getCurrentLosers()));
        m.put("blacklist",state.getBlacklist()); m.put("whitelist",state.getWhitelist());
        m.put("reserved",state.getReserved()); m.put("selectedToday",state.getSelectedToday());
        m.put("executedSchedules",state.getExecutedSchedules());
        return m;
    }
    private List<Map<String,Object>> mapStocks(List<StockChange> stocks) {
        List<Map<String,Object>> rows=new ArrayList<>();
        for(StockChange s:stocks) {
            Map<String,Object> r=new LinkedHashMap<>();
            r.put("symbol",s.getSymbol()); r.put("changePercent",s.getChange());
            r.put("ltp",selection.getLatestPrice(s.getSymbol()));
            r.put("traded",state.isBlacklisted(s.getSymbol())); r.put("pending",state.isReserved(s.getSymbol()));
            r.put("selectedToday",state.isSelectedToday(s.getSymbol()));
            r.put("whitelisted",state.getWhitelist().contains(s.getSymbol())); rows.add(r);
        }
        return rows;
    }
    private void broadcastSnapshot(){try{broadcasts.broadcastMomentumState(getDashboardSnapshot());}
        catch(Exception e){log.warn("[Momentum] Dashboard broadcast failed: {}",e.getMessage());}}
    private Map<String,Object> result(String status,int submitted,int skipped) {
        Map<String,Object> r=new LinkedHashMap<>(); r.put("status",status); r.put("submitted",submitted);
        r.put("skipped",skipped); r.put("timestamp",System.currentTimeMillis()); return r;
    }
    private static class Candidate {
        final String symbol; final EntryType entryType; final String reason;
        Candidate(String s,EntryType t,String r){symbol=s;entryType=t;reason=r;}
    }
}