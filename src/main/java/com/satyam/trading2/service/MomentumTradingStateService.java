package com.satyam.trading2.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.TradeSide;
import com.satyam.trading2.order.PendingOrderRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

@Service
public class MomentumTradingStateService {
    private static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");
    private static final Pattern SYMBOL = Pattern.compile("[A-Z0-9&._-]{1,40}");
    private static final String STRATEGY = "Dip-Accumulator-Momentum";
    private final ObjectMapper mapper;
    private final PendingOrderRepository orders;
    private final Object lock = new Object();
    @Value("${momentum.state.file.path:/home/ec2-user/momentum_trading_state.json}")
    private String filePath;
    private State state = new State();

    public MomentumTradingStateService(ObjectMapper mapper, PendingOrderRepository orders) {
        this.mapper = mapper;
        this.orders = orders;
    }

    @PostConstruct
    public void init() {
        synchronized (lock) {
            Path file = Path.of(filePath);
            if (Files.exists(file)) {
                try { state = mapper.readValue(file.toFile(), State.class); }
                catch (Exception e) { throw new IllegalStateException("Cannot safely load momentum state: " + file, e); }
            }
            if (state == null) state = new State();
            rollDate();
            normalize();
            recoverFromLedger();
            persist();
        }
    }

    private void normalize() {
        if (state.blacklist == null) state.blacklist = new LinkedHashSet<>();
        if (state.whitelist == null) state.whitelist = new LinkedHashSet<>();
        if (state.reserved == null) state.reserved = new LinkedHashSet<>();
        if (state.selectedToday == null) state.selectedToday = new LinkedHashSet<>();
        if (state.seenCandidates == null) state.seenCandidates = new LinkedHashSet<>();
        if (state.executedSchedules == null) state.executedSchedules = new LinkedHashSet<>();
        if (!Double.isFinite(state.momentumThresholdPercent)
                || state.momentumThresholdPercent <= 0.0 || state.momentumThresholdPercent > 20.0) {
            state.momentumThresholdPercent = 2.0;
        }
        if (state.momentumTopCount < 1 || state.momentumTopCount > 100) {
            state.momentumTopCount = 10;
        }
    }

    private void rollDate() {
        String today = LocalDate.now(ZONE).toString();
        if (!today.equals(state.tradingDate)) {
            boolean g = state.gainersEnabled, l = state.losersEnabled;
            double threshold = state.momentumThresholdPercent;
            int topCount = state.momentumTopCount;
            state = new State();
            state.tradingDate = today;
            state.gainersEnabled = g;
            state.losersEnabled = l;
            state.momentumThresholdPercent = threshold;
            state.momentumTopCount = topCount;
        }
    }

    private void recoverFromLedger() {
        LocalDate today = LocalDate.now(ZONE);
        for (PendingOrder po : orders.findAll()) {
            if (po == null || po.getSide() != TradeSide.BUY || !momentum(po.getStrategy())
                    || po.getCreatedTime() <= 0) continue;
            LocalDate date = Instant.ofEpochMilli(po.getCreatedTime()).atZone(ZONE).toLocalDate();
            if (!today.equals(date)) continue;
            String symbol = normalizeSymbol(po.getSymbol());
            if (symbol == null) continue;
            if (po.getState() == PendingOrderState.COMPLETE && po.getRequestQty() > 0
                    && po.getFilledQty() == po.getRequestQty()) {
                state.blacklist.add(symbol);
                state.selectedToday.add(symbol);
                state.reserved.remove(symbol);
                state.whitelist.remove(symbol);
            } else if (po.getFilledQty() > 0 || po.isOpen()
                    || po.getState() == PendingOrderState.COMPLETE) {
                // An open/partially-filled ledger entry proves the broker accepted
                // the BUY even if the process stopped before persisting its state.
                state.blacklist.add(symbol);
                state.reserved.add(symbol);
                state.selectedToday.add(symbol);
                state.whitelist.remove(symbol);
            }
        }
    }

    public boolean tryReserveSymbol(String raw) {
        String symbol = normalizeSymbol(raw);
        if (symbol == null) return false;
        synchronized (lock) {
            rollDate();
            if (state.blacklist.contains(symbol) || state.reserved.contains(symbol)
                    || state.selectedToday.contains(symbol)) return false;
            state.reserved.add(symbol);
            state.selectedToday.add(symbol);
            persist();
            return true;
        }
    }

    /** Mark a broker-accepted momentum BUY immediately; reservation stays active. */
    public void markBuyAccepted(String raw) {
        String s = normalizeSymbol(raw);
        if (s == null) return;
        synchronized (lock) {
            rollDate();
            state.blacklist.add(s);
            state.reserved.add(s);
            state.selectedToday.add(s);
            state.whitelist.remove(s);
            persist();
        }
    }

    public void markSelectedToday(String raw) {
        String s = normalizeSymbol(raw);
        if (s == null) return;
        synchronized (lock) { state.selectedToday.add(s); persist(); }
    }

    public void releaseReservationAfterNoOrder(String raw) {
        String s = normalizeSymbol(raw);
        if (s == null) return;
        synchronized (lock) { state.reserved.remove(s); state.selectedToday.remove(s); persist(); }
    }

    public void onBuyCompleted(PendingOrder po) {
        if (!isMomentumBuy(po)) return;
        String s = normalizeSymbol(po.getSymbol());
        if (s == null) return;
        synchronized (lock) {
            if (po.isCompleted() && po.getRequestQty() > 0 && po.getFilledQty() == po.getRequestQty()) {
                state.blacklist.add(s);
                state.reserved.remove(s);
                state.selectedToday.add(s);
                state.whitelist.remove(s);
            } else state.reserved.add(s);
            persist();
        }
    }

    public void onBuyTerminal(PendingOrder po, Integer confirmedFilled) {
        if (!isMomentumBuy(po)) return;
        String s = normalizeSymbol(po.getSymbol());
        if (s == null) return;
        synchronized (lock) {
            if (Integer.valueOf(0).equals(confirmedFilled) && po.getFilledQty() == 0
                    && (po.getState() == PendingOrderState.CANCELLED
                    || po.getState() == PendingOrderState.REJECTED)) {
                // Roll back the submission-time blacklist only when this is the
                // only same-day momentum BUY for the symbol and the broker has
                // explicitly confirmed zero cumulative fills.
                if (!hasOtherAcceptedMomentumBuy(s, po)) {
                    state.blacklist.remove(s);
                    state.reserved.remove(s);
                    state.selectedToday.remove(s);
                } else {
                    state.blacklist.add(s);
                    state.reserved.add(s);
                    state.selectedToday.add(s);
                }
            }
            else { state.blacklist.add(s); state.reserved.add(s); state.selectedToday.add(s); }
            persist();
        }
    }

    private boolean hasOtherAcceptedMomentumBuy(String symbol, PendingOrder current) {
        LocalDate today = LocalDate.now(ZONE);
        String currentId = current.getInternalOrderId();
        for (PendingOrder other : orders.findAll()) {
            if (other == null || other == current || other.getSide() != TradeSide.BUY
                    || !momentum(other.getStrategy()) || !symbol.equals(normalizeSymbol(other.getSymbol()))
                    || other.getCreatedTime() <= 0) continue;
            if (currentId != null && currentId.equals(other.getInternalOrderId())) continue;
            LocalDate orderDate = Instant.ofEpochMilli(other.getCreatedTime()).atZone(ZONE).toLocalDate();
            if (today.equals(orderDate) && (other.getFilledQty() > 0 || other.isOpen()
                    || other.getState() == PendingOrderState.COMPLETE)) return true;
        }
        return false;
    }

    public void addBlacklist(String raw) {
        String s = requireSymbol(raw);
        synchronized (lock) {
            state.blacklist.add(s); state.whitelist.remove(s); state.reserved.remove(s);
            state.selectedToday.add(s); persist();
        }
    }

    public void addWhitelist(String raw) {
        String s = requireSymbol(raw);
        synchronized (lock) {
            if (!state.blacklist.contains(s) && !state.reserved.contains(s)
                    && !state.selectedToday.contains(s)) state.whitelist.add(s);
            persist();
        }
    }

    public void removeWhitelist(String raw) {
        String s = normalizeSymbol(raw);
        if (s != null) synchronized (lock) { state.whitelist.remove(s); persist(); }
    }

    public void markCandidatesSeen(Set<String> symbols) {
        synchronized (lock) {
            if (symbols != null) for (String raw : symbols) {
                String s = normalizeSymbol(raw);
                if (s != null) state.seenCandidates.add(s);
            }
            persist();
        }
    }

    public boolean markScheduleExecuted(String id) {
        synchronized (lock) {
            rollDate();
            if (!state.executedSchedules.add(id)) return false;
            persist();
            return true;
        }
    }

    public void setMomentumTopCount(int value) {
        if (value < 1 || value > 100) {
            throw new IllegalArgumentException("Top count must be between 1 and 100");
        }
        synchronized (lock) { state.momentumTopCount = value; persist(); }
    }
    public int getMomentumTopCount() { synchronized (lock) { return state.momentumTopCount; } }

    public void setMomentumThresholdPercent(double value) {
        if (!Double.isFinite(value) || value <= 0.0 || value > 20.0) {
            throw new IllegalArgumentException("Threshold must be greater than 0 and at most 20 percent");
        }
        synchronized (lock) { state.momentumThresholdPercent = value; persist(); }
    }
    public double getMomentumThresholdPercent() { synchronized (lock) { return state.momentumThresholdPercent; } }

    public void setGainersEnabled(boolean v) { synchronized (lock) { state.gainersEnabled=v; persist(); } }
    public void setLosersEnabled(boolean v) { synchronized (lock) { state.losersEnabled=v; persist(); } }
    public boolean isGainersEnabled() { synchronized (lock) { return state.gainersEnabled; } }
    public boolean isLosersEnabled() { synchronized (lock) { return state.losersEnabled; } }
    public boolean isBlacklisted(String s) { synchronized (lock) { return state.blacklist.contains(normalizeSymbol(s)); } }
    public boolean isReserved(String s) { synchronized (lock) { return state.reserved.contains(normalizeSymbol(s)); } }
    public boolean isSelectedToday(String s) { synchronized (lock) { return state.selectedToday.contains(normalizeSymbol(s)); } }
    public boolean hasSeenCandidate(String s) { synchronized (lock) { return state.seenCandidates.contains(normalizeSymbol(s)); } }
    public Set<String> getBlacklist() { synchronized (lock) { return copy(state.blacklist); } }
    public Set<String> getWhitelist() { synchronized (lock) { return copy(state.whitelist); } }
    public Set<String> getReserved() { synchronized (lock) { return copy(state.reserved); } }
    public Set<String> getSelectedToday() { synchronized (lock) { return copy(state.selectedToday); } }
    public Set<String> getExecutedSchedules() { synchronized (lock) { return copy(state.executedSchedules); } }
    public String getTradingDate() { synchronized (lock) { rollDate(); return state.tradingDate; } }

    private boolean isMomentumBuy(PendingOrder po) {
        return po != null && po.getSide() == TradeSide.BUY && momentum(po.getStrategy());
    }
    private boolean momentum(String strategy) { return strategy != null && strategy.startsWith(STRATEGY); }
    private String requireSymbol(String raw) {
        String s = normalizeSymbol(raw);
        if (s == null || !SYMBOL.matcher(s).matches()) throw new IllegalArgumentException("Invalid NSE trading symbol");
        return s;
    }
    private String normalizeSymbol(String raw) {
        if (raw == null) return null;
        String s = raw.trim().toUpperCase(Locale.ROOT);
        if (s.startsWith("NSE:")) s = s.substring(4);
        return s.isEmpty() ? null : s;
    }
    private Set<String> copy(Set<String> s) { return Collections.unmodifiableSet(new LinkedHashSet<>(s)); }

    private void persist() {
        try {
            Path file = Path.of(filePath), parent = file.getParent();
            if (parent != null) Files.createDirectories(parent);
            Path temp = file.resolveSibling(file.getFileName().toString() + ".tmp");
            mapper.writeValue(temp.toFile(), state);
            try { Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE); }
            catch (IOException e) { Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING); }
        } catch (IOException e) { throw new IllegalStateException("Unable to persist momentum state", e); }
    }

    public static class State {
        public String tradingDate;
        public Set<String> blacklist = new LinkedHashSet<>();
        public Set<String> whitelist = new LinkedHashSet<>();
        public Set<String> reserved = new LinkedHashSet<>();
        public Set<String> selectedToday = new LinkedHashSet<>();
        public Set<String> seenCandidates = new LinkedHashSet<>();
        public Set<String> executedSchedules = new LinkedHashSet<>();
        public boolean gainersEnabled = true;
        public boolean losersEnabled = true;
        public double momentumThresholdPercent = 2.0;
        public int momentumTopCount = 10;
    }
}