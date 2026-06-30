package com.satyam.trading2.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyam.trading2.datamodel.EntryType;
import com.satyam.trading2.datamodel.TradeEvent;
import com.satyam.trading2.datamodel.TradeRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.supercsv.cellprocessor.Trim;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@RequiredArgsConstructor
public class TradeJournalService {

    // ⚠️ MEMORY FIX: Limit in-memory storage to recent items only
    private static final int MAX_EVENTS_IN_MEMORY = 1000;  // Keep last 1000 events
    private static final int MAX_TRADES_IN_MEMORY = 500;   // Keep last 500 trades

    private final List<TradeEvent> events = new CopyOnWriteArrayList<>();
    private final String EVENT_FILE = "/home/ec2-user/trade_events.csv";
    private static final String FILE = "/home/ec2-user/trades.csv";

    private final com.satyam.trading2.domain.service.PnLCalculator pnlCalculator;

    private final List<TradeRecord> trades = new CopyOnWriteArrayList<>();
    private final ObjectMapper mapper = new ObjectMapper();
//    private final String FILE = "trades.json";

    // -------------------------
    // SAVE
    // -------------------------

    public synchronized void saveTrade(TradeRecord t) {
        // Add to in-memory list
        trades.add(t);

        // ⚠️ MEMORY FIX: Trim list if it exceeds max size
        if (trades.size() > MAX_TRADES_IN_MEMORY) {
            trades.remove(0); // Remove oldest
            System.out.println("🧹 Trimmed trades list to " + MAX_TRADES_IN_MEMORY + " (removed oldest)");
        }

        System.out.println("💾 Saving trade to CSV: " + t.getInstrument() + " " + t.getStrategyName() +
                          " Entry:" + t.getEntryPrice() + " Exit:" + t.getExitPrice() +
                          " Qty:" + t.getQuantity() + " P&L:" + t.getPnl() +
                          " EntryType:" + (t.getEntryType() != null ? t.getEntryType() : "UNKNOWN"));

        // Persist to file with analytics fields (entryType, wasConvertedToHolding, holdingDuration)
        try (FileWriter fw = new FileWriter(FILE, true)) {
            String line = t.getInstrument() + "," +
                         t.getStrategyName() + "," +
                         t.getEntryPrice() + "," +
                         t.getExitPrice() + "," +
                         t.getQuantity() + "," +
                         t.getPnl() + "," +
                         t.getTimestamp() + "," +
                         (t.getEntryType() != null ? t.getEntryType() : "UNKNOWN") + "," +
                         t.isWasConvertedToHolding() + "," +
                         (t.getHoldingDuration() != null ? t.getHoldingDuration() : "") + "\n";
            fw.write(line);
            fw.flush(); // Force write to disk
            System.out.println("✅ Trade saved to " + FILE);
        } catch (Exception e) {
            System.err.println("❌ ERROR saving trade to CSV file: " + FILE);
            System.err.println("   Exception: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace();
        }
    }

    // -------------------------
    // LOAD ON STARTUP
    // -------------------------

    @PostConstruct
    public void loadHistory() {
        File file = new File(FILE);
        if (!file.exists()) {
            System.out.println("No trades.csv file found - starting fresh");
            return;
        }

        // ⚠️ MEMORY FIX: Load ALL trades but only keep last N in memory
        List<TradeRecord> allTrades = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("symbol")) continue;
                String[] p = line.split(",");
                if (p.length < 6) {
                    continue;
                }
                String symbol = p[0].trim();
                String strategy = p[1].trim();
                double entryPrice = Double.parseDouble(p[2].trim());
                double exitPrice = Double.parseDouble(p[3].trim());
                int quantity = Integer.parseInt(p[4].trim());
                double pnl = Double.parseDouble(p[5].trim());

                // Handle multiple formats:
                // Old format (6 cols): symbol,strategy,entry,exit,qty,pnl
                // Medium format (7 cols): ...,timestamp
                // New format (10 cols): ...,timestamp,entryType,wasConverted,holdingDuration
                long timestamp = (p.length >= 7 && !p[6].trim().isEmpty()) ? Long.parseLong(p[6].trim()) : System.currentTimeMillis();

                EntryType entryType = EntryType.UNKNOWN;
                boolean wasConverted = false;
                Long holdingDuration = null;

                if (p.length >= 8 && !p[7].trim().isEmpty()) {
                    try {
                        entryType = EntryType.valueOf(p[7].trim());
                    } catch (IllegalArgumentException e) {
                        entryType = EntryType.UNKNOWN;
                    }
                }

                if (p.length >= 9 && !p[8].trim().isEmpty()) {
                    wasConverted = Boolean.parseBoolean(p[8].trim());
                }

                if (p.length >= 10 && !p[9].trim().isEmpty()) {
                    holdingDuration = Long.parseLong(p[9].trim());
                }

                TradeRecord t = new TradeRecord(symbol, strategy, entryPrice, exitPrice, quantity, pnl, timestamp,
                                               entryType, wasConverted, holdingDuration);
                allTrades.add(t);

                // Restore strategy P&L to PnLCalculator
                pnlCalculator.restoreStrategyPnL(strategy, pnl);
                pnlCalculator.restoreTotalPnL(pnl);

                count++;
            }

            // ⚠️ MEMORY FIX: Only keep last MAX_TRADES_IN_MEMORY trades in memory
            int startIndex = Math.max(0, allTrades.size() - MAX_TRADES_IN_MEMORY);
            trades.addAll(allTrades.subList(startIndex, allTrades.size()));

            System.out.println("✅ Loaded " + count + " trades from CSV");
            System.out.println("📊 Kept last " + trades.size() + " trades in memory (saved " + (count - trades.size()) + " from memory)");
            System.out.println("✅ Restored strategy P&L: " + pnlCalculator.getAllStrategyPnLs());
            System.out.println("✅ Total P&L restored: ₹" + pnlCalculator.getTotalPnL());
        } catch (Exception e) {
            System.out.println("❌ Error loading trade history: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public synchronized void saveEvent(TradeEvent e){
        events.add(e);

        // ⚠️ MEMORY FIX: Trim list if it exceeds max size
        if (events.size() > MAX_EVENTS_IN_MEMORY) {
            events.remove(0); // Remove oldest
            System.out.println("🧹 Trimmed events list to " + MAX_EVENTS_IN_MEMORY + " (removed oldest)");
        }

        System.out.println("💾 Saving event to CSV: " + e.getType() + " " + e.getSymbol() + " " +
                          e.getStrategy() + " Price:" + e.getPrice() + " Qty:" + e.getQty() + " P&L:" + e.getPnl() +
                          " EntryType:" + (e.getEntryType() != null ? e.getEntryType() : ""));

        try(FileWriter fw = new FileWriter(EVENT_FILE, true)){
            String line = e.getTime() + "," +
                         e.getSymbol() + "," +
                         e.getStrategy() + "," +
                         e.getType() + "," +
                         e.getPrice() + "," +
                         e.getQty() + "," +
                         e.getPnl() + "," +
                         (e.getEntryType() != null ? e.getEntryType() : "") + "\n";
            fw.write(line);
            fw.flush(); // Force write to disk
        }catch(Exception ex){
            System.err.println("❌ ERROR saving event to CSV file: " + EVENT_FILE);
            System.err.println("   Exception: " + ex.getClass().getName() + " - " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    public void sendHistoricalTrades(WebSocketSession session) throws Exception {
        File file = new File(FILE);
        if (!file.exists()) {
            return;
        }
        try (
                BufferedReader br = new BufferedReader(new FileReader(file))
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("symbol")) continue;
                String[] p = line.split(",");
                if (p.length < 6) continue;

                // Handle both old format (6 columns) and new format (7 columns with timestamp)
                String timestamp = (p.length >= 7) ? p[6] : String.valueOf(System.currentTimeMillis());

                session.sendMessage(
                        new TextMessage(
                                "{ \"type\":\"TRADE\", " +
                                        "\"symbol\":\"" + p[0] + "\"," +
                                        "\"strategy\":\"" + p[1] + "\"," +
                                        "\"entry\":" + p[2] + "," +
                                        "\"exit\":" + p[3] + "," +
                                        "\"qty\":" + p[4] + "," +
                                        "\"pnl\":" + p[5] + "," +
                                        "\"time\":" + timestamp +
                                        "}"
                        )
                );
            }
        }
    }

    public List<TradeRecord> getAll() {
        return trades;
    }

    public List<TradeEvent> getAllEvents() {
        return events;
    }

    @PostConstruct
    public void loadEvents(){
        File file = new File(EVENT_FILE);
        if(!file.exists()) {
            System.out.println("No trade_events.csv file found - starting fresh");
            return;
        }

        // ⚠️ MEMORY FIX: Load ALL events but only keep last N in memory
        List<TradeEvent> allEvents = new ArrayList<>();

        try(BufferedReader br = new BufferedReader(new FileReader(file))){
            String line;
            while((line=br.readLine())!=null){
                String[] p = line.split(",");
                if(p.length < 7) continue;
                TradeEvent e = new TradeEvent();
                e.setTime(Long.parseLong(p[0]));
                e.setSymbol(p[1]);
                e.setStrategy(p[2]);
                e.setType(p[3]);
                e.setPrice(Double.parseDouble(p[4]));
                e.setQty(Integer.parseInt(p[5]));
                e.setPnl(Double.parseDouble(p[6]));

                // Handle new format with entryType (8th column)
                if (p.length >= 8 && !p[7].trim().isEmpty()) {
                    try {
                        e.setEntryType(EntryType.valueOf(p[7].trim()));
                    } catch (IllegalArgumentException ex) {
                        e.setEntryType(null);
                    }
                }

                allEvents.add(e);
            }

            // ⚠️ MEMORY FIX: Only keep last MAX_EVENTS_IN_MEMORY events in memory
            int startIndex = Math.max(0, allEvents.size() - MAX_EVENTS_IN_MEMORY);
            events.addAll(allEvents.subList(startIndex, allEvents.size()));

            System.out.println("✅ Loaded " + allEvents.size() + " total trade events");
            System.out.println("📊 Kept last " + events.size() + " events in memory (saved " + (allEvents.size() - events.size()) + " from memory)");
        }catch(Exception e){
            System.out.println("❌ Error loading trade events: " + e.getMessage());
            e.printStackTrace();
        }
    }
}