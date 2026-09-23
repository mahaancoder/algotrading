package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.EntryType;
import com.satyam.trading2.datamodel.TradeRecord;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service to track analytics by entry type (TOP_GAINER, TOP_LOSER, ACCUMULATION)
 * Tracks P&L, order counts, and performance metrics for each entry type
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EntryTypeAnalyticsService {

    private final TradeJournalService tradeJournalService;
    
    // P&L by entry type
    private final Map<EntryType, Double> pnlByEntryType = new ConcurrentHashMap<>();
    
    // Buy order counts (completed)
    private final Map<EntryType, Integer> buyCountsByEntryType = new ConcurrentHashMap<>();
    
    // Closed trade counts
    private final Map<EntryType, Integer> closedCountsByEntryType = new ConcurrentHashMap<>();
    
    // Win counts
    private final Map<EntryType, Integer> winCountsByEntryType = new ConcurrentHashMap<>();

    // ===== NEW: Track conversions to holdings =====
    // Count how many positions of each entry type were converted to holdings
    private final Map<EntryType, Integer> convertedToHoldingCountsByEntryType = new ConcurrentHashMap<>();

    /**
     * Record a completed buy order
     */
    public void recordBuyOrder(EntryType entryType) {
        if (entryType == null) entryType = EntryType.UNKNOWN;
        buyCountsByEntryType.merge(entryType, 1, Integer::sum);
        log.debug("📊 Buy order recorded for {}: total={}", entryType, buyCountsByEntryType.get(entryType));
    }

    /**
     * Record when a position is converted to holding (at 3:15 PM for loss positions)
     */
    public void recordConversionToHolding(EntryType entryType) {
        if (entryType == null) entryType = EntryType.UNKNOWN;
        convertedToHoldingCountsByEntryType.merge(entryType, 1, Integer::sum);
        log.info("📊 Conversion to holding recorded for {}: total={}",
                entryType, convertedToHoldingCountsByEntryType.get(entryType));
    }
    
    /**
     * Record a closed trade (from TradeRecord)
     */
    public void recordClosedTrade(TradeRecord trade) {
        EntryType entryType = trade.getEntryType() != null ? trade.getEntryType() : EntryType.UNKNOWN;
        double pnl = trade.getPnl();
        
        // Update P&L
        pnlByEntryType.merge(entryType, pnl, Double::sum);
        
        // Update closed count
        closedCountsByEntryType.merge(entryType, 1, Integer::sum);
        
        // Update win count
        if (pnl > 0) {
            winCountsByEntryType.merge(entryType, 1, Integer::sum);
        }
        
        log.info("📊 Trade recorded for {}: PnL={}, Total PnL={}, Closed={}, Wins={}", 
                entryType, pnl, 
                pnlByEntryType.get(entryType),
                closedCountsByEntryType.get(entryType),
                winCountsByEntryType.get(entryType));
    }
    
    /**
     * Get analytics summary for all entry types
     */
    public Map<String, EntryTypeStats> getAnalyticsSummary() {
        Map<String, EntryTypeStats> summary = new HashMap<>();

        for (EntryType type : EntryType.values()) {
            EntryTypeStats stats = new EntryTypeStats();
            stats.setEntryType(type.name());
            stats.setTotalPnL(pnlByEntryType.getOrDefault(type, 0.0));
            stats.setBuyOrdersCompleted(buyCountsByEntryType.getOrDefault(type, 0));
            stats.setTradesClosed(closedCountsByEntryType.getOrDefault(type, 0));
            stats.setWins(winCountsByEntryType.getOrDefault(type, 0));
            stats.setConvertedToHoldings(convertedToHoldingCountsByEntryType.getOrDefault(type, 0));

            int closed = stats.getTradesClosed();
            if (closed > 0) {
                stats.setWinRate((stats.getWins() * 100.0) / closed);
                stats.setAvgPnL(stats.getTotalPnL() / closed);
            }

            // Calculate conversion rate: what % of opened positions got converted to holdings
            int totalOpened = stats.getBuyOrdersCompleted();
            if (totalOpened > 0) {
                stats.setConversionRate((stats.getConvertedToHoldings() * 100.0) / totalOpened);
            }

            summary.put(type.name(), stats);
        }

        return summary;
    }
    
    /**
     * Restore analytics from historical trades (called on startup)
     */
    @PostConstruct
    public void restoreFromHistory() {
        log.info("📊 Restoring entry type analytics from trade history...");

        // Clear existing data
        pnlByEntryType.clear();
        closedCountsByEntryType.clear();
        winCountsByEntryType.clear();
        convertedToHoldingCountsByEntryType.clear();

        // Rebuild from all historical trades
        for (TradeRecord trade : tradeJournalService.getAll()) {
            EntryType entryType = trade.getEntryType() != null ? trade.getEntryType() : EntryType.UNKNOWN;
            double pnl = trade.getPnl();

            pnlByEntryType.merge(entryType, pnl, Double::sum);
            closedCountsByEntryType.merge(entryType, 1, Integer::sum);

            if (pnl > 0) {
                winCountsByEntryType.merge(entryType, 1, Integer::sum);
            }

            // Track if this trade was converted to holding before being closed
            if (trade.isWasConvertedToHolding()) {
                convertedToHoldingCountsByEntryType.merge(entryType, 1, Integer::sum);
            }
        }

        log.info("✅ Entry type analytics restored:");
        for (EntryType type : EntryType.values()) {
            log.info("  {}: P&L={}, Closed={}, Wins={}, ConvertedToHolding={}",
                    type,
                    pnlByEntryType.getOrDefault(type, 0.0),
                    closedCountsByEntryType.getOrDefault(type, 0),
                    winCountsByEntryType.getOrDefault(type, 0),
                    convertedToHoldingCountsByEntryType.getOrDefault(type, 0));
        }
    }
    
    /**
     * Statistics for a specific entry type
     */
    @Data
    public static class EntryTypeStats {
        private String entryType;
        private double totalPnL;
        private int buyOrdersCompleted;     // How many buy orders completed (total positions opened)
        private int tradesClosed;           // How many trades fully closed directly
        private int wins;                   // How many profitable trades
        private double winRate;             // Win percentage (wins / closed)
        private double avgPnL;              // Average P&L per trade

        // ===== NEW: Conversion tracking =====
        private int convertedToHoldings;    // How many positions converted to holdings (3:15 PM rule)
        private double conversionRate;      // Conversion rate (converted / opened) %
    }
}

