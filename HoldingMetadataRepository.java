package com.satyam.trading2.repository;

import com.satyam.trading2.datamodel.EntryType;
import com.satyam.trading2.datamodel.HoldingMetadata;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Repository for persisting holding metadata across application restarts.
 * 
 * Purpose: Broker APIs don't store our custom metadata (entryType, strategy, etc.)
 * When we fetch holdings from broker, we need to restore this metadata from our local store.
 * 
 * Storage: CSV file (holding_metadata.csv)
 * Format: symbol,strategy,entryType,conversionTime,wasConvertedToHolding
 * 
 * Lifecycle:
 * - Save: When position is converted from INTRADAY to HOLDING
 * - Load: At application startup
 * - Lookup: When fetching holdings from broker to enrich them
 * - Delete: When holding is sold and position is closed
 */
@Slf4j
@Component
public class HoldingMetadataRepository {
    
    private final Map<String, HoldingMetadata> metadataMap = new ConcurrentHashMap<>();
    private final String FILE_PATH = "holding_metadata.csv";
    private final Object lock = new Object();
    
    @PostConstruct
    public void init() {
        loadFromFile();
    }
    
    /**
     * Create composite key from symbol and strategy
     */
    private String makeKey(String symbol, String strategy) {
        return symbol + "_" + strategy;
    }
    
    /**
     * Save holding metadata
     */
    public void save(HoldingMetadata metadata) {
        synchronized (lock) {
            metadataMap.put(makeKey(metadata.getSymbol(), metadata.getStrategy()), metadata);
            rewriteFile();
            log.info("💾 Saved holding metadata: {} - {} (EntryType: {})", 
                    metadata.getSymbol(), metadata.getStrategy(), metadata.getEntryType());
        }
    }
    
    /**
     * Get metadata for a specific holding
     */
    public HoldingMetadata get(String symbol, String strategy) {
        return metadataMap.get(makeKey(symbol, strategy));
    }
    
    /**
     * Remove metadata when holding is sold
     */
    public void remove(String symbol, String strategy) {
        synchronized (lock) {
            HoldingMetadata removed = metadataMap.remove(makeKey(symbol, strategy));
            if (removed != null) {
                rewriteFile();
                log.info("🗑️ Removed holding metadata: {} - {}", symbol, strategy);
            }
        }
    }
    
    /**
     * Get all metadata (useful for debugging)
     */
    public Map<String, HoldingMetadata> getAll() {
        return new ConcurrentHashMap<>(metadataMap);
    }
    
    /**
     * Load metadata from file at startup
     */
    private void loadFromFile() {
        File file = new File(FILE_PATH);
        if (!file.exists()) {
            log.info("📂 Holding metadata file does not exist yet: {}", FILE_PATH);
            return;
        }
        
        try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            int lineNumber = 0;
            int loadedCount = 0;
            
            while ((line = br.readLine()) != null) {
                lineNumber++;
                
                // Skip header line
                if (lineNumber == 1 && line.startsWith("symbol,")) {
                    continue;
                }
                
                try {
                    HoldingMetadata metadata = parseLine(line);
                    if (metadata != null) {
                        metadataMap.put(makeKey(metadata.getSymbol(), metadata.getStrategy()), metadata);
                        loadedCount++;
                    }
                } catch (Exception e) {
                    log.warn("⚠️ Failed to parse holding metadata at line {}: {}", lineNumber, line);
                    log.warn("   Error: {}", e.getMessage());
                }
            }
            
            log.info("✅ Loaded {} holding metadata entries from {}", loadedCount, FILE_PATH);
            
        } catch (IOException e) {
            log.error("❌ Error loading holding metadata from file: {}", e.getMessage());
        }
    }
    
    /**
     * Rewrite entire file (similar to DipAccumulatorService pattern)
     */
    private void rewriteFile() {
        try (FileWriter fw = new FileWriter(FILE_PATH, false)) {
            // Write header
            fw.write("symbol,strategy,entryType,conversionTime,wasConvertedToHolding\n");
            
            // Write all metadata entries
            for (HoldingMetadata metadata : metadataMap.values()) {
                fw.write(toCSVLine(metadata));
            }
            
            fw.flush();
            
        } catch (IOException e) {
            log.error("❌ Error writing holding metadata to file: {}", e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Parse CSV line to HoldingMetadata
     * Format: symbol,strategy,entryType,conversionTime,wasConvertedToHolding
     */
    private HoldingMetadata parseLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return null;
        }
        
        String[] parts = line.split(",", -1); // -1 to include empty strings
        if (parts.length < 5) {
            log.warn("⚠️ Invalid holding metadata line (expected 5 fields): {}", line);
            return null;
        }
        
        try {
            HoldingMetadata metadata = new HoldingMetadata();
            metadata.setSymbol(parts[0].trim());
            metadata.setStrategy(parts[1].trim());
            
            // Parse EntryType (handle UNKNOWN or missing values)
            String entryTypeStr = parts[2].trim();
            if (entryTypeStr.isEmpty()) {
                metadata.setEntryType(EntryType.UNKNOWN);
            } else {
                metadata.setEntryType(EntryType.valueOf(entryTypeStr));
            }
            
            // Parse conversionTime (may be null/empty)
            String conversionTimeStr = parts[3].trim();
            if (!conversionTimeStr.isEmpty()) {
                metadata.setConversionTime(Long.parseLong(conversionTimeStr));
            }
            
            // Parse wasConvertedToHolding
            metadata.setWasConvertedToHolding(Boolean.parseBoolean(parts[4].trim()));
            
            return metadata;
            
        } catch (Exception e) {
            log.warn("⚠️ Error parsing holding metadata line: {}", line);
            log.warn("   Exception: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Convert HoldingMetadata to CSV line
     * Format: symbol,strategy,entryType,conversionTime,wasConvertedToHolding
     */
    private String toCSVLine(HoldingMetadata metadata) {
        return String.format("%s,%s,%s,%s,%s\n",
                metadata.getSymbol(),
                metadata.getStrategy(),
                metadata.getEntryType() != null ? metadata.getEntryType() : EntryType.UNKNOWN,
                metadata.getConversionTime() != null ? metadata.getConversionTime() : "",
                metadata.isWasConvertedToHolding()
        );
    }
}

