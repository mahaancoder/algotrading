package com.satyam.trading2.datamodel;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Metadata for holdings that need to be persisted locally.
 * 
 * Since holdings are always fetched fresh from the broker, we lose metadata like entryType.
 * This class stores that metadata so it can be restored when holdings are re-fetched.
 * 
 * Created when: A position is converted from INTRADAY to HOLDING
 * Used when: Holdings are fetched from broker to enrich them with entryType
 * Deleted when: The holding is sold and position is closed
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HoldingMetadata {
    
    private String symbol;
    private String strategy;
    
    // Analytics: How this position was originally entered
    private EntryType entryType;
    
    // When was this position converted from intraday to holding
    private Long conversionTime;
    
    // Additional metadata that might be useful
    private boolean wasConvertedToHolding;  // True if converted from intraday, false if directly bought as holding
    
    /**
     * Create metadata from a Position that's being converted to holding
     */
    public static HoldingMetadata fromPosition(Position position) {
        return new HoldingMetadata(
            position.getSymbol(),
            position.getStrategy(),
            position.getEntryType() != null ? position.getEntryType() : EntryType.UNKNOWN,
            position.getConversionTime(),
            position.isConvertedToHolding()
        );
    }
    
    /**
     * Enrich a holding Position with this metadata
     */
    public void enrichPosition(Position holding) {
        holding.setEntryType(this.entryType);
        holding.setConversionTime(this.conversionTime);
        holding.setConvertedToHolding(this.wasConvertedToHolding);
    }
}

