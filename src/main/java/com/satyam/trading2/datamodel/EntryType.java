package com.satyam.trading2.datamodel;

/**
 * Enum to track how a position was entered
 * Used for analytics to determine which entry type performs better
 */
public enum EntryType {
    /**
     * Position entered when stock was in top 10 gainers (>= 1% up)
     */
    TOP_GAINER,
    
    /**
     * Position entered when stock was in top 10 losers (<= -2% down)
     */
    TOP_LOSER,
    
    /**
     * Position entered as an accumulation (buying more of existing position)
     */
    ACCUMULATION,
    
    /**
     * Entry type unknown (for legacy data or external positions)
     */
    UNKNOWN
}

