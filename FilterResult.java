package com.satyam.trading2.strategy;

import com.satyam.trading2.broker.ExecutionRoute;
import com.satyam.trading2.datamodel.TradeSignal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Result of evaluating a TradeSignal against the Group 1 / Group 2 filter rules.
 *
 * Group 1 takes priority: if both groups match, executionRoute = INDSTOCKS.
 * If neither matches, executionRoute = KITE (the fallback broker).
 *
 * The "neither" case maps to Kite, which is also the market-data source —
 * this is by design: Kite is always available as the execution fallback.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FilterResult {

    /** True if the signal matched Group 1 rules (→ INDSTOCKS). */
    private boolean group1Passed;

    /** True if the signal matched Group 2 rules (→ SHOONYA). */
    private boolean group2Passed;

    /** The resolved execution route, with Group 1 priority applied. */
    private ExecutionRoute executionRoute;

    /**
     * Convenience: build a result from the two boolean group flags.
     * Applies priority: group1 wins if both pass; KITE if neither.
     */
    public static FilterResult from(boolean group1Passed, boolean group2Passed) {
        ExecutionRoute route;
        if (group1Passed) {
            route = ExecutionRoute.INDSTOCKS;
        } else if (group2Passed) {
            route = ExecutionRoute.SHOONYA;
        } else {
            route = ExecutionRoute.KITE;
        }
        return FilterResult.builder()
                .group1Passed(group1Passed)
                .group2Passed(group2Passed)
                .executionRoute(route)
                .build();
    }
}
