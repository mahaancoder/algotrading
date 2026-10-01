package com.satyam.trading2.service;

/**
 * Cash-flow regime that drives the margin/budget scheduler.
 *
 * HIGH   — aggressive time-slot multipliers; getMargin() returns full net when
 *          (net - collateral) >= 3L.
 * MEDIUM — conservative time-slot multipliers; getMargin() keeps the 3L rule.
 * LOW    — same conservative multipliers as MEDIUM; getMargin() ignores the 3L
 *          rule and always returns (net - collateral).
 */
public enum CashFlowMode {
    HIGH,
    MEDIUM,
    LOW
}
