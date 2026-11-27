# Momentum trailing exit (feature-flagged)

This policy applies only to `Dip-Accumulator-Momentum`. Other strategies retain their existing fixed-target behaviour.

## Strategy parameters

- Activation: first LTP touch at +0.7% from the broker-confirmed average BUY fill price.
- Initial SELL SL-LIMIT trigger: +0.5%.
- Initial SELL limit: 0.05% below the trigger, rounded to the instrument tick.
- Trailing: for each additional +0.1% move from the activation price, raise the trigger by +0.1% of average BUY price.
- The stop only moves upward. Once triggered, trailing stops moving.
- Partial SELL fills are applied by the existing broker event/reconciliation lifecycle. A replacement is permitted only after the old order is terminal and its cumulative fills have been applied to the local position.
- Repricing floor: estimated net break-even. If LTP is below the floor, do not lower the SELL limit; retain the unresolved position and alert.
- EOD: entry cutoff defaults to 14:55 IST; EOD exit management starts at 15:00 IST. Pending momentum intraday BUYs are cancelled first; no SELL is submitted until they are terminal and any fills are reconciled. Eligible momentum positions are excluded from the legacy MIS-to-holding conversion scheduler.
- An unresolved below-break-even position raises an alert before the configured MIS square-off deadline. No automatic delivery conversion or loss-making override is performed.

## Configuration

All controls are deliberately disabled by default:

```properties
momentum.trailing.enabled=false
momentum.trailing.live-enabled=false
momentum.trailing.entry-cutoff=14:55
momentum.trailing.eod-cutoff=15:00
momentum.trailing.mis-squareoff-deadline=15:20
momentum.trailing.poll-ms=500
momentum.trailing.action-interval-ms=1000
momentum.trailing.alert-repeat-ms=60000
momentum.trailing.ltp-stale-ms=5000
```

To collect trailing state without placing broker orders, set `momentum.trailing.enabled=true` and keep `momentum.trailing.live-enabled=false`. In this shadow mode, existing fixed-target and legacy EOD handling remain active; the new service only records the trailing state.

Live order placement requires both flags to be explicitly enabled and a broker-specific estimated all-in round-trip cost percentage, for example:

```properties
momentum.trailing.round-trip-cost-percent.KITE=<configured-estimate>
momentum.trailing.round-trip-cost-percent.SHOONYA=<configured-estimate>
```

The percentage is an estimate used to calculate the break-even floor; it is not a broker-generated exact charges quote. Do not enable live trading until it is calibrated against actual contract notes/charges. Set the MIS square-off deadline per broker; the 15:20 fallback is not a verified universal broker deadline.

## Broker and market-data limitations

- Kite and Shoonya adapters declare SELL SL-LIMIT support. Their live trigger/limit behaviour still requires broker-account verification.
- INDstocks currently does not send a trigger-price field in its place-order payload, so the adapter explicitly does not advertise SELL SL-LIMIT support. When trailing live mode is enabled, INDstocks positions retain the legacy fixed-target and EOD management; the new trailing exit policy is not activated for that broker.
- The current market-data path stores LTP but not best-bid depth. The triggered/EOD limit repricer currently uses LTP as a quote proxy, not the actual best bid. This is one reason live order placement remains disabled by default.
- Stop amendments use broker-confirmed cancel-and-replace because the Kite adapter does not implement a native modify operation for this order. This creates a protection gap during replacement; do not enable live trading until this behaviour is accepted and broker-specific modification support is verified.
- Tick-size rounding is applied before submission. If a valid SELL SL-LIMIT cannot be formed with the limit at/above the configured break-even floor, the service refuses the order and alerts.
- State is persisted atomically to `/home/ec2-user/momentum_trailing_state.json` by default. An unreadable state file fails startup rather than silently resetting a protective stop.

## Operational safety

- Do not merge/enable for live trading solely because the code compiles.
- Validate SL-LIMIT placement, trigger activation, cancel/replace, partial fills, broker inventory semantics, restart recovery, and EOD square-off behaviour separately for each broker/account.
- The service shares the existing position-level SELL lifecycle lock and BrokerOrderRouter inventory/reservation checks. It does not bypass the short-selling guard.
- Tests/build were not run for this change, per the project's standing instruction.
