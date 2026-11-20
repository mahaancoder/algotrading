# Pre-market stock risk gate

## Placement

`PreMarketStockRiskService` is invoked by `MomentumTradingCoordinator.executeBatch()`
before the batch reservation lock is acquired. It applies to automatically selected
gainers/losers and manually whitelisted momentum candidates. A rejected symbol is
marked selected for the trading day, so repeated scheduler refreshes do not keep
attempting the same rejected entry.

The service reads completed daily candles only. Its historical-data query ends at
23:59:59 IST on the previous calendar day; today's developing daily candle is never
used. Per-symbol results are cached for the trading date. Historical requests are
serialized and spaced by at least 400 ms.

## Initial technical rules

| Status | Rule |
|---|---|
| BLOCK | Three consecutive red daily sessions and 3-session return <= -6% |
| BLOCK | 5-session return <= -8% |
| CAUTION | At least two consecutive red sessions and 3-session return <= -4% |
| CAUTION | 5-session return <= -5% |
| CAUTION | 5-session return underperforms Nifty 50 by at least 3 percentage points |
| NORMAL | No configured technical rule is triggered |
| UNKNOWN | Missing instrument token, unauthenticated Kite session, insufficient candles, or historical API failure |

Both BLOCK and CAUTION are excluded from momentum BUY execution. UNKNOWN is also
excluded by default. This is deliberately conservative because the service cannot
prove a candidate is safe when its historical data is unavailable.

Thresholds are initial guardrails, not statistically optimized parameters. They
must be backtested against the trade-entry metrics and actual MAE/MFE before tuning.

## News coverage

There is no verified live news provider wired into this service yet. The dashboard
assessment reports `newsStatus=NOT_CONFIGURED` unless an operator-maintained override
is configured. Do not interpret that status as evidence that no adverse news exists.

Optional comma-separated operator overrides:

```properties
premarket.risk.news-blocked-symbols=
premarket.risk.news-caution-symbols=
```

A blocked override returns BLOCK; a caution override returns CAUTION. These are
manual controls, not automated news classification. A timestamp-aware NSE/BSE
filing and news provider should be connected before claiming event-risk screening.

## Configuration

```properties
premarket.risk.block-on-unavailable=true
premarket.risk.news-blocked-symbols=
premarket.risk.news-caution-symbols=
```

Keep `block-on-unavailable=true` for live trading. Setting it to false permits
automatic BUYs when the historical risk assessment is UNKNOWN and is not recommended
until data availability is operationally reliable.

## Operational limitations

- This first version evaluates the momentum batch candidates (normally up to 20),
  not every instrument in the Nifty 500 universe.
- The service currently uses daily close history; it does not yet use abnormal
  delivery/volume, promoter transaction filings, block-deal feed, or opening-range
  failure. Those require separate validated data sources.
- A 9:15 pre-market decision cannot know that a stock will reverse later in the
  session. Opening-range/VWAP failure should be a separate live intraday gate after
  enough candles have formed.
