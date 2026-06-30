package com.satyam.trading2;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Kite Auto Trader Pro — v2.0
 * ─────────────────────────────────────────────────────────────────────────
 * A professional-grade automated trading system for Rs12 Lakh capital.
 *
 * WHAT'S INCLUDED:
 *  ✅ 7 Equity strategies  (EMA Crossover, EMA Pullback, Range Breakout,
 *                           Candlestick Reversal, VWAP Support,
 *                           RSI Oversold, ORB + Flag)
 *  ✅ 3 F&O strategies     (Protective Put, Covered Call, Bull Call Spread)
 *  ✅ Multi-instrument     (trade up to 5 stocks simultaneously)
 *  ✅ Confluence voting    (2+ strategies must agree to execute)
 *  ✅ Portfolio risk mgmt  (daily/weekly loss limits, max positions, sector guard)
 *  ✅ Trailing stop-loss   (break-even, profit lock, trail at 3R)
 *  ✅ Market regime        (BULL/BEAR/RANGE detection via Nifty EMA200)
 *  ✅ Database persistence (H2 — crash-safe, trade history, analytics)
 *  ✅ Telegram alerts      (instant phone notifications for every event)
 *  ✅ GTT orders           (overnight protection for positional holds)
 *  ✅ WebSocket streaming  (real-time tick data instead of polling)
 *  ✅ Backtesting engine   (test strategies on 60 days of 1-min candles)
 *  ✅ Performance analytics (win rate, strategy breakdown, Kelly sizing)
 *  ✅ Paper trading mode   (simulate everything before going live)
 *  ✅ Pro dashboard        (live indicators, trade log, controls)
 *
 * HOW TO START:
 *  1. Set kite.api.key + kite.api.secret in application.properties
 *  2. Set trading.paper.mode=true (START HERE — paper trade for 4 weeks)
 *  3. Set up Telegram bot (optional but highly recommended for Rs12L)
 *  4. Run this class in IntelliJ
 *  5. Open http://localhost:8080 → Login with Zerodha
 *  6. Watch the logs and Telegram for trade signals
 *
 * CAPITAL STRUCTURE (Rs12 Lakhs):
 *  Rs 3,00,000  → Intraday (trading.capital.intraday)
 *  Rs 4,00,000  → Positional/swing holds
 *  Rs 3,00,000  → F&O margin (options hedging)
 *  Rs 2,00,000  → Emergency buffer (never touch)
 */
@SpringBootApplication
@EnableScheduling
public class KiteTraderApplication {

    public static void main(String[] args) {
        SpringApplication.run(KiteTraderApplication.class, args);

//        System.out.println("KITE AUTO TRADER PRO  v2.0  ✅ Started");
    }
}
