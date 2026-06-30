package com.satyam.trading2.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Telegram Notification Service
 * ─────────────────────────────────────────────────────────────────────────
 * Sends instant messages to your Telegram phone for every trade event.
 *
 * SETUP (5 minutes):
 *  1. Open Telegram → search @BotFather → /newbot → follow steps
 *  2. Copy the token BotFather gives you → paste into telegram.bot.token
 *  3. Start a chat with your new bot (send any message)
 *  4. Open: https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
 *  5. Find "chat":{"id": 123456789} → paste that into telegram.chat.id
 *  6. Set telegram.enabled=true in application.properties
 *
 * MESSAGES YOU'LL RECEIVE:
 *  🚀 BUY executed
 *  🛑 Stop-loss hit
 *  🎯 Target reached
 *  ⚠️ Convert to positional — action required
 *  📊 Daily summary at 3:30 PM
 *  🚨 Daily loss limit hit — bot stopped
 *  🛡️ Protective put bought
 *  💰 Covered call sold
 */
@Service
public class TelegramNotificationService {

    private static final Logger log = LoggerFactory.getLogger(TelegramNotificationService.class);
    private static final String TELEGRAM_API = "https://api.telegram.org/bot";
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Value("${telegram.enabled:false}")
    private boolean enabled;

    @Value("${telegram.bot.token:}")
    private String botToken;

    @Value("${telegram.chat.id:}")
    private String chatId;

    @Value("${trading.paper.mode:false}")
    private boolean paperMode;

    private final HttpClient http = HttpClient.newHttpClient();

    // ── Trade Events ──────────────────────────────────────────────────────

    public void buyExecuted(String instrument, double price, double sl, double target, int qty,
                             String strategy, boolean isPaper) {
        String mode = isPaper ? "📄 PAPER" : "🚀 LIVE";
        send(String.format(
            "%s BUY EXECUTED\n" +
            "📌 %s\n" +
            "💰 Entry: ₹%.2f × %d shares\n" +
            "🛑 Stop-Loss: ₹%.2f  (risk ₹%.0f)\n" +
            "🎯 Target: ₹%.2f  (reward ₹%.0f)\n" +
            "📊 Strategy: %s\n" +
            "⏰ %s",
            mode, instrument, price, qty,
            sl, (price - sl) * qty,
            target, (target - price) * qty,
            strategy, LocalDateTime.now().format(FMT)
        ));
    }

    public void stopLossHit(String instrument, double entryPrice, double exitPrice, double loss, String strategy) {
        send(String.format(
            "🛑 STOP-LOSS HIT\n" +
            "📌 %s\n" +
            "📉 Entry: ₹%.2f → Exit: ₹%.2f\n" +
            "💸 Loss: ₹%.2f\n" +
            "📊 Strategy: %s\n" +
            "⏰ %s",
            instrument, entryPrice, exitPrice, loss, strategy, LocalDateTime.now().format(FMT)
        ));
    }

    public void targetHit(String instrument, double entryPrice, double exitPrice, double profit, String strategy) {
        send(String.format(
            "🎯 TARGET REACHED!\n" +
            "📌 %s\n" +
            "📈 Entry: ₹%.2f → Exit: ₹%.2f\n" +
            "✅ Profit: ₹%.2f\n" +
            "📊 Strategy: %s\n" +
            "⏰ %s",
            instrument, entryPrice, exitPrice, profit, strategy, LocalDateTime.now().format(FMT)
        ));
    }

    public void convertToPositional(String instrument, double entryPrice, double sl, String strategy, String reason) {
        send(String.format(
            "⚠️ ACTION REQUIRED — CONVERT TO POSITIONAL\n\n" +
            "📌 %s  |  Entry: ₹%.2f\n" +
            "📊 Strategy: %s\n" +
            "💡 Reason: %s\n\n" +
            "👉 Go to Kite → Positions → Convert MIS → CNC\n" +
            "⏰ Must act BEFORE 3:20 PM\n\n" +
            "New Stop-Loss: ₹%.2f (weekly support)\n" +
            "🕐 %s",
            instrument, entryPrice, strategy, reason, sl, LocalDateTime.now().format(FMT)
        ));
    }

    public void dailyLossLimitHit(double loss, double limit) {
        send(String.format(
            "🚨 DAILY LOSS LIMIT REACHED — TRADING STOPPED\n\n" +
            "💸 Today's Loss: ₹%.2f\n" +
            "🔒 Limit: ₹%.2f\n\n" +
            "Bot will not take new trades today.\n" +
            "Review what happened and check open positions in Kite.\n" +
            "⏰ %s",
            Math.abs(loss), limit, LocalDateTime.now().format(FMT)
        ));
    }

    public void weeklyLossLimitHit(double weeklyLoss, double limit) {
        send(String.format(
            "🚨 WEEKLY LOSS LIMIT REACHED\n\n" +
            "💸 Weekly Loss: ₹%.2f\n" +
            "🔒 Limit: ₹%.2f\n\n" +
            "Consider reducing position size next week.\n" +
            "Review strategy performance in dashboard.\n" +
            "⏰ %s",
            Math.abs(weeklyLoss), limit, LocalDateTime.now().format(FMT)
        ));
    }

    public void trailingStopUpdated(String instrument, double newSl, double currentPrice) {
        send(String.format(
            "📍 TRAILING STOP MOVED\n" +
            "📌 %s\n" +
            "Current Price: ₹%.2f\n" +
            "New Stop: ₹%.2f (protecting ₹%.2f profit)\n" +
            "⏰ %s",
            instrument, currentPrice, newSl, currentPrice - newSl, LocalDateTime.now().format(FMT)
        ));
    }

    public void protectivePutBought(String symbol, double strike, double premium, int qty) {
        send(String.format(
            "🛡️ PROTECTIVE PUT BOUGHT\n" +
            "📌 %s  %s Put\n" +
            "Strike: ₹%.0f  |  Premium: ₹%.2f/share\n" +
            "Qty: %d  |  Total Cost: ₹%.0f\n" +
            "📌 Your downside is now capped!\n" +
            "⏰ %s",
            symbol, strike > 0 ? "₹" + (int)strike : "", strike, premium,
            qty, premium * qty, LocalDateTime.now().format(FMT)
        ));
    }

    public void coveredCallSold(String symbol, double strike, double premium, int qty) {
        send(String.format(
            "💰 COVERED CALL SOLD\n" +
            "📌 %s  ₹%.0f Call\n" +
            "Premium Collected: ₹%.2f/share\n" +
            "Qty: %d  |  Total Income: ₹%.0f\n" +
            "📌 Income locked in. Upside capped at ₹%.0f\n" +
            "⏰ %s",
            symbol, strike, premium, qty, premium * qty, strike, LocalDateTime.now().format(FMT)
        ));
    }

    public void dailySummary(double pnl, int total, int wins, int losses,
                              double winRate, String regime, boolean limitHit) {
        String status = pnl >= 0 ? "✅ PROFITABLE DAY" : "❌ LOSING DAY";
        send(String.format(
            "📊 DAILY SUMMARY — %s\n\n" +
            "%s  |  P&L: %s₹%.2f\n\n" +
            "Total Trades: %d  |  ✅ %d  ❌ %d\n" +
            "Win Rate: %.1f%%\n" +
            "Market: %s\n" +
            "%s\n" +
            "⏰ %s",
            LocalDateTime.now().toLocalDate(),
            status, pnl >= 0 ? "+" : "", pnl,
            total, wins, losses, winRate * 100,
            regime,
            limitHit ? "⚠️ Daily limit was hit today." : "✅ Within risk limits.",
            LocalDateTime.now().format(FMT)
        ));
    }

    public void marketRegimeChanged(String oldRegime, String newRegime) {
        String emoji = newRegime.equals("BULL") ? "📈" : newRegime.equals("BEAR") ? "📉" : "↔️";
        send(String.format(
            "%s MARKET REGIME CHANGE\n" +
            "%s → %s\n\n" +
            "Bot is adjusting strategy aggression.\n" +
            "⏰ %s",
            emoji, oldRegime, newRegime, LocalDateTime.now().format(FMT)
        ));
    }

    public void systemAlert(String title, String message) {
        send(String.format("⚙️ SYSTEM: %s\n%s\n⏰ %s", title, message, LocalDateTime.now().format(FMT)));
    }

    // ── Core send method ─────────────────────────────────────────────────

    public void send(String message) {
        if (!enabled) {
            log.info("[TELEGRAM disabled] {}", message.replace("\n", " | "));
            return;
        }
        try {
            String prefix = paperMode ? "📄 [PAPER MODE]\n" : "";
            String encoded = URLEncoder.encode(prefix + message, StandardCharsets.UTF_8);
            String url = TELEGRAM_API + botToken + "/sendMessage?chat_id=" + chatId + "&text=" + encoded + "&parse_mode=HTML";
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (!resp.body().contains("\"ok\":true")) {
                log.warn("Telegram send failed: {}", resp.body());
            }
        } catch (Exception e) {
            log.error("Telegram error: {}", e.getMessage());
        }
    }
}
