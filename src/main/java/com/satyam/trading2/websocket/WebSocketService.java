package com.satyam.trading2.websocket;

import com.satyam.trading2.application.dto.PositionDto;
import com.satyam.trading2.datamodel.*;
import com.satyam.trading2.helpers.LatestPriceHelper;
import com.satyam.trading2.infrastructure.messaging.BroadcastService;
import com.satyam.trading2.service.*;
import com.satyam.trading2.strategy.StrategyEngine;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.zerodhatech.ticker.KiteTicker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import static com.satyam.trading2.datamodel.Nifty500Stocks.getInstrumentFromToken;
import static com.satyam.trading2.datamodel.Position.PositionType.HOLDING;
import static com.satyam.trading2.helpers.LatestPriceHelper.getLatestPrice;
import static com.satyam.trading2.risk.Exits.isExitedToday;
import static com.satyam.trading2.risk.Exits.isSymbolExitedToday;

@Service
@RequiredArgsConstructor
public class WebSocketService {

    private final CandleAggregator candleAggregator;
    private final MarketContextBuilder marketContextBuilder;
    private final StrategyEngine strategyEngine;
    private final ExecutionEngine executionEngine;
    private final BroadcastService broadcastService;
    private final TradeMetricsCollector tradeMetricsCollector;
    private final com.satyam.trading2.domain.service.PositionManager positionManager;
    private final KillSwitchService killSwitchService;


    public void startTicker(String apiKey, String accessToken) {

        KiteTicker ticker = new KiteTicker(accessToken, apiKey);

        ticker.setOnConnectedListener(() -> {
            System.out.println("✅ WebSocket Connected!");

            ArrayList<Long> tokens =
                    new ArrayList<>(Nifty500Stocks.Nifty500tokenToInstrument.keySet());

            System.out.println("📡 Subscribing to " + tokens.size() + " instruments...");
            ticker.subscribe(tokens);
            ticker.setMode(tokens, KiteTicker.modeFull);
            System.out.println("✅ Subscribed to " + tokens.size() + " instruments");
        });

        ticker.setOnTickerArrivalListener(ticks -> {
            // ═══════════════════════════════════════════════════════════════════════
            // 🛑 KILL SWITCH - HIGHEST PRIORITY CHECK
            // If kill switch is active, skip ALL tick processing
            // This prevents: BUY signals, SELL signals, target updates, ATR checks
            // ═══════════════════════════════════════════════════════════════════════
            if (killSwitchService.isActive()) {
                // Silently return - don't process any ticks when kill switch is active
                // Price updates still happen via updatePriceEverywhere for dashboard display
                ticks.parallelStream().forEach(tick -> {
                    Instrument instrument = getInstrumentFromToken(tick.getInstrumentToken());
                    String symbol = instrument.getTradingSymbol();
                    double price = tick.getLastTradedPrice();
                    // Only update prices for dashboard - no trading logic
                    updatePriceEverywhere(symbol, price);
                });
                return;
            }

            ticks.parallelStream().forEach(tick -> {
                Instrument instrument = getInstrumentFromToken(tick.getInstrumentToken());
                String symbol = instrument.getTradingSymbol();
                Nifty500Stocks.tradingSymbols.add(symbol);
                if (symbol == null || isSymbolExitedToday(symbol)) return;

                double price = tick.getLastTradedPrice();
                double vwap = tick.getAverageTradePrice(); // Get VWAP
                long volume = tick.getVolumeTradedToday(); // Get cumulative volume for the day
                try {
                    updatePriceEverywhere(symbol, price);
                    updateVwap(symbol, vwap);

                    candleAggregator.onTick(symbol, price, volume);

                    // Check ATR target for all open positions of this symbol
                    checkATRTargetForSymbol(symbol, price);

                    MarketContext ctx = marketContextBuilder.build(symbol, tick);
                    if (ctx == null) return;

                    List<TradeSignal> signals = strategyEngine.evaluate(ctx);

                    if (signals != null && !signals.isEmpty()) {
                        for (TradeSignal signal : signals) {
                            executionEngine.processSignal(signal, ctx);
                        }
                    }
                } catch (Exception e) {
                    // Log but don't crash the tick processing
                    System.out.println("❌ Error processing tick for " + symbol + ": " + e.getMessage());
                    e.printStackTrace();
                }
            });
        });

        ticker.connect();
    }

    private void updatePriceEverywhere(String symbol, double price) {
        LatestPriceHelper.updatePrice(symbol, price);
        StockSelectionService.updateLtp(symbol, price);
        TickWindow.addTickWindow(symbol, price);
        broadcastService.DoBroadcashOnEachTickReceiving(symbol, price);
    }

    private void updateVwap(String symbol, double vwap) {
        com.satyam.trading2.helpers.LatestVwapHelper.updateVwap(symbol, vwap);
    }

    /**
     * Check if the current price touches ATR target for any open positions of this symbol
     * This is called for every tick received via websocket
     */
    private void checkATRTargetForSymbol(String symbol, double price) {
        try {
            // Get all positions for this symbol (across all strategies)
            Map<String, Position> strategyPositions = positionManager.getPositionsForSymbol(symbol);

            if (strategyPositions.isEmpty()) return;

            // Check ATR target for each strategy's position
            for (Map.Entry<String, Position> entry : strategyPositions.entrySet()) {
                String strategy = entry.getKey();
                Position position = entry.getValue();

                // Only check for open intraday positions (not holdings)
                if (position.isOpen() && position.getPositionType() == Position.PositionType.INTRADAY) {
                    tradeMetricsCollector.checkAndUpdateATRTarget(symbol, strategy, price);
                }
            }
        } catch (Exception e) {
            // Silent fail - don't disrupt tick processing
        }
    }


}