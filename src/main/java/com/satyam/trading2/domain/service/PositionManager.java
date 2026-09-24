package com.satyam.trading2.domain.service;

import com.satyam.trading2.datamodel.Position;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


@Slf4j
@Service
public class PositionManager {

    // symbol -> (broker::strategy) -> position
    // The inner map key changed from plain strategy to broker::strategy so that
    // the same symbol can exist at multiple brokers independently.
    // Old callers pass strategy only → broker defaults to KITE → key is "KITE::strategy".
    private final Map<String, Map<String, Position>> positionMap = new ConcurrentHashMap<>();

    /** Build the inner-map key from broker + strategy. */
    private String makeKey(String broker, String strategy) {
        return (broker != null ? broker : "KITE") + "::" + strategy;
    }

    /** Extract broker from a key (for debugging / migration). */
    private String brokerFromKey(String key) {
        int idx = key.indexOf("::");
        return idx >= 0 ? key.substring(0, idx) : "KITE";
    }


    public void addPosition(Position position) {
        String broker = position.getBroker() != null ? position.getBroker() : "KITE";
        positionMap
            .computeIfAbsent(position.getSymbol(), k -> new ConcurrentHashMap<>())
            .put(makeKey(broker, position.getStrategy()), position);
    }

    /**
     * Add or update a position (overload with explicit symbol and strategy)
     * If position already exists for the strategy, adds quantity and averages entry price.
     * Defaults broker to KITE for backward compat.
     */
    public void addPosition(String symbol, String strategy, Position position) {
        addPosition(symbol, strategy, position, position.getBroker());
    }

    /** Broker-aware overload — the canonical form new code should use. */
    public void addPosition(String symbol, String strategy, Position position, String broker) {
        String b = broker != null ? broker : (position.getBroker() != null ? position.getBroker() : "KITE");
        // Ensure the position object carries its broker for downstream consumers
        if (position.getBroker() == null) {
            position.setBroker(b);
        }
        Map<String, Position> strategyMap = positionMap.computeIfAbsent(symbol, k -> new ConcurrentHashMap<>());
        String key = makeKey(b, strategy);

        Position existingPosition = strategyMap.get(key);

        if (existingPosition != null && existingPosition.isOpen()) {
            // Position already exists - add quantity and average entry price
            int oldQty = existingPosition.getTotalQuantity();
            double oldEntry = existingPosition.getAveragePrice();
            int newQty = position.getTotalQuantity();
            double newEntry = position.getAveragePrice();

            int totalQty = oldQty + newQty;
            double avgEntry = ((oldEntry * oldQty) + (newEntry * newQty)) / totalQty;

            existingPosition.setTotalQuantity(totalQty);
            existingPosition.setAveragePrice(avgEntry);

        } else {
            // New position or replacing closed position
            strategyMap.put(key, position);
        }
    }


    // ─────────────────────────────────────────────────────────────────────
    // getPosition — backward compat (broker defaults to KITE)
    // ─────────────────────────────────────────────────────────────────────
    public Position getPosition(String symbol, String strategy) {
        return getPosition(symbol, strategy, "KITE");
    }

    /** Broker-aware overload — canonical form. */
    public Position getPosition(String symbol, String strategy, String broker) {
        Map<String, Position> strategyMap = positionMap.get(symbol);
        return strategyMap != null ? strategyMap.get(makeKey(broker, strategy)) : null;
    }


    public Map<String, Position> getPositionsForSymbol(String symbol) {
        return positionMap.getOrDefault(symbol, new ConcurrentHashMap<>());
    }

    public void updatePosition(Position position) {
        addPosition(position); // Same as add since we use maps
    }

    // ─────────────────────────────────────────────────────────────────────
    // removePosition — backward compat (broker defaults to KITE)
    // ─────────────────────────────────────────────────────────────────────
    public void removePosition(String symbol, String strategy) {
        removePosition(symbol, strategy, "KITE");
    }

    /** Broker-aware overload — canonical form. */
    public void removePosition(String symbol, String strategy, String broker) {
        Map<String, Position> strategyMap = positionMap.get(symbol);
        if (strategyMap != null) {
            strategyMap.remove(makeKey(broker, strategy));
            log.debug("Position removed: {} - {} - {}", symbol, strategy, broker);

            // Clean up empty symbol maps
            if (strategyMap.isEmpty()) {
                positionMap.remove(symbol);
            }
        }
    }

    public int getOpenPositionCount() {
        return (int) positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && !p.isExitProcessed())
            .count();
    }
    

    public List<Position> getAllOpenPositions() {
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && !p.isExitProcessed())
            .collect(Collectors.toList());
    }
    

    public List<Position> getAllHoldings() {
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && p.getPositionType() == Position.PositionType.HOLDING)
            .collect(Collectors.toList());
    }

    public List<Position> getAllIntradayPositions() {
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && p.getPositionType() == Position.PositionType.INTRADAY)
            .collect(Collectors.toList());
    }

    public double getCurrentCapitalUsed() {
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && !p.isExitProcessed())
            .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
            .sum();
    }


    public double getCapitalUsedByStrategy(String strategy) {
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && !p.isExitProcessed()
                      && p.getStrategy().equals(strategy))
            .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
            .sum();
    }

    /** Per-broker overload (Decision #3: per-broker capital tracking). */
    public double getCapitalUsedByStrategyAndBroker(String strategy, String broker) {
        String b = broker != null ? broker : "KITE";
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && !p.isExitProcessed()
                      && p.getStrategy().equals(strategy)
                      && b.equals(p.getBroker() != null ? p.getBroker() : "KITE"))
            .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
            .sum();
    }


    public double getIntradayCapitalUsedByStrategy(String strategy) {
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && !p.isExitProcessed()
                      && p.getStrategy().equals(strategy)
                      && p.getPositionType() == Position.PositionType.INTRADAY)
            .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
            .sum();
    }

    /** Per-broker overload (Decision #3: per-broker capital tracking). */
    public double getIntradayCapitalUsedByStrategyAndBroker(String strategy, String broker) {
        String b = broker != null ? broker : "KITE";
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && !p.isExitProcessed()
                      && p.getStrategy().equals(strategy)
                      && p.getPositionType() == Position.PositionType.INTRADAY
                      && b.equals(p.getBroker() != null ? p.getBroker() : "KITE"))
            .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
            .sum();
    }

    public Position createPosition(String symbol, String strategy, Position.PositionType positionType){
        Position position = new Position(symbol, 0, 0, 0, true, strategy, null, null, null, positionType);
        return position;
    }

    /** Broker-aware overload — sets broker + route on the new position. */
    public Position createPosition(String symbol, String strategy, Position.PositionType positionType,
                                   String broker, com.satyam.trading2.broker.ExecutionRoute route) {
        Position position = new Position(symbol, 0, 0, 0, true, strategy, null, null, null, positionType);
        position.setBroker(broker != null ? broker : "KITE");
        position.setExecutionRoute(route != null ? route : com.satyam.trading2.broker.ExecutionRoute.KITE);
        return position;
    }


    // ─────────────────────────────────────────────────────────────────────
    // findBySymbolAndStrategy — backward compat (broker defaults to KITE).
    // NOTE: the inner map key is now "KITE::strategy", so this looks up by
    // that key. Old callers that never set broker get KITE positions, which
    // is correct for the current Kite-only system.
    // ─────────────────────────────────────────────────────────────────────
    public Position findBySymbolAndStrategy(String symbol, String strategy) {
        return findBySymbolAndStrategy(symbol, strategy, "KITE");
    }

    /** Broker-aware overload — canonical form. */
    public Position findBySymbolAndStrategy(String symbol, String strategy, String broker) {
        return positionMap.getOrDefault(symbol, new ConcurrentHashMap<>()).get(makeKey(broker, strategy));
    }

    // ─────────────────────────────────────────────────────────────────────
    // findByTargetOrderId — backward compat (searches all brokers).
    // Order IDs CAN collide across brokers, so this is unsafe in a multi-broker
    // world. Prefer findByBrokerAndTargetOrderId. Kept for existing callers
    // (OrderCompletionProcessor.onSellCompleted) until they migrate.
    // ─────────────────────────────────────────────────────────────────────
    public Position findByTargetOrderId(String targetOrderId){
        if (targetOrderId == null) {
            log.warn("findByTargetOrderId called with null targetOrderId");
            return null;
        }

        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && p.getTargetOrderId() != null && p.getTargetOrderId().equals(targetOrderId))
            .findFirst()
            .orElse(null);
    }

    /** Broker-aware overload — canonical, no collision risk. */
    public Position findByBrokerAndTargetOrderId(String broker, String targetOrderId) {
        if (targetOrderId == null) {
            log.warn("findByBrokerAndTargetOrderId called with null targetOrderId");
            return null;
        }
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && targetOrderId.equals(p.getTargetOrderId())
                      && (broker != null ? broker : "KITE").equals(p.getBroker() != null ? p.getBroker() : "KITE"))
            .findFirst()
            .orElse(null);
    }

    // ─────────────────────────────────────────────────────────────────────
    // findBySecondaryTargetOrderId — for dual sell-order CNC positions.
    // Searches all positions for one whose secondaryTargetOrderId matches.
    // ─────────────────────────────────────────────────────────────────────
    public Position findBySecondaryTargetOrderId(String secondaryTargetOrderId) {
        if (secondaryTargetOrderId == null) {
            return null;
        }
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && secondaryTargetOrderId.equals(p.getSecondaryTargetOrderId()))
            .findFirst()
            .orElse(null);
    }

    // ─────────────────────────────────────────────────────────────────────
    // findHoldingBySymbol — backward compat, returns the FIRST holding found
    // for the symbol across all brokers/strategies. In a multi-broker world
    // the same symbol can be held at multiple brokers; prefer
    // findHoldingsBySymbol (returns List) per Decision #4.
    // ─────────────────────────────────────────────────────────────────────
    public Position findHoldingBySymbol(String symbol) {
        Map<String, Position> strategyPositions = positionMap.get(symbol);
        if (strategyPositions == null || strategyPositions.isEmpty()) {
            return null;
        }

        // Return the first holding found for this symbol (across any strategy/broker)
        return strategyPositions.values().stream()
            .filter(p -> p != null && p.getPositionType() == Position.PositionType.HOLDING)
            .findFirst()
            .orElse(null);
    }

    // ─────────────────────────────────────────────────────────────────────
    // findHoldingBySymbolAndBroker — broker-aware holding lookup.
    // The same symbol can be held at multiple brokers as independent rows
    // (e.g. RELIANCE at KITE and at SHOONYA). A holding buy must find (and
    // accumulate into) only its own broker's holding, never merge into another
    // broker's. Returns the first holding for this symbol at the given broker.
    // ─────────────────────────────────────────────────────────────────────
    public Position findHoldingBySymbolAndBroker(String symbol, String broker) {
        Map<String, Position> strategyPositions = positionMap.get(symbol);
        if (strategyPositions == null || strategyPositions.isEmpty()) {
            return null;
        }
        String b = broker != null ? broker : "KITE";
        return strategyPositions.values().stream()
            .filter(p -> p != null && p.getPositionType() == Position.PositionType.HOLDING
                      && b.equals(p.getBroker() != null ? p.getBroker() : "KITE"))
            .findFirst()
            .orElse(null);
    }

    /**
     * Find ALL holdings for a symbol across all brokers and strategies.
     * Per Decision #4: same symbol can be held at Kite and INDstocks
     * simultaneously as independent positions. Returns an empty list if none.
     */
    public List<Position> findHoldingsBySymbol(String symbol) {
        Map<String, Position> strategyPositions = positionMap.get(symbol);
        if (strategyPositions == null || strategyPositions.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        return strategyPositions.values().stream()
            .filter(p -> p != null && p.getPositionType() == Position.PositionType.HOLDING)
            .collect(Collectors.toList());
    }

}

