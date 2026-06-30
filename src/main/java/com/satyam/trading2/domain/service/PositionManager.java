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
    
    // symbol -> strategy -> position
    private final Map<String, Map<String, Position>> positionMap = new ConcurrentHashMap<>();


    public void addPosition(Position position) {
        positionMap
            .computeIfAbsent(position.getSymbol(), k -> new ConcurrentHashMap<>())
            .put(position.getStrategy(), position);
    }

    /**
     * Add or update a position (overload with explicit symbol and strategy)
     * If position already exists for the strategy, adds quantity and averages entry price
     */
    public void addPosition(String symbol, String strategy, Position position) {
        Map<String, Position> strategyMap = positionMap.computeIfAbsent(symbol, k -> new ConcurrentHashMap<>());

        Position existingPosition = strategyMap.get(strategy);

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
            strategyMap.put(strategy, position);
        }
    }
    

    public Position getPosition(String symbol, String strategy) {
        Map<String, Position> strategyMap = positionMap.get(symbol);
        return strategyMap != null ? strategyMap.get(strategy) : null;
    }
    

    public Map<String, Position> getPositionsForSymbol(String symbol) {
        return positionMap.getOrDefault(symbol, new ConcurrentHashMap<>());
    }

    public void updatePosition(Position position) {
        addPosition(position); // Same as add since we use maps
    }

    public void removePosition(String symbol, String strategy) {
        Map<String, Position> strategyMap = positionMap.get(symbol);
        if (strategyMap != null) {
            strategyMap.remove(strategy);
            log.debug("Position removed: {} - {}", symbol, strategy);
            
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


    public double getIntradayCapitalUsedByStrategy(String strategy) {
        return positionMap.values().stream()
            .flatMap(strategyMap -> strategyMap.values().stream())
            .filter(p -> p != null && !p.isExitProcessed()
                      && p.getStrategy().equals(strategy)
                      && p.getPositionType() == Position.PositionType.INTRADAY)
            .mapToDouble(p -> p.getAveragePrice() * p.getTotalQuantity())
            .sum();
    }

    public Position createPosition(String symbol, String strategy, Position.PositionType positionType){
        Position position = new Position(symbol, 0, 0, 0, true, strategy, null, null, null, positionType);
        return position;
    }


    public Position findBySymbolAndStrategy(String symbol, String strategy) {
        return positionMap.getOrDefault(symbol, new ConcurrentHashMap<>()).get(strategy);
    }

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

    /**
     * Find a holding by symbol only (not by strategy)
     * Since we can only have ONE holding per symbol, search across all strategies
     * Returns the first holding found for this symbol
     */
    public Position findHoldingBySymbol(String symbol) {
        Map<String, Position> strategyPositions = positionMap.get(symbol);
        if (strategyPositions == null || strategyPositions.isEmpty()) {
            return null;
        }

        // Return the first holding found for this symbol (across any strategy)
        return strategyPositions.values().stream()
            .filter(p -> p != null && p.getPositionType() == Position.PositionType.HOLDING)
            .findFirst()
            .orElse(null);
    }

}

