package com.satyam.trading2.service;

import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TickWindow {

    private final Deque<Double> prices = new LinkedList<>();
    private static final int MAX_SIZE = 15;

    private static final Map<String, TickWindow> tickWindows = new ConcurrentHashMap<>();

    public synchronized void add(double price) {
        prices.addLast(price);
        if (prices.size() > MAX_SIZE) {
            prices.removeFirst();
        }
    }

    public synchronized List<Double> getPrices(){
        return new ArrayList<>(prices);
    }

    public synchronized boolean isReady(){
        return prices.size() >= 10;
    }

    public static void addTickWindow(String symbol, Double price){
        tickWindows.computeIfAbsent(symbol, k -> new TickWindow()).add(price);
    }

    public static TickWindow getTickWindow(String symbol) {
        return tickWindows.get(symbol);
    }
}
