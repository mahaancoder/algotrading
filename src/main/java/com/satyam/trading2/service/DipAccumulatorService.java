package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.DipState;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DipAccumulatorService {

    private final Map<String, DipState> dipMap = new ConcurrentHashMap<>();
    private final String FILE = "dip_states.csv";

    /**
     * Create composite key from symbol and strategy
     */
    private String makeKey(String symbol, String strategy) {
        return symbol + "_" + strategy;
    }

    public DipState get(String symbol, String strategy){
        return dipMap.get(makeKey(symbol, strategy));
    }

    public void save(DipState s){
        dipMap.put(makeKey(s.getSymbol(), s.getOwnerStrategy()), s);
        persist(s);
    }

    public void remove(String symbol, String strategy){
        dipMap.remove(makeKey(symbol, strategy));
        rewriteFile();
    }

    private void persist(DipState s) {
        // Always rewrite entire file to keep it in sync
        rewriteFile();
    }

    private void rewriteFile() {
        try {
            FileWriter fw = new FileWriter(FILE, false); // false = overwrite
            fw.write("symbol,strategy,levelFilled,totalCapital,totalQty,lastPrice,avgCost,targetOrderId,lastBuyTime,targetPrice,lastObservedPrice,convertedToHolding\n");
            for (DipState s : dipMap.values()) {
                fw.write(s.getSymbol() + "," +
                        s.getOwnerStrategy() + "," +
                        s.getLevelFilled() + "," +
                        s.getTotalCapital() + "," +
                        s.getTotalQty() + "," +
                        s.getLastPrice() + "," +
                        s.getAvgCost() + "," +
                        s.getLastBuyTime() + "," +
                        s.getLastObservedPrice() + "," +
                        s.isConvertedToHolding() + "\n");
            }
            fw.close();
//            System.out.println("✅ Updated dip_states.csv - " + dipMap.size() + " states persisted");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void loadDipStates() {
        File file = new File(FILE);
        if (!file.exists()) {
            System.out.println("No dip_states.csv file found - starting fresh");
            return;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            boolean firstLine = true;
            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue; // Skip header
                }
                String[] p = line.split(",");
                if (p.length < 12) {
                    continue;
                }
                DipState state = new DipState();
                state.setSymbol(p[0].trim());
                state.setOwnerStrategy(p[1].trim());
                state.setLevelFilled(Integer.parseInt(p[2].trim()));
                state.setTotalCapital(Double.parseDouble(p[3].trim()));
                state.setTotalQty(Integer.parseInt(p[4].trim()));
                state.setLastPrice(Double.parseDouble(p[5].trim()));
                state.setAvgCost(Double.parseDouble(p[6].trim()));
                state.setLastBuyTime(Long.parseLong(p[8].trim()));
                state.setLastObservedPrice(Double.parseDouble(p[10].trim()));
                state.setConvertedToHolding(Boolean.parseBoolean(p[11].trim()));

                dipMap.put(makeKey(state.getSymbol(), state.getOwnerStrategy()), state);
            }
            System.out.println("✅ Loaded " + dipMap.size() + " DipStates from CSV");
        } catch (Exception e) {
            System.out.println("Error loading DipStates: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Reconstruct DipState from an existing HOLDING position
     * Used when DipState is lost but HOLDING exists (e.g., file corruption)
     *
     * Level calculation based on invested capital:
     * - < ₹52,000: Level 1
     * - ₹52K - ₹1.02L: Level 2
     * - ₹1.02L - ₹1.55L: Level 3
     * - > ₹1.55L: Level 4
     */
    public DipState reconstructFromHolding(com.satyam.trading2.datamodel.Position holding, String strategy) {
        DipState state = new DipState();
        state.setSymbol(holding.getSymbol());
        state.setOwnerStrategy(strategy);

        // Calculate invested capital
        double investedCapital = holding.getAveragePrice() * holding.getTotalQuantity();

        // Determine level based on invested capital
        int level;
        if (investedCapital < 52000) {
            level = 1;
        } else if (investedCapital < 102000) {
            level = 2;
        } else if (investedCapital < 155000) {
            level = 3;
        } else {
            level = 4;
        }

        state.setLevelFilled(level);
        state.setTotalCapital(investedCapital);
        state.setTotalQty(holding.getTotalQuantity());
        state.setLastPrice(holding.getAveragePrice());
        state.setAvgCost(holding.getAveragePrice());
        state.setLastObservedPrice(holding.getAveragePrice());
        state.setConvertedToHolding(true);
        state.setLastBuyTime(System.currentTimeMillis() - 60000); // Set 1 min in past to avoid cooldown

        // Save the reconstructed state
        save(state);

        System.out.println("✅ Reconstructed DipState: " + holding.getSymbol() +
                " | Level=" + level +
                " | Capital=₹" + String.format("%.0f", investedCapital) +
                " | Qty=" + holding.getTotalQuantity());

        return state;
    }

}