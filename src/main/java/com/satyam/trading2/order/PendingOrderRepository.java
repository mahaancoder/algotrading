package com.satyam.trading2.order;

import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PendingOrderRepository {

    private final Map<String, PendingOrder> orderByBrokerId = new ConcurrentHashMap<>();
    private final String filePath = "/home/ec2-user/pending_orders.csv";
    private final Object lock = new Object();

    @PostConstruct
    public void init() {
        // Load pending orders from file
        try {
            loadFromFile();
        }
        catch (Exception e){
            System.out.println("Error loading pending orders: " + e.getMessage());
        }
    }

    public PendingOrder create(String orderId, String symbol, TradeSide side, String strategy, int qty, Double entry, boolean isHolding ){
        PendingOrder po = new PendingOrder();
        po.setOrderId(orderId);
        po.setState(PendingOrderState.PENDING);
        po.setSymbol(symbol);
        po.setSide(side);
        po.setStrategy(strategy);
        po.setRequestQty(qty);
        po.setCreatedTime(System.currentTimeMillis());
        po.setAvgPrice(entry);
        po.setPositionType(isHolding ? Position.PositionType.HOLDING : Position.PositionType.INTRADAY);
        return po;
    }

    public void save(PendingOrder po) throws IOException {
        synchronized (lock) {
            orderByBrokerId.put(po.getOrderId(), po);
            rewriteFile();
        }
    }

    public List<PendingOrder> findOpenOrders(){
        return orderByBrokerId.values().stream()
                .filter(po -> po != null && po.getState() != null)  // ✅ Null safety
                .filter(po -> !po.getState().equals(PendingOrderState.COMPLETE) &&
                        !po.getState().equals(PendingOrderState.REJECTED) &&
                        !po.getState().equals(PendingOrderState.CANCELLED))
                .collect(java.util.stream.Collectors.toList());
    }

    public Optional<PendingOrder> findByBrokerOrderId(String brokerOrderId) {
        return Optional.ofNullable(orderByBrokerId.get(brokerOrderId));
    }

    private void loadFromFile() throws IOException {
        // Load pending orders from file
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int lineNumber = 0;
            while ((line = br.readLine()) != null) {
                lineNumber++;
                try {
                    PendingOrder po = fromCsv(line);
                    if (po != null && po.getOrderId() != null) {
                        orderByBrokerId.put(po.getOrderId(), po);
                    }
                } catch (Exception e) {
                    System.err.println("⚠️ Failed to parse pending order at line " + lineNumber + ": " + line);
                    System.err.println("   Error: " + e.getMessage());
                    // Skip corrupted line and continue
                }
            }
            System.out.println("✅ Loaded " + orderByBrokerId.size() + " pending orders from file");
        }
    }


    private void rewriteFile() throws IOException {
        try (BufferedWriter bw = Files.newBufferedWriter(Path.of(filePath))) {
            for (PendingOrder po : orderByBrokerId.values()) {
                bw.write(toCsv(po));  // ✅ Use toCsv() for proper format
                bw.newLine();
            }
        }
        catch (Exception e){
            System.out.println("❌ Exception while writing pending orders file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void appendToFile(PendingOrder po) throws IOException {
        try(BufferedWriter bw = Files.newBufferedWriter(Path.of(filePath), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            bw.write(toCsv(po));
            bw.newLine();
        }
    }

    private String toCsv(PendingOrder pendingOrder){
        // Null safety: provide defaults for potentially null fields
        String orderId = pendingOrder.getOrderId() != null ? pendingOrder.getOrderId() : "UNKNOWN";
        String symbol = pendingOrder.getSymbol() != null ? pendingOrder.getSymbol() : "UNKNOWN";
        String side = pendingOrder.getSide() != null ? pendingOrder.getSide().name() : "UNKNOWN";
        String state = pendingOrder.getState() != null ? pendingOrder.getState().name() : "UNKNOWN";
        String strategy = pendingOrder.getStrategy() != null ? pendingOrder.getStrategy() : "UNKNOWN";

        return String.join(",",
                orderId,
                symbol,
                side,
                state,
                String.valueOf(pendingOrder.getRequestQty()),
                String.valueOf(pendingOrder.getFilledQty()),
                String.valueOf(pendingOrder.getAvgPrice()),
                String.valueOf(pendingOrder.getCreatedTime()),
                String.valueOf(pendingOrder.getUpdatedTime()),
                strategy
        );
    }

    private PendingOrder fromCsv(String line){
        String[] p = line.split(",");

        // Validate format: expect 10 fields
        if (p.length < 10) {
            System.err.println("⚠️ Invalid CSV format (expected 10 fields, got " + p.length + "): " + line);
            return null;
        }

        PendingOrder po = new PendingOrder();
        po.setOrderId(p[0]);
        po.setSymbol(p[1]);
        po.setSide(TradeSide.valueOf(p[2]));
        po.setState(PendingOrderState.valueOf(p[3]));
        po.setRequestQty(Integer.parseInt(p[4]));
        po.setFilledQty(Integer.parseInt(p[5]));
        po.setAvgPrice(Double.parseDouble(p[6]));
        po.setCreatedTime(Long.parseLong(p[7]));
        po.setUpdatedTime(Long.parseLong(p[8]));
        po.setStrategy(p[9]);
        return po;
    }


}
