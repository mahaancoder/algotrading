package com.satyam.trading2.order;

import com.satyam.trading2.broker.ExecutionRoute;
import com.satyam.trading2.datamodel.PendingOrder;
import com.satyam.trading2.datamodel.PendingOrderState;
import com.satyam.trading2.datamodel.Position;
import com.satyam.trading2.datamodel.TradeSide;
import org.springframework.beans.factory.annotation.Value;
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

    // Keyed by internalOrderId (our UUID) — never collides across brokers.
    // A secondary lookup by (broker, brokerOrderId) is provided for event handlers.
    private final Map<String, PendingOrder> ordersByInternalId = new ConcurrentHashMap<>();
    private final Map<String, PendingOrder> ordersByBrokerOrderId = new ConcurrentHashMap<>();

    @Value("${pending.orders.file.path:/home/ec2-user/pending_orders.csv}")
    private String filePath;

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

    // ─────────────────────────────────────────────────────────────────────
    // create() — backward-compatible overload (defaults to KITE broker)
    // Existing callers (ExecutionEngine, schedulers, OrderCompletionProcessor)
    // all pass Kite order IDs, so KITE is the safe default until they migrate.
    // ─────────────────────────────────────────────────────────────────────
    public PendingOrder create(String orderId, String symbol, TradeSide side,
                               String strategy, int qty, Double entry, boolean isHolding) {
        return create(orderId, symbol, side, strategy, qty, entry, isHolding,
                "KITE", ExecutionRoute.KITE, null);
    }

    // ─────────────────────────────────────────────────────────────────────
    // create() — full broker-aware overload (new canonical form)
    // ─────────────────────────────────────────────────────────────────────
    public PendingOrder create(String brokerOrderId, String symbol, TradeSide side,
                               String strategy, int qty, Double entry, boolean isHolding,
                               String broker, ExecutionRoute route, String signalId) {
        PendingOrder po = new PendingOrder();
        po.setInternalOrderId(PendingOrder.generateInternalOrderId());
        po.setBrokerOrderId(brokerOrderId);
        po.setOrderId(brokerOrderId); // backward-compat sync
        po.setState(PendingOrderState.PENDING);
        po.setSymbol(symbol);
        po.setSide(side);
        po.setStrategy(strategy);
        po.setRequestQty(qty);
        po.setCreatedTime(System.currentTimeMillis());
        po.setAvgPrice(entry);
        po.setPositionType(isHolding ? Position.PositionType.HOLDING : Position.PositionType.INTRADAY);
        po.setBroker(broker);
        po.setExecutionRoute(route);
        po.setSignalId(signalId);
        return po;
    }

    public void save(PendingOrder po) throws IOException {
        synchronized (lock) {
            // Index by internal id (primary key — never collides)
            String internalId = po.getInternalOrderId();
            if (internalId == null) {
                internalId = PendingOrder.generateInternalOrderId();
                po.setInternalOrderId(internalId);
            }
            ordersByInternalId.put(internalId, po);

            // Secondary index by (broker + brokerOrderId) for fast event lookup.
            // Falls back to orderId/brokerOrderId for backward compat.
            String broker = po.getBroker() != null ? po.getBroker() : "KITE";
            String brokerOrderId = po.getBrokerOrderId() != null ? po.getBrokerOrderId() : po.getOrderId();
            if (brokerOrderId != null) {
                ordersByBrokerOrderId.put(makeBrokerKey(broker, brokerOrderId), po);
            }
            rewriteFile();
        }
    }

    public List<PendingOrder> findOpenOrders(){
        return ordersByInternalId.values().stream()
                .filter(po -> po != null && po.getState() != null)
                .filter(po -> !po.getState().equals(PendingOrderState.COMPLETE) &&
                        !po.getState().equals(PendingOrderState.REJECTED) &&
                        !po.getState().equals(PendingOrderState.CANCELLED))
                .collect(java.util.stream.Collectors.toList());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Lookup: canonical (broker + brokerOrderId) — the form event handlers
    // (BrokerOrderEventProcessor) should use. No cross-broker collision.
    // ─────────────────────────────────────────────────────────────────────
    public Optional<PendingOrder> findByBrokerAndBrokerOrderId(String broker, String brokerOrderId) {
        return Optional.ofNullable(ordersByBrokerOrderId.get(makeBrokerKey(broker, brokerOrderId)));
    }

    // ─────────────────────────────────────────────────────────────────────
    // Lookup: backward-compat (orderId only). Defaults broker to KITE.
    // Used by existing OrderLifecycleManager / KiteWebhookService until they
    // migrate to the broker-aware form. Safe because today all orders ARE Kite.
    // ─────────────────────────────────────────────────────────────────────
    public Optional<PendingOrder> findByBrokerOrderId(String brokerOrderId) {
        return findByBrokerAndBrokerOrderId("KITE", brokerOrderId);
    }

    private String makeBrokerKey(String broker, String brokerOrderId) {
        return broker + "::" + brokerOrderId;
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
                        // Index by internal id (generate if missing — legacy rows have none)
                        String internalId = po.getInternalOrderId();
                        if (internalId == null) {
                            internalId = PendingOrder.generateInternalOrderId();
                            po.setInternalOrderId(internalId);
                        }
                        ordersByInternalId.put(internalId, po);

                        // Secondary index by (broker, brokerOrderId)
                        String broker = po.getBroker() != null ? po.getBroker() : "KITE";
                        ordersByBrokerOrderId.put(makeBrokerKey(broker, po.getOrderId()), po);
                    }
                } catch (Exception e) {
                    System.err.println("⚠️ Failed to parse pending order at line " + lineNumber + ": " + line);
                    System.err.println("   Error: " + e.getMessage());
                    // Skip corrupted line and continue
                }
            }
            System.out.println("✅ Loaded " + ordersByInternalId.size() + " pending orders from file");
        }
    }


    private void rewriteFile() throws IOException {
        try (BufferedWriter bw = Files.newBufferedWriter(Path.of(filePath))) {
            for (PendingOrder po : ordersByInternalId.values()) {
                bw.write(toCsv(po));
                bw.newLine();
            }
        }
        catch (Exception e){
            System.out.println("❌ Exception while writing pending orders file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String toCsv(PendingOrder po){
        // Null safety: provide defaults for potentially null fields
        String internalOrderId = po.getInternalOrderId() != null ? po.getInternalOrderId() : "";
        String brokerOrderId = po.getBrokerOrderId() != null ? po.getBrokerOrderId() : (po.getOrderId() != null ? po.getOrderId() : "UNKNOWN");
        String symbol = po.getSymbol() != null ? po.getSymbol() : "UNKNOWN";
        String side = po.getSide() != null ? po.getSide().name() : "UNKNOWN";
        String state = po.getState() != null ? po.getState().name() : "UNKNOWN";
        String strategy = po.getStrategy() != null ? po.getStrategy() : "UNKNOWN";
        String broker = po.getBroker() != null ? po.getBroker() : "KITE";
        String route = po.getExecutionRoute() != null ? po.getExecutionRoute().name() : "KITE";
        String signalId = po.getSignalId() != null ? po.getSignalId() : "";

        // Schema: internalOrderId, brokerOrderId, symbol, side, state, requestQty,
        //         filledQty, avgPrice, createdTime, updatedTime, strategy, broker, route, signalId
        return String.join(",",
                internalOrderId,
                brokerOrderId,
                symbol,
                side,
                state,
                String.valueOf(po.getRequestQty()),
                String.valueOf(po.getFilledQty()),
                String.valueOf(po.getAvgPrice()),
                String.valueOf(po.getCreatedTime()),
                String.valueOf(po.getUpdatedTime()),
                strategy,
                broker,
                route,
                signalId
        );
    }

    private PendingOrder fromCsv(String line){
        String[] p = line.split(",");

        // New schema: 14 fields. Old schema: 10 fields (no broker/route/signalId/internalId).
        // Handle both for backward compat with existing CSV files on disk.
        if (p.length < 10) {
            System.err.println("⚠️ Invalid CSV format (expected 10-14 fields, got " + p.length + "): " + line);
            return null;
        }

        PendingOrder po = new PendingOrder();

        if (p.length >= 14) {
            // New schema: internalOrderId, brokerOrderId, symbol, side, state, requestQty,
            //            filledQty, avgPrice, createdTime, updatedTime, strategy, broker, route, signalId
            po.setInternalOrderId(p[0].isEmpty() ? null : p[0]);
            po.setBrokerOrderId(p[1]);
            po.setOrderId(p[1]); // backward-compat sync
            po.setSymbol(p[2]);
            po.setSide(TradeSide.valueOf(p[3]));
            po.setState(PendingOrderState.valueOf(p[4]));
            po.setRequestQty(Integer.parseInt(p[5]));
            po.setFilledQty(Integer.parseInt(p[6]));
            po.setAvgPrice(Double.parseDouble(p[7]));
            po.setCreatedTime(Long.parseLong(p[8]));
            po.setUpdatedTime(Long.parseLong(p[9]));
            po.setStrategy(p[10]);
            po.setBroker(p[11]);
            po.setExecutionRoute(ExecutionRoute.valueOf(p[12]));
            po.setSignalId(p[13].isEmpty() ? null : p[13]);
        } else {
            // Old schema (10 fields): orderId, symbol, side, state, requestQty,
            //                       filledQty, avgPrice, createdTime, updatedTime, strategy
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
            // Defaults for broker dimension
            po.setBroker("KITE");
            po.setExecutionRoute(ExecutionRoute.KITE);
        }
        return po;
    }


}
