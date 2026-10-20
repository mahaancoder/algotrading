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
import java.nio.file.StandardCopyOption;
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
        // A missing file is normal on first boot. An existing but unreadable or
        // malformed file is not: starting with an empty/partial order ledger could
        // authorize a SELL against inventory already consumed by an earlier order.
        Path persistenceFile = Path.of(filePath);
        if (!Files.exists(persistenceFile)) {
            return;
        }
        try {
            loadFromFile();
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Cannot safely start: pending-order ledger could not be loaded from "
                            + persistenceFile + ". Repair/restore the ledger before restarting.", e);
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
                indexByBrokerOrderId(broker, brokerOrderId, po);
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

    /**
     * Orders whose broker event or position callback may have been lost during a
     * restart. A COMPLETE order is included only when its callback was not durably
     * processed; normal terminal orders are not polled again.
     */
    public List<PendingOrder> findOrdersRequiringReconciliation() {
        return ordersByInternalId.values().stream()
                .filter(po -> po != null && po.getState() != null)
                .filter(po -> !po.getState().equals(PendingOrderState.REJECTED)
                        && !po.getState().equals(PendingOrderState.CANCELLED)
                        && (!po.getState().equals(PendingOrderState.COMPLETE)
                            || !po.isCompletionProcessed()))
                .collect(java.util.stream.Collectors.toList());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Lookup: canonical (broker + brokerOrderId) — the form event handlers
    // (BrokerOrderEventProcessor) should use. No cross-broker collision.
    // ─────────────────────────────────────────────────────────────────────
    public Optional<PendingOrder> findByBrokerAndBrokerOrderId(String broker, String brokerOrderId) {
        PendingOrder po = ordersByBrokerOrderId.get(makeBrokerKey(broker, brokerOrderId));
        if (po == null) {
            // Fall back to the normalized (segment-prefix-stripped) id. INDstocks
            // returns "EQ-101177677" from REST but pushes the bare "101177677" over
            // the order-update WebSocket — this lets either form resolve the order.
            String normalized = normalizeBrokerOrderId(brokerOrderId);
            if (normalized != null && !normalized.equals(brokerOrderId)) {
                po = ordersByBrokerOrderId.get(makeBrokerKey(broker, normalized));
            }
        }
        return Optional.ofNullable(po);
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

    /**
     * Index a PendingOrder under (broker, brokerOrderId), and — when the id carries
     * a segment prefix (e.g. INDstocks "EQ-101177677") — also under the bare
     * normalized id ("101177677"). This bridges the INDstocks asymmetry where REST
     * returns the prefixed id but the order-update WebSocket sends the bare numeric
     * id, so completion events resolve back to the order that placed them.
     */
    private void indexByBrokerOrderId(String broker, String brokerOrderId, PendingOrder po) {
        if (brokerOrderId == null) return;
        ordersByBrokerOrderId.put(makeBrokerKey(broker, brokerOrderId), po);
        String normalized = normalizeBrokerOrderId(brokerOrderId);
        if (normalized != null && !normalized.equals(brokerOrderId)) {
            ordersByBrokerOrderId.put(makeBrokerKey(broker, normalized), po);
        }
    }

    /**
     * Strip a leading alphabetic segment prefix + hyphen from a broker order id
     * (e.g. "EQ-101177677" → "101177677"). Kite/Shoonya ids are plain numeric with
     * no such prefix, so they pass through unchanged.
     */
    private String normalizeBrokerOrderId(String id) {
        if (id == null) return null;
        int dash = id.indexOf('-');
        if (dash > 0 && dash < id.length() - 1) {
            String prefix = id.substring(0, dash);
            boolean allLetters = true;
            for (int i = 0; i < prefix.length(); i++) {
                if (!Character.isLetter(prefix.charAt(i))) { allLetters = false; break; }
            }
            if (allLetters) return id.substring(dash + 1);
        }
        return id;
    }

    private void loadFromFile() throws IOException {
        // Load pending orders from file
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int lineNumber = 0;
            while ((line = br.readLine()) != null) {
                lineNumber++;
                if (line.trim().isEmpty()) {
                    continue;
                }
                try {
                    PendingOrder po = fromCsv(line);
                    if (po == null || po.getOrderId() == null || po.getOrderId().trim().isEmpty()) {
                        throw new IllegalArgumentException("row has no broker order id");
                    }
                    // Index by internal id (generate if missing — legacy rows have none)
                    String internalId = po.getInternalOrderId();
                    if (internalId == null) {
                        internalId = PendingOrder.generateInternalOrderId();
                        po.setInternalOrderId(internalId);
                    }
                    ordersByInternalId.put(internalId, po);

                    // Secondary index by (broker, brokerOrderId) — dual-index the
                    // normalized id too (see indexByBrokerOrderId) for INDstocks.
                    String broker = po.getBroker() != null ? po.getBroker() : "KITE";
                    indexByBrokerOrderId(broker, po.getOrderId(), po);
                } catch (Exception e) {
                    throw new IOException("Corrupt pending-order ledger at line " + lineNumber
                            + "; refusing to skip an order and risk authorizing an unsafe SELL", e);
                }
            }
            System.out.println("✅ Loaded " + ordersByInternalId.size() + " pending orders from file");
        }
    }


    private void rewriteFile() throws IOException {
        Path target = Path.of(filePath).toAbsolutePath();
        Path parent = target.getParent();
        if (parent != null) Files.createDirectories(parent);
        Path temp = Files.createTempFile(parent, target.getFileName().toString(), ".tmp");
        boolean moved = false;
        try {
            try (BufferedWriter bw = Files.newBufferedWriter(temp)) {
                for (PendingOrder po : ordersByInternalId.values()) {
                    bw.write(toCsv(po));
                    bw.newLine();
                }
            }
            try {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            moved = true;
        } finally {
            if (!moved) Files.deleteIfExists(temp);
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
        String positionType = po.getPositionType() != null ? po.getPositionType().name() : "";
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
                signalId,
                positionType,
                String.valueOf(po.isCompletionProcessed())
        );
    }

    private PendingOrder fromCsv(String line){
        // Use limit -1 so a trailing empty field (e.g. an empty signalId written
        // as "...,SHOONYA,") is preserved. Default split(",") drops trailing
        // empties, yielding 13 tokens for a 14-column row, which misaligns the
        // parser into the legacy 10-field branch (symbol read as side, etc.).
        String[] p = line.split(",", -1);

        // Current schema: 16 fields. Previous broker-aware schema: 14 fields.
        // Legacy schema: 10 fields (no broker/route/signalId/internalId).
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
            if (p.length >= 15 && !p[14].isEmpty()) {
                po.setPositionType(Position.PositionType.valueOf(p[14]));
            } else {
                // Old rows did not persist product type. INTRADAY is only a
                // placeholder for lock metadata; sell authorization still uses
                // the live Position's product and broker-confirmed inventory.
                po.setPositionType(Position.PositionType.INTRADAY);
            }
            if (p.length >= 16) {
                po.setCompletionProcessed(Boolean.parseBoolean(p[15]));
            } else if (po.getSide() == TradeSide.SELL
                    && po.getState() == PendingOrderState.COMPLETE) {
                // Previous CSV formats did not persist the idempotency marker.
                // Never replay a historical completed SELL after restart: a
                // broker reconciliation must repair any crash-window mismatch.
                po.setCompletionProcessed(true);
            } else if (po.getSide() == TradeSide.SELL && po.getFilledQty() > 0
                    && (po.getState() == PendingOrderState.CANCELLED
                    || po.getState() == PendingOrderState.REJECTED)) {
                // A legacy terminal partial SELL may not have been applied to
                // the position before the process stopped. Keep it reserved.
                po.setState(PendingOrderState.PARTIALLY_FILLED);
            }
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
