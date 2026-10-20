package com.satyam.trading2.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBeanBuilder;
import com.satyam.trading2.config.KiteConfig;
import com.satyam.trading2.datamodel.*;
import com.satyam.trading2.domain.service.OrderSanitizer;
import com.satyam.trading2.infrastructure.ratelimit.RateLimiter;
import com.satyam.trading2.order.PendingOrderRepository;
import com.satyam.trading2.repository.HoldingMetadataRepository;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.Margin;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.StringReader;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

@Service
@RequiredArgsConstructor
public class OrderServiceV2 {

    private final KiteConfig kiteConfig;
    private final RateLimiter rateLimiter;
    private final OrderSanitizer orderSanitizer;
    private final HoldingMetadataRepository holdingMetadataRepository;
    private final CashFlowModeService cashFlowModeService;
    private final PendingOrderRepository pendingOrderRepository;

    // ===== PERFORMANCE OPTIMIZATION: HTTP Client with connection pooling and timeouts =====
    // Connection pooling dramatically reduces latency for order placement
    // Aggressive timeouts ensure we don't wait too long for slow responses
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofMillis(500))  // Fast connection timeout
            .version(HttpClient.Version.HTTP_1_1)              // HTTP/1.1 for better broker compatibility
            .build();

    private final ObjectMapper objectMapper = new ObjectMapper();

    // ================= EXIT =================
    @Deprecated
    public String exitPosition(String symbol, int qty, boolean isHolding, String strategy) {
        System.err.println("🛑 [exitPosition] Refusing unguarded direct SELL for " + symbol
                + "; use the Position-aware BrokerOrderRouter.");
        return null;
    }

    // ================= CANCEL =================
    public void cancelOrder(String orderId) {
        try {
            // 🚦 Rate limiting
            rateLimiter.acquire("cancelOrder");

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/orders/regular/" + orderId))
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .DELETE()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            // Check for rate limit error
            try {
                JsonNode json = objectMapper.readTree(response.body());
                if (!"success".equals(json.get("status").asText())) {
                    String errorMsg = json.has("message") ? json.get("message").asText() : "Unknown error";

                    if (errorMsg.contains("Maximum allowed order requests exceeded") ||
                        errorMsg.contains("Too many requests") ||
                        errorMsg.contains("Rate limit exceeded")) {
                        System.err.println("🚨🚨🚨 KITE RATE LIMIT ERROR DETECTED in cancelOrder! Triggering circuit breaker");
                        rateLimiter.tripCircuitBreaker();
                    }
                }
            } catch (Exception parseEx) {
                // Ignore JSON parsing errors for cancel - not critical
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ================= CORE =================
    /**
     * General legacy entry point is BUY-only. SELLs require a Position-aware
     * reservation in BrokerOrderRouter; callers cannot bypass it with symbol/qty.
     */
    public String placeOrder(String symbol, String side, String type, int qty, double price, double trigger, String product, String strategy) {
        if ("SELL".equalsIgnoreCase(side)) {
            System.err.println("🛑 [placeOrder] Refusing direct SELL for " + symbol
                    + "; use BrokerOrderRouter so long inventory and pending SELLs are checked.");
            return null;
        }
        return placeOrderInternal(symbol, side, type, qty, price, trigger, product, strategy);
    }

    public String placeRoutedSell(Position position, String symbol, int qty, double price,
                                  boolean isHolding, String strategy) {
        if (!com.satyam.trading2.order.SellOrderAuthorizationContext.isAuthorized()) {
            System.err.println("🛑 [placeRoutedSell] Refusing SELL without BrokerOrderRouter authorization.");
            return null;
        }
        if (position == null || symbol == null || qty <= 0 || strategy == null
                || !symbol.equalsIgnoreCase(position.getSymbol())) {
            System.err.println("🛑 [placeRoutedSell] Invalid Position-aware SELL request.");
            return null;
        }
        String product = isHolding ? "CNC" : "MIS";
        return placeOrderInternal(symbol, "SELL", "LIMIT", qty, price, 0, product, strategy);
    }

    private String placeOrderInternal(String symbol, String side, String type, int qty, double price, double trigger, String product, String strategy) {
             OrderRequest orderRequest =  orderSanitizer.sanitize(OrderRequest.builder().symbol(symbol).side(TradeSide.valueOf(side)).orderType(type).quantity(qty).price(price).triggerPrice(trigger).product(product).strategy(strategy).build());
             price = orderRequest.getPrice();
        try {
            // 🚦 Rate limiting - wait if necessary to respect 10 req/sec limit
            rateLimiter.acquire("placeOrder");

            // ===== OPTIMIZATION: Pre-build orderRequest body and URI to reduce processing time =====
            String body = "tradingsymbol=" + symbol + "&exchange=NSE" + "&transaction_type=" + side + "&order_type=" + type + "&quantity=" + qty + "&product=" + product + "&validity=DAY" + (price > 0 ? "&price=" + price : "") + (trigger > 0 ? "&trigger_price=" + trigger : "");

            // ===== OPTIMIZATION: Request with timeout to prevent hanging =====
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/orders/regular"))
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .timeout(java.time.Duration.ofMillis(2000))  // 2 second timeout for order placement
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode json = objectMapper.readTree(response.body());

            // Check for API errors
            if (!"success".equals(json.get("status").asText())) {
                String errorMsg = json.has("message") ? json.get("message").asText() : "Unknown error";

                // ===== CIRCUIT BREAKER: Detect rate limit error from Kite =====
                if (errorMsg.contains("Maximum allowed order requests exceeded") ||
                    errorMsg.contains("Too many requests") ||
                    errorMsg.contains("Rate limit exceeded")) {
                    System.err.println("🚨🚨🚨 KITE RATE LIMIT ERROR DETECTED! Triggering circuit breaker for 12 seconds");
                    rateLimiter.tripCircuitBreaker();
                }

                System.err.println("❌ Kite API Error for " + symbol + " " + side + " " + type + ": " + errorMsg);
                System.err.println("   Full response: " + response.body());
                return null;
            }

            return json.get("data").get("order_id").asText();

        } catch (Exception e) {
            System.err.println("❌ Exception placing order for " + symbol + " " + side + " " + type + ": " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Legacy API lacks the position inventory required to reserve SELL quantity.
     * Fail closed; callers must use the Position-aware overload below.
     */
    @Deprecated
    public String updateTargetOrder(String symbol, double target, int qty, String oldOrderId,
                                    boolean isHolding, String strategy) {
        System.err.println("🛑 [updateTargetOrder] Refusing unguarded target SELL for " + symbol
                + "; use the Position-aware BrokerOrderRouter.");
        return null;
    }

    public String updateTargetOrder(Position position, String symbol, double target, int qty,
                                    String oldOrderId, boolean isHolding, String strategy) {
        if (!com.satyam.trading2.order.SellOrderAuthorizationContext.isAuthorized()) {
            System.err.println("🛑 [updateTargetOrder] Refusing target SELL without BrokerOrderRouter authorization.");
            return null;
        }
        if (position == null) return null;
        synchronized (com.satyam.trading2.order.SellLifecycleLockRegistry.lockFor(position)) {
            return updateTargetOrderLocked(symbol, target, qty, oldOrderId, isHolding, strategy);
        }
    }

    private String updateTargetOrderLocked(String symbol, double target, int qty, String oldOrderId,
                                           boolean isHolding, String strategy) {
        if (qty <= 0) {
            System.err.println("🛑 [updateTargetOrder] Refusing SELL with non-positive quantity for " + symbol);
            return null;
        }
        String product = isHolding ? "CNC" : "MIS";
        try {
            // Do not replace an order until the broker confirms cancellation and
            // proves that the old order had zero fills. Cancellation requests can
            // race with exchange fills; a terminal CANCELLED status alone is not enough.
            List<Order> snapshot = fetchOrders();
            if (snapshot == null) {
                System.err.println("🛑 [updateTargetOrder] Kite order book unavailable; refusing SELL replacement for " + symbol);
                return null;
            }
            List<Order> activeSymbolSells = snapshot.stream()
                    .filter(Objects::nonNull)
                    .filter(o -> symbol.equalsIgnoreCase(o.getTradingSymbol()))
                    .filter(o -> "SELL".equalsIgnoreCase(o.getTransactionType()))
                    .filter(o -> product.equalsIgnoreCase(o.getProduct()))
                    .filter(o -> isActiveOrderStatus(o.getStatus()))
                    .collect(java.util.stream.Collectors.toList());

            // Do not cancel unrelated SELLs merely because they share a symbol.
            // Multiple strategy allocations can coexist locally, but the broker
            // executes against one physical symbol position. Unknown active orders
            // must be reconciled rather than silently cancelled or overlapped.
            boolean untrackedActiveSell = activeSymbolSells.stream()
                    .filter(o -> oldOrderId == null || !oldOrderId.equals(o.getOrderId()))
                    .anyMatch(o -> pendingOrderRepository
                            .findByBrokerAndBrokerOrderId("KITE", o.getOrderId())
                            .map(pending -> !pending.isOpen()
                                    || pending.getRequestQty() != o.getQuantity())
                            .orElse(true));
            if (untrackedActiveSell) {
                System.err.println("🛑 [updateTargetOrder] An active " + product + " SELL for " + symbol
                        + " has no matching open KITE pending record; refusing replacement until reconciliation.");
                return null;
            }

            List<Order> candidates = oldOrderId == null
                    ? Collections.emptyList()
                    : snapshot.stream()
                        .filter(Objects::nonNull)
                        .filter(o -> oldOrderId.equals(o.getOrderId()))
                        .collect(java.util.stream.Collectors.toList());

            if (oldOrderId != null && candidates.isEmpty()) {
                System.err.println("🛑 [updateTargetOrder] Existing SELL " + oldOrderId
                        + " is missing from Kite order book; refusing replacement for " + symbol);
                return null;
            }
            for (Order candidate : candidates) {
                if (!symbol.equalsIgnoreCase(candidate.getTradingSymbol())
                        || !"SELL".equalsIgnoreCase(candidate.getTransactionType())
                        || !product.equalsIgnoreCase(candidate.getProduct())) {
                    System.err.println("🛑 [updateTargetOrder] Order " + oldOrderId
                            + " does not match expected SELL symbol/product; refusing replacement.");
                    return null;
                }
            }

            Set<String> toCancel = new LinkedHashSet<>();
            for (Order order : candidates) {
                if (order == null || order.getOrderId() == null) {
                    System.err.println("🛑 [updateTargetOrder] Incomplete order snapshot for " + symbol
                            + "; refusing replacement.");
                    return null;
                }
                String status = order.getStatus();
                if ("COMPLETE".equalsIgnoreCase(status)) {
                    System.err.println("🛑 [updateTargetOrder] Existing SELL " + order.getOrderId()
                            + " already completed; reconcile position before another SELL.");
                    return null;
                }
                if (isActiveOrderStatus(status)) {
                    toCancel.add(order.getOrderId());
                } else if (("CANCELLED".equalsIgnoreCase(status) || "REJECTED".equalsIgnoreCase(status))
                        && (order.getFilledQuantity() == null || order.getFilledQuantity() != 0)) {
                    System.err.println("🛑 [updateTargetOrder] Terminal SELL " + order.getOrderId()
                            + " has unknown/non-zero filled quantity; refusing replacement.");
                    return null;
                }
            }

            for (String existingOrderId : toCancel) {
                cancelOrder(existingOrderId);
                if (!awaitKiteSellCancellationWithZeroFills(existingOrderId, 2500L)) {
                    System.err.println("🛑 [updateTargetOrder] Cancellation/fill state for SELL "
                            + existingOrderId + " is not safely confirmed; refusing replacement for " + symbol);
                    return null;
                }
            }

            // For an explicit intraday target id, also verify terminal zero-fill
            // state even if it was already terminal in the first snapshot.
            if (!isHolding && oldOrderId != null && !toCancel.contains(oldOrderId)) {
                if (!awaitKiteSellCancellationWithZeroFills(oldOrderId, 0L)) {
                    System.err.println("🛑 [updateTargetOrder] Existing SELL " + oldOrderId
                            + " is not confirmed zero-fill/terminal; refusing replacement.");
                    return null;
                }
            }

            return placeOrderInternal(symbol, "SELL", "LIMIT", qty, target, 0, product, strategy);
        } catch (Exception e) {
            System.err.println("❌ Failed to update target SELL for " + symbol
                    + "; replacement blocked to avoid overselling: " + e.getMessage());
            return null;
        }
    }

    private boolean isActiveOrderStatus(String status) {
        return "OPEN".equalsIgnoreCase(status)
                || "TRIGGER PENDING".equalsIgnoreCase(status)
                || "UPDATE".equalsIgnoreCase(status)
                || "PARTIALLY FILLED".equalsIgnoreCase(status);
    }

    /**
     * Poll Kite until the order is terminal and explicitly has zero fills.
     * Missing orders, API failures, partial fills and COMPLETE orders all fail closed.
     */
    private boolean awaitKiteSellCancellationWithZeroFills(String orderId, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        long lastCancelAttempt = System.currentTimeMillis();
        do {
            List<Order> snapshot = fetchOrders();
            Order order = snapshot == null ? null : snapshot.stream()
                    .filter(Objects::nonNull)
                    .filter(o -> orderId.equals(o.getOrderId()))
                    .findFirst()
                    .orElse(null);
            if (order == null || order.getStatus() == null || order.getFilledQuantity() == null) {
                return false;
            }
            String status = order.getStatus();
            if ("COMPLETE".equalsIgnoreCase(status)) {
                return false;
            }
            if ("CANCELLED".equalsIgnoreCase(status) || "REJECTED".equalsIgnoreCase(status)) {
                return order.getFilledQuantity() == 0;
            }
            long now = System.currentTimeMillis();
            if (timeoutMillis == 0L || now >= deadline) {
                return false;
            }
            if (now - lastCancelAttempt >= 1000L) {
                // The previous cancel request may have been rate-limited or lost.
                // Retry while the order remains active, but never replace until
                // the order book confirms terminal status and zero fills.
                cancelOrder(orderId);
                lastCancelAttempt = now;
            }
            try {
                Thread.sleep(200L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        } while (System.currentTimeMillis() <= deadline);
        return false;
    }

    /**
     * Check if an order is still active (OPEN or TRIGGER PENDING)
     */
    private boolean isOrderStillActive(String orderId) {
        try {
            List<Order> allOrders = fetchOrders();
            return allOrders.stream()
                    .anyMatch(o -> orderId.equals(o.getOrderId()) &&
                                 ("OPEN".equalsIgnoreCase(o.getStatus()) ||
                                  "TRIGGER PENDING".equalsIgnoreCase(o.getStatus())));
        } catch (Exception e) {
            System.err.println("⚠️ Failed to check order status for " + orderId + ": " + e.getMessage());
            return true; // Assume it's active if we can't verify (safer to prevent duplicates)
        }
    }

    /**
     * Fresh Kite-confirmed net inventory. Null means the API response could not
     * be trusted; zero is returned only for a successful response with no row.
     * For CNC holdings we deliberately count settled quantity only.
     */
    public Integer getNetSellableQuantity(String symbol, boolean holding) {
        if (symbol == null || symbol.trim().isEmpty()) return null;
        try {
            rateLimiter.acquire("fetchSellableInventory");
            String endpoint = holding ? "/portfolio/holdings" : "/portfolio/positions";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + endpoint))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) return null;
            JsonNode root = objectMapper.readTree(response.body());
            if (!"success".equals(root.path("status").asText())) return null;
            JsonNode rows = holding ? root.path("data") : root.path("data").path("net");
            if (!rows.isArray()) return null;
            for (JsonNode row : rows) {
                if (!symbol.equalsIgnoreCase(row.path("tradingsymbol").asText())) continue;
                if (holding) {
                    // Kite quantity is settled demat inventory. used_quantity is
                    // already committed/sold during the day and must not be made
                    // available for another SELL. T1 and collateral quantities are
                    // intentionally excluded from this sell authorization.
                    JsonNode qty = row.get("quantity");
                    JsonNode used = row.get("used_quantity");
                    if (qty == null || !qty.canConvertToInt()
                            || used == null || !used.canConvertToInt()) return null;
                    return Math.max(0, qty.asInt() - used.asInt());
                }
                if (!"MIS".equalsIgnoreCase(row.path("product").asText())) continue;
                JsonNode qty = row.get("quantity");
                if (qty == null || !qty.canConvertToInt()) return null;
                return Math.max(0, qty.asInt());
            }
            return 0;
        } catch (Exception e) {
            System.err.println("⚠️ Unable to confirm Kite sellable inventory for " + symbol + ": " + e.getMessage());
            return null;
        }
    }

    public Map<String, Double> getPreviousClosePrices(List<String> symbols) {
        Map<String, Double> result = new HashMap<>();
        try {
            if (symbols == null || symbols.isEmpty()) {
                System.err.println("⚠️ getPreviousClosePrices called with empty symbols list");
                return result;
            }

            // Build query string: i=NSE:INFY&i=NSE:TCS...
            StringBuilder query = new StringBuilder();
            for (int i = 0; i < symbols.size(); i++) {
                if (i > 0) query.append("&");
                query.append("i=").append(symbols.get(i));
            }
            String url = kiteConfig.getBaseUrl() + "/quote?" + query;

            // Log URL length to check for potential issues
            System.out.println("🔍 Fetching quotes for " + symbols.size() + " symbols, URL length: " + url.length());

            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).header("X-Kite-Version", "3").header("Authorization", kiteConfig.getAuthHeader()).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            JsonNode json = objectMapper.readTree(response.body());
            String status = json.get("status").asText();

            if (!"success".equals(status)) {
                System.err.println("❌ API returned status: " + status);
                if (json.has("message")) {
                    System.err.println("   Message: " + json.get("message").asText());
                }
                return result;
            }

            JsonNode data = json.get("data");
            if (data == null) {
                System.err.println("❌ API response has no 'data' field");
                return result;
            }

            int foundCount = 0;
            int missingOhlc = 0;
            int missingNode = 0;

            for (String symbol : symbols) {
                JsonNode sNode = data.get(symbol);
                if (sNode == null) {
                    missingNode++;
                    continue;
                }
                JsonNode ohlc = sNode.get("ohlc");
                if (ohlc == null) {
                    missingOhlc++;
                    continue;
                }
                double prevClose = ohlc.get("close").asDouble();
                result.put(symbol, prevClose);
                foundCount++;
            }

            System.out.println("✅ getPreviousClosePrices: " + foundCount + " found, " + missingNode + " missing node, " + missingOhlc + " missing OHLC");

        } catch (Exception e) {
            System.err.println("❌ Exception in getPreviousClosePrices: " + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Get yesterday's high prices for symbols
     */
    public Map<String, Double> getYesterdayHighPrices(List<String> symbols) {
        Map<String, Double> result = new HashMap<>();
        try {
            if (symbols == null || symbols.isEmpty()) {
                return result;
            }

            // Build query string: i=NSE:INFY&i=NSE:TCS...
            StringBuilder query = new StringBuilder();
            for (int i = 0; i < symbols.size(); i++) {
                if (i > 0) query.append("&");
                query.append("i=").append(symbols.get(i));
            }
            String url = kiteConfig.getBaseUrl() + "/quote?" + query;
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).header("X-Kite-Version", "3").header("Authorization", kiteConfig.getAuthHeader()).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            JsonNode json = objectMapper.readTree(response.body());
            if (!"success".equals(json.get("status").asText())) {
                return result;
            }
            JsonNode data = json.get("data");
            for (String symbol : symbols) {
                JsonNode sNode = data.get(symbol);
                if (sNode == null) continue;
                JsonNode ohlc = sNode.get("ohlc");
                if (ohlc == null) continue;
                // OHLC contains high from yesterday
                double yesterdayHigh = ohlc.get("high").asDouble();
                result.put(symbol, yesterdayHigh);
            }
        } catch (Exception e) {
        }
        return result;
    }

    /**
     * Fetch complete quote data including circuit limits and OHLC for given symbols
     * Returns map of symbol -> Instrument with populated circuit limits and OHLC data
     */
    public Map<String, Instrument> getQuoteData(List<String> symbols) {
        Map<String, Instrument> result = new HashMap<>();
        try {
            if (symbols == null || symbols.isEmpty()) {
                return result;
            }

            // Build query string: i=NSE:INFY&i=NSE:TCS...
            StringBuilder query = new StringBuilder();
            for (int i = 0; i < symbols.size(); i++) {
                if (i > 0) query.append("&");
                query.append("i=").append(symbols.get(i));
            }

            String url = kiteConfig.getBaseUrl() + "/quote?" + query;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode json = objectMapper.readTree(response.body());

            if (!"success".equals(json.get("status").asText())) {
                return result;
            }

            JsonNode data = json.get("data");
            for (String symbol : symbols) {
                JsonNode sNode = data.get(symbol);
                if (sNode == null) continue;

                Instrument instrument = new Instrument();
                instrument.setTradingSymbol(symbol.replace("NSE:", ""));

                // Parse circuit limits
                JsonNode upperCircuit = sNode.get("upper_circuit_limit");
                JsonNode lowerCircuit = sNode.get("lower_circuit_limit");
                if (upperCircuit != null && !upperCircuit.isNull()) {
                    instrument.setUpperCircuitLimit(upperCircuit.asDouble());
                }
                if (lowerCircuit != null && !lowerCircuit.isNull()) {
                    instrument.setLowerCircuitLimit(lowerCircuit.asDouble());
                }

                // Parse OHLC data
                JsonNode ohlc = sNode.get("ohlc");
                if (ohlc != null) {
                    JsonNode open = ohlc.get("open");
                    JsonNode high = ohlc.get("high");
                    JsonNode low = ohlc.get("low");
                    JsonNode close = ohlc.get("close");

                    if (open != null && !open.isNull()) {
                        instrument.setTodayOpenPrice(open.asDouble());
                    }
                }

                result.put(instrument.getTradingSymbol(), instrument);
            }
        } catch (Exception e) {
            System.err.println("❌ Error fetching quote data: " + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    public List<Position> getHoldings() {
        List<Position> holdings = new ArrayList<>();

        try {
            // 🚦 Rate limiting
            rateLimiter.acquire("getHoldings");

            // Call Kite API: GET /portfolio/holdings
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/portfolio/holdings"))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            JsonNode json = objectMapper.readTree(response.body());

            // Check if API call was successful
            if (!"success".equals(json.get("status").asText())) {
                return holdings;
            }

            // Parse the data array
            JsonNode data = json.get("data");

            // 🔍 DEBUG: Check data node
            if (data == null) {
                return holdings;
            }

            if (!data.isArray()) {
                return holdings;
            }

            if (data.isArray()) {
                for (JsonNode holdingNode : data) {
                    try {
                        String symbol = holdingNode.get("tradingsymbol").asText();
                        int quantity = holdingNode.get("t1_quantity").asInt() + holdingNode.get("quantity").asInt() + holdingNode.get("collateral_quantity").asInt();
                        double avgPrice = holdingNode.get("average_price").asDouble();

                        // Skip if quantity is 0
                        if (quantity <= 0) {
                            continue;
                        }

                        // Create Position object with HOLDING type
                        // Strategy will be set later when reconciling with existing positions
                        Position position = new Position(
                                symbol,
                                quantity,
                                avgPrice,
                                0, // target will be calculated later
                                true, // open
                                "Dip-Accumulator-Momentum", // strategy placeholder - will be updated during reconciliation
                                null, // entryOrderId
                                null, // stopLossOrderId
                                null, // targetOrderId
                                Position.PositionType.HOLDING
                        );

                        // ===== ENRICHMENT: Restore entryType from local metadata store =====
                        // Try to find metadata for this holding with default strategy first
                        HoldingMetadata metadata = holdingMetadataRepository.get(symbol, "Dip-Accumulator-Momentum");
                        if (metadata != null) {
                            metadata.enrichPosition(position);
                            //System.out.println("✅ Enriched holding with metadata: " + symbol +
                                         //    " | EntryType: " + position.getEntryType() +
                                          //   " | Strategy: " + metadata.getStrategy());
                        }

                        holdings.add(position);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Error fetching holdings: " + e.getMessage());
            e.printStackTrace();
        }
        return holdings;
    }

    /**
     * Fetch current positions (both MIS intraday and net positions) from Kite API
     * Returns only day positions with positive quantity
     *
     * @return List of Position objects representing today's intraday positions
     */
    public List<Position> getPositions() {
        List<Position> positions = new ArrayList<>();

        try {
            // 🚦 Rate limiting
            rateLimiter.acquire("getPositions");

            // Call Kite API: GET /portfolio/positions
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/portfolio/positions"))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode json = objectMapper.readTree(response.body());

            // Check if API call was successful
            if (!"success".equals(json.get("status").asText())) {
                return positions;
            }

            // Parse the data object
            JsonNode data = json.get("data");
            if (data == null) {
                return positions;
            }

            // Get "day" positions (MIS intraday positions)
            JsonNode dayPositions = data.get("day");
            if (dayPositions != null && dayPositions.isArray()) {
                for (JsonNode posNode : dayPositions) {
                    try {
                        String symbol = posNode.get("tradingsymbol").asText();
                        int quantity = posNode.get("quantity").asInt();
                        double avgPrice = posNode.get("average_price").asDouble();
                        String product = posNode.get("product").asText();

                        // Skip if quantity is 0 or if not MIS
                        if (quantity <= 0) {
                            continue;
                        }

                        // Create Position object with INTRADAY type
                        Position position = new Position(
                                symbol,
                                quantity,
                                avgPrice,
                                0, // target will be set during reconciliation
                                true, // open
                                "Dip-Accumulator-Momentum", // default strategy - will be updated during reconciliation
                                null, // entryOrderId
                                null, // stopLossOrderId
                                null, // targetOrderId
                                "MIS".equals(product) ? Position.PositionType.INTRADAY : Position.PositionType.HOLDING
                        );

                        positions.add(position);
                    } catch (Exception ignored) {
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return positions;
    }

    public void convertToHolding(String symbol, int qty) {

        try {
            // 🚦 Rate limiting
            rateLimiter.acquire("convertToHolding");

            String body = "tradingsymbol=" + symbol +
                    "&exchange=NSE" +
                    "&position_type=day" +        // MIS position
                    "&transaction_type=BUY" +
                    "&quantity=" + qty +
                    "&old_product=MIS" +
                    "&new_product=CNC";

            // ===== OPTIMIZATION: Request with timeout to prevent hanging =====
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/portfolio/positions"))
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .timeout(java.time.Duration.ofMillis(3000))  // 3 second timeout for position conversion
                    .PUT(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            // Validate response
            JsonNode json = objectMapper.readTree(response.body());

            // Check for API errors
            if (!"success".equals(json.get("status").asText())) {
                String errorMsg = json.has("message") ? json.get("message").asText() : "Unknown error";

                // ===== CIRCUIT BREAKER: Detect rate limit error from Kite =====
                if (errorMsg.contains("Maximum allowed order requests exceeded") ||
                    errorMsg.contains("Too many requests") ||
                    errorMsg.contains("Rate limit exceeded")) {
                    System.err.println("🚨🚨🚨 KITE RATE LIMIT ERROR DETECTED in convertToHolding! Triggering circuit breaker");
                    rateLimiter.tripCircuitBreaker();
                }

                System.err.println("❌ Kite API Error converting " + symbol + " to holding: " + errorMsg);
                System.err.println("   Full response: " + response.body());
                throw new RuntimeException("Failed to convert to holding: " + errorMsg);
            }

            System.out.println("✅ Successfully converted " + symbol + " (qty=" + qty + ") from MIS to CNC");

        } catch (Exception e) {
            System.err.println("❌ Exception converting " + symbol + " to holding: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to convert to holding", e);
        }
    }


    public List<Order> fetchOrders() {
        List<Order> orders = new ArrayList<>();

        try {
            // 🚦 Rate limiting
            rateLimiter.acquire("getAllOrders");

            // Call Kite API: GET /orders
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/orders"))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode json = objectMapper.readTree(response.body());

            // Check if API call was successful
            if (!"success".equals(json.get("status").asText())) {
                return orders;
            }

            // Parse the data array
            JsonNode data = json.get("data");
            if (data != null && data.isArray()) {
                for (JsonNode orderNode : data) {
                    try {
                        Order order = objectMapper.treeToValue(orderNode, Order.class);
                        orders.add(order);
                    } catch (Exception e) {
                        System.err.println("Failed to parse order: " + e.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return orders;
    }

    public List<Instrument> populateInstrumentsData() {
        List<Instrument> instruments = new ArrayList<>();
        try {

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(kiteConfig.getBaseUrl() + "/instruments"))
                    .header("X-Kite-Version", "3")
                    .header("Authorization", kiteConfig.getAuthHeader())
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            // The response is in CSV format, not JSON
            // Parse CSV using OpenCSV library
            instruments = new CsvToBeanBuilder<Instrument>(new StringReader(response.body()))
                    .withType(Instrument.class)
                    .withIgnoreLeadingWhiteSpace(true)
                    .withFilter(line -> Objects.equals(line[9], "EQ"))
                    .build()
                    .parse();

        } catch (Exception e) {
            System.err.println("❌ Error fetching instruments: " + e.getMessage());
            e.printStackTrace();
        }
        return instruments;
    }

    public Double getMargin(){
        rateLimiter.acquire("getMargin");
        KiteConnect kiteConnect = new KiteConnect(kiteConfig.getApiKey());
        kiteConnect.setAccessToken(kiteConfig.getAccessToken());
        try {
            Margin equityMargin =  kiteConnect.getMargins().get("equity");

            // Get 'net' field
            Field netField =  equityMargin.getClass().getDeclaredField("net");
            netField.setAccessible(true);
            String netValue = (String) netField.get(equityMargin);
            double net = Double.parseDouble(netValue);

            // Get 'available' nested object
            Field availableField =  equityMargin.getClass().getDeclaredField("available");
            availableField.setAccessible(true);
            Object available = availableField.get(equityMargin);

            // Get 'collateral' field from 'available' object
            Field collateralField = available.getClass().getDeclaredField("collateral");
            collateralField.setAccessible(true);
            String collateralValue = (String) collateralField.get(available);
            double collateral = Double.parseDouble(collateralValue);

            // Logic: If (net - collateral) >= 3 lakhs, return net; otherwise return (net - collateral).
            // In LOW cash-flow mode the 3L "return net" rule is skipped entirely —
            // we always return the (net - collateral) difference.
            double difference = net - collateral;
            if (!cashFlowModeService.isSkipNetThreshold() && difference >= 300000) {
                System.out.println("💰 Margin: net=" + net + ", collateral=" + collateral + " → returning net (difference >= 3L)");
                return net;
            } else {
                System.out.println("💰 Margin: net=" + net + ", collateral=" + collateral + " → returning difference=" + difference);
                return difference;
            }
        } catch (Exception | KiteException e) {
            e.printStackTrace();
        }
        return null;
    }
}