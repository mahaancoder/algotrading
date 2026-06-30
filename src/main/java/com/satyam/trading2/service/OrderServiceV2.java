package com.satyam.trading2.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBeanBuilder;
import com.satyam.trading2.config.KiteConfig;
import com.satyam.trading2.datamodel.*;
import com.satyam.trading2.domain.service.OrderSanitizer;
import com.satyam.trading2.infrastructure.ratelimit.RateLimiter;
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

    // ===== PERFORMANCE OPTIMIZATION: HTTP Client with connection pooling and timeouts =====
    // Connection pooling dramatically reduces latency for order placement
    // Aggressive timeouts ensure we don't wait too long for slow responses
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofMillis(500))  // Fast connection timeout
            .version(HttpClient.Version.HTTP_1_1)              // HTTP/1.1 for better broker compatibility
            .build();

    private final ObjectMapper objectMapper = new ObjectMapper();

    // ================= EXIT =================
    public String exitPosition(String symbol, int qty, boolean isHolding, String strategy) {
        String product = isHolding ? "CNC" : "MIS";
        return placeOrder(symbol, "SELL", "LIMIT", qty, 0, 0, product, strategy);
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
    public String placeOrder(String symbol, String side, String type, int qty, double price, double trigger, String product, String strategy) {
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

    public String updateTargetOrder(String symbol, double target, int qty, String oldOrderId, boolean isHolding, String strategy) {
        try {
            String product = isHolding ? "CNC" : "MIS";

            // ✅ FIX: For holdings, fetch ALL open SELL orders for this symbol and cancel them
            // This ensures we don't have duplicate sell orders when accumulating holdings
            if (isHolding) {
                System.out.println("🔍 [updateTargetOrder] Checking for existing SELL orders for holding: " + symbol);
                List<Order> allOrders = fetchOrders();
                List<String> existingSellOrders = allOrders.stream()
                    .filter(o -> symbol.equals(o.getTradingSymbol()))
                    .filter(o -> "SELL".equalsIgnoreCase(o.getTransactionType()))
                    .filter(o -> "CNC".equals(o.getProduct()))
                    .filter(o -> "OPEN".equalsIgnoreCase(o.getStatus()) || "TRIGGER PENDING".equalsIgnoreCase(o.getStatus()))
                    .map(Order::getOrderId)
                    .collect(java.util.stream.Collectors.toList());

                if (!existingSellOrders.isEmpty()) {
                    System.out.println("🔄 [updateTargetOrder] Found " + existingSellOrders.size() + " existing SELL orders for " + symbol + " - cancelling all");
                    for (String existingOrderId : existingSellOrders) {
                        try {
                            System.out.println("   Cancelling order: " + existingOrderId);
                            cancelOrder(existingOrderId);
                        } catch (Exception e) {
                            System.err.println("⚠️ Failed to cancel order " + existingOrderId + ": " + e.getMessage());
                        }
                    }
                    // Wait for all cancellations to process
                    Thread.sleep(800);
                }
            } else {
                // ✅ For intraday: Cancel old target order (if exists)
                if (oldOrderId != null) {
                    try {
                        cancelOrder(oldOrderId);
                        Thread.sleep(500);
                    } catch (Exception e) {
                        System.err.println("⚠️ Failed to cancel old order " + oldOrderId + " for " + symbol + ": " + e.getMessage());
                        // ✅ Check if order still exists after failed cancellation
                        if (isOrderStillActive(oldOrderId)) {
                            System.err.println("❌ Old order " + oldOrderId + " is still ACTIVE - ABORTING to prevent duplicate!");
                            return null; // Don't place new order if old one couldn't be cancelled
                        } else {
                            System.out.println("✅ Old order " + oldOrderId + " is not active anymore - safe to proceed");
                        }
                    }
                }
            }

            // ✅ FIX: Place new LIMIT SELL
            String newOrderId = placeOrder(symbol, "SELL", "LIMIT", qty, target, 0, product, strategy);
            if (newOrderId != null) {
                System.out.println("🎯 Target updated for " + symbol + " → ₹" + String.format("%.2f", target) +
                                 " (qty=" + qty + ", orderId=" + newOrderId + ", product=" + product + ")");
            }
            return newOrderId;

        } catch (Exception e) {
            System.err.println("❌ Failed to update target order for " + symbol + ": " + e.getMessage());
            e.printStackTrace();
            return null; // Return null to indicate failure
        }
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
                            System.out.println("✅ Enriched holding with metadata: " + symbol +
                                             " | EntryType: " + position.getEntryType() +
                                             " | Strategy: " + metadata.getStrategy());
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

            // Logic: If (net - collateral) >= 3 lakhs, return net; otherwise return (net - collateral)
            double difference = net - collateral;
            if (difference >= 300000) {
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