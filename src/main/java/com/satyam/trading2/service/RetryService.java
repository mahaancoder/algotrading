//package com.satyam.trading2.service;
//
//
//import com.satyam.trading2.datamodel.OrderRequest;
//import com.satyam.trading2.datamodel.Position;
//import com.satyam.trading2.datamodel.RetryOrder;
//import com.satyam.trading2.domain.service.OrderExecutor;
//import com.satyam.trading2.domain.service.PositionManager;
//import lombok.RequiredArgsConstructor;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Service;
//
//import java.time.Instant;
//import java.util.Comparator;
//import java.util.Objects;
//import java.util.PriorityQueue;
//
//@Service
//@RequiredArgsConstructor
//public class RetryService {
//
//    private final FailureLogger failureLogger;
//    private final PriorityQueue<RetryOrder> queue = new PriorityQueue<>(Comparator.comparing(RetryOrder::getNextRetryTime));
//    private final OrderExecutor brokerService;
//    private final PositionManager positionManager;
//
//    public void enqueue(OrderRequest request, String error) {
//
//        RetryOrder retryOrder = new RetryOrder();
//        retryOrder.setOrderRequest(request);
//        retryOrder.setRetryCount(0);
//        retryOrder.setNextRetryTime(Instant.now().plusSeconds(2));
//        retryOrder.setLastError(error);
//        retryOrder.setCreatedAt(Instant.now());
//
//        synchronized (queue) {
//            queue.add(retryOrder);
//        }
//    }
//
//    @Scheduled(fixedDelay = 2000)
//    public void processRetries() {
//        while (true) {
//            RetryOrder retryOrder;
//            synchronized (queue) {
//                retryOrder = queue.peek();
//                if (retryOrder == null || retryOrder.getNextRetryTime().isAfter(Instant.now())) {
//                    return;
//                }
//                queue.poll();
//            }
//            tryRetry(retryOrder);
//        }
//    }
//
//    private void tryRetry(RetryOrder retryOrder) {
//        try {
//            if (retryOrder.getRetryCount() > 10) {
//                failureLogger.log(retryOrder.getOrderRequest(), "Max retries exceeded: " + retryOrder.getLastError());
//                return;
//            }
//            OrderRequest request = retryOrder.getOrderRequest();
//            Position position = positionManager.findBySymbolAndStrategy(request.getSymbol(), request.getStrategy());
//            if(position == null) {
//                System.out.println("Position not found for " + request.getSymbol() + " - " + request.getStrategy());
//                return;
//            }
//            if(position.isTargetPlaced()) {
//                System.out.println("⚠️ Order already processed (idempotency check), exiting");
//                return;
//            }
//            String orderId = brokerService.placeTargetOrder(request.getSymbol(), request.getPrice(), request.getQuantity(), null, request.getProduct().equals("CNC"),request.getStrategy());
//            if(orderId != null) {
//                position.setTargetOrderId(orderId);
//                position.setTargetPlaced(true);
//            }
//        } catch (Exception e) {
//            String error = e.getMessage();
//            if (OrderFailureClassifier.isRetryable(error)) {
//                retryOrder.setRetryCount(retryOrder.getRetryCount() + 1);
//                retryOrder.setNextRetryTime(Instant.now().plusSeconds(2));
//                retryOrder.setLastError(error);
//                requeue(retryOrder);
//            } else {
//                failureLogger.log(retryOrder.getOrderRequest(), error);
//            }
//        }
//    }
//
//    private void requeue(RetryOrder order) {
//        synchronized (queue) {
//            queue.add(order);
//        }
//    }
//}