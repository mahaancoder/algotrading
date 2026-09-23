package com.satyam.trading2.service;

import com.satyam.trading2.datamodel.OrderRequest;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

@Component
public class FailureLogger {

    private final String filePath = "home/ec2-user/order_failures.csv";

    public synchronized void log(OrderRequest request, String error) {
        try (BufferedWriter writer = Files.newBufferedWriter(Path.of(filePath), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(format(request, error));
            writer.newLine();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String format(OrderRequest request, String error) {
        return String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%.2f\",\"%s\",\"%s\"",
                Instant.now(),
                request.getExchange(),
                request.getSymbol(),
                request.getSide(),
                request.getOrderType(),
                request.getQuantity(),
                request.getPrice(),
                request.getTriggerPrice() == null ? "" : request.getTriggerPrice(),
                sanitize(error)
        );
    }

    private String sanitize(String input) {
        if (input == null) return "";
        return input.replace(",", " ").replace("\n", " ").replace("\r", " ");
    }
}