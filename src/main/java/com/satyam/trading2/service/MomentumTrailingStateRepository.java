package com.satyam.trading2.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyam.trading2.datamodel.MomentumTrailingState;
import com.satyam.trading2.datamodel.Position;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Atomic file-backed persistence for momentum trailing state across restarts. */
@Component
public class MomentumTrailingStateRepository {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, MomentumTrailingState> states = new ConcurrentHashMap<>();
    private final Object lock = new Object();

    @Value("${momentum.trailing.state.file:/home/ec2-user/momentum_trailing_state.json}")
    private String filePath;

    @PostConstruct
    public void load() {
        Path path = Path.of(filePath);
        if (!Files.exists(path)) return;
        synchronized (lock) {
            try {
                Map<String, MomentumTrailingState> loaded = mapper.readValue(
                        path.toFile(), new TypeReference<Map<String, MomentumTrailingState>>() {});
                if (loaded != null) states.putAll(loaded);
            } catch (Exception e) {
                throw new IllegalStateException("Cannot safely start: momentum trailing state is unreadable at "
                        + path + ". Restore or repair it before enabling momentum trailing exits.", e);
            }
        }
    }

    public MomentumTrailingState getOrCreate(Position position, String tradingDate) {
        String key = position.compositeKey();
        String fingerprint = position.getBuyOrderIds() == null ? ""
                : position.getBuyOrderIds().stream().filter(java.util.Objects::nonNull)
                    .sorted().collect(java.util.stream.Collectors.joining("|"));
        synchronized (lock) {
            MomentumTrailingState state = states.get(key);
            if (state == null || (!tradingDate.equals(state.getTradingDate())
                    && !fingerprint.isEmpty()
                    && !fingerprint.equals(state.getPositionFingerprint()))) {
                state = new MomentumTrailingState();
                state.setPositionKey(key);
                state.setTradingDate(tradingDate);
                state.setPositionFingerprint(fingerprint);
                state.setAverageBuyPrice(position.getAveragePrice());
                state.setTrackedQuantity(position.getTotalQuantity());
                state.setPhase("INACTIVE");
                states.put(key, state);
                persist();
                return state;
            }
            // A same-day accumulation adds BUY order IDs to the position. Keep the
            // existing high-water/stop state, but durably update its fingerprint.
            if (!fingerprint.isEmpty() && !fingerprint.equals(state.getPositionFingerprint())) {
                state.setPositionFingerprint(fingerprint);
                persist();
            }
            return state;
        }
    }

    public MomentumTrailingState find(String key) {
        return states.get(key);
    }

    public void save(MomentumTrailingState state) {
        if (state == null || state.getPositionKey() == null) {
            throw new IllegalArgumentException("Momentum trailing state requires a position key");
        }
        synchronized (lock) {
            states.put(state.getPositionKey(), state);
            persist();
        }
    }

    public void remove(String key) {
        synchronized (lock) {
            states.remove(key);
            persist();
        }
    }

    private void persist() {
        Path target = Path.of(filePath).toAbsolutePath();
        Path parent = target.getParent();
        Path temp = null;
        try {
            if (parent != null) Files.createDirectories(parent);
            temp = Files.createTempFile(parent, target.getFileName().toString(), ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), states);
            try {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            if (temp != null) {
                try { Files.deleteIfExists(temp); } catch (IOException ignored) { }
            }
            throw new IllegalStateException("Failed to persist momentum trailing state to " + target, e);
        }
    }
}
