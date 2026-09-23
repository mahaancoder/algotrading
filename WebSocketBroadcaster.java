package com.satyam.trading2.websocket;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class WebSocketBroadcaster {

    private final List<WebSocketSession> sessions = new CopyOnWriteArrayList<>();

    public void addSession(WebSocketSession session) {
        sessions.add(session);
    }

    public void removeSession(WebSocketSession session) {
        sessions.remove(session);
    }

    public void broadcast(String message) {
        for (WebSocketSession session : sessions) {
            try {
                if (session.isOpen()) {
                    // Synchronize on the session to prevent concurrent writes
                    // This fixes the TEXT_PARTIAL_WRITING state error
                    synchronized (session) {
                        if (session.isOpen()) {  // Double-check after acquiring lock
                            session.sendMessage(new TextMessage(message));
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Broadcast failed: " + e.getMessage());
            }
        }
//        System.out.println("Broadcasted to " + sessions.size() + " sessions");
    }
}

