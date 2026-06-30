package com.satyam.trading2.websocket;

import com.satyam.trading2.infrastructure.messaging.SnapshotService;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Component
@RequiredArgsConstructor
public class LiveDataHandler extends TextWebSocketHandler {

    private final SnapshotService snapshotService;
    private final WebSocketBroadcaster broadcaster;

    @Override
    public void afterConnectionEstablished(@NotNull WebSocketSession session) throws Exception {
        broadcaster.addSession(session);
        snapshotService.sendFullSnapshot(session);
    }

    @Override
    public void afterConnectionClosed(
            WebSocketSession session,
            CloseStatus status
    ){
        broadcaster.removeSession(session);
    }
}