package org.apache.nifi.processors.xmpp;

import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stanza.model.Presence;
import rocks.xmpp.extensions.muc.model.DiscussionHistory;

import java.util.concurrent.Future;
import java.util.function.Consumer;

public class RocksChatRoom implements ChatRoom {

    private final rocks.xmpp.extensions.muc.ChatRoom chatRoom;

    public RocksChatRoom(rocks.xmpp.extensions.muc.ChatRoom chatRoom) {
        this.chatRoom = chatRoom;
    }

    @Override
    public Future<Presence> enter(String nick, DiscussionHistory history) {
        return chatRoom.enter(nick, history);
    }

    @Override
    public Future<Void> exit() {
        return chatRoom.exit();
    }

    @Override
    public Future<Void> sendMessage(String message) {
        return chatRoom.sendMessage(message);
    }

    @Override
    public void addInboundMessageListener(Consumer<MessageEvent> messageListener) {
        chatRoom.addInboundMessageListener(messageListener);
    }
}
