package dev.regadas.trino.pubsub.listener;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import java.io.IOException;
import java.util.Map;

public class PubSubEventListenerFactory implements EventListenerFactory {

    @Override
    public String getName() {
        return "pubsub";
    }

    @Override
    public EventListener create(Map<String, String> config) {
        var listenerConfig = PubSubEventListenerConfig.create(config);
        try {
            return PubSubEventListener.create(listenerConfig);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create PubSubEventListener", e);
        }
    }
}
