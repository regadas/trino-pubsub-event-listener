package dev.regadas.trino.pubsub.listener;

import dev.regadas.trino.pubsub.listener.metrics.MBeanRegister;
import dev.regadas.trino.pubsub.listener.metrics.PubSubEventListenerStats;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import java.io.IOException;
import java.util.Map;

public final class PubSubEventListenerFactory implements EventListenerFactory {

    @Override
    public String getName() {
        return "pubsub";
    }

    @Override
    public EventListener create(Map<String, String> config) {
        var listenerConfig = PubSubEventListenerConfig.create(config);

        try {
            PubSubEventListenerStats stats = PubSubEventListenerStats.init();
            PubSubEventListener eventListener = PubSubEventListener.create(listenerConfig, stats);
            MBeanRegister.registerMBean(config, stats);

            return eventListener;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create PubSubEventListener", e);
        }
    }
}
