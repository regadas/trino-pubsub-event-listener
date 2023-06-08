package dev.regadas.trino.pubsub.listener;

import dev.regadas.trino.pubsub.listener.metrics.CountersPerEventType;
import dev.regadas.trino.pubsub.listener.metrics.MBeanRegister;
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
            CountersPerEventType countersPerEventType = CountersPerEventType.create();
            PubSubEventListener eventListener =
                    PubSubEventListener.create(listenerConfig, countersPerEventType);
            MBeanRegister.registerMBean(config, countersPerEventType);

            return eventListener;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create PubSubEventListener", e);
        }
    }
}
