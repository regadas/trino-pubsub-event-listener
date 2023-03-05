package dev.regadas.trino.pubsub.listener;

import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListenerFactory;

import static java.util.Collections.singletonList;

public class PubSubEventListenerPlugin implements Plugin {
    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories() {
        return singletonList(new PubSubEventListenerFactory());
    }
}