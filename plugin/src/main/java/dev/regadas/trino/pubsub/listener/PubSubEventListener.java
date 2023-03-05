package dev.regadas.trino.pubsub.listener;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

public class PubSubEventListener implements EventListener {
    // private final PubSubClient pubSubClient;
    // private final PubSubEventListenerConfig config;

    // @Inject
    // public PubSubEventListener(PubSubClient pubSubClient,
    // PubSubEventListenerConfig config) {
    // this.pubSubClient = pubSubClient;
    // this.config = config;
    // }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        // if (config.isPublishQueryCreated()) {
        // pubSubClient.publish(queryCreatedEvent);
        // }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        // if (config.isPublishQueryCompleted()) {
        // pubSubClient.publish(queryCompletedEvent);
        // }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        // if (config.isPublishSplitCompleted()) {
        // pubSubClient.publish(splitCompletedEvent);
        // }
    }
}
