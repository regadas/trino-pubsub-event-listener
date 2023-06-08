package dev.regadas.trino.pubsub.listener;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.Message;
import dev.regadas.trino.pubsub.listener.metrics.CountersPerEventType;
import dev.regadas.trino.pubsub.listener.metrics.EventCounters;
import dev.regadas.trino.pubsub.listener.pubsub.PubSubPublisher;
import dev.regadas.trino.pubsub.listener.pubsub.Publisher;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class PubSubEventListener implements EventListener, AutoCloseable {
    private static final Logger LOG =
            Logger.getLogger(PubSubEventListener.class.getPackage().getName());

    private final PubSubEventListenerConfig config;
    private final Publisher publisher;
    private final CountersPerEventType countersPerEventType;

    PubSubEventListener(
            PubSubEventListenerConfig config,
            Publisher publisher,
            CountersPerEventType countersPerEventType) {
        this.config = requireNonNull(config, "config is null");
        this.publisher = requireNonNull(publisher, "publisher is null");
        this.countersPerEventType =
                requireNonNull(countersPerEventType, "countersPerEventType is null");
    }

    public static PubSubEventListener create(
            PubSubEventListenerConfig config, CountersPerEventType countersPerEventType)
            throws IOException {
        var publisher =
                PubSubPublisher.create(
                        config.projectId(),
                        config.topicId(),
                        config.encoding(),
                        config.credentialsFilePath());
        return new PubSubEventListener(config, publisher, countersPerEventType);
    }

    @Override
    public void queryCreated(QueryCreatedEvent event) {
        if (config.trackQueryCreatedEvent()) {
            publish(SchemaHelpers.from(event), countersPerEventType.queryCreated());
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        if (config.trackQueryCompletedEvent()) {
            publish(SchemaHelpers.from(event), countersPerEventType.queryCompleted());
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent event) {
        if (config.trackSplitCompletedEvent()) {
            publish(SchemaHelpers.from(event), countersPerEventType.splitCompleted());
        }
    }

    void publish(Message event, EventCounters counters) {
        try {
            var future = publisher.publish(event);

            future.whenComplete(
                    (id, t) -> {
                        if (t == null) {
                            counters.succeeded().increment();
                            LOG.log(Level.ALL, "published event with id: " + id);
                        } else {
                            counters.failed().increment();
                            LOG.log(Level.SEVERE, "Failed to publish event", t);
                        }
                    });
        } catch (Exception e) {
            counters.failed().increment();
            LOG.log(Level.SEVERE, "Failed to publish", e);
        }
    }

    @Override
    public void close() {
        try {
            publisher.close();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Failed to shutdown publisher", e);
        }
    }
}
