package dev.regadas.trino.pubsub.listener;

import static java.util.Objects.requireNonNull;

import dev.regadas.trino.pubsub.listener.event.QueryEvent;
import dev.regadas.trino.pubsub.listener.metrics.EventCounters;
import dev.regadas.trino.pubsub.listener.metrics.PubSubEventListenerStats;
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
    private final PubSubEventListenerStats stats;

    private PubSubEventListener(
            PubSubEventListenerConfig config, Publisher publisher, PubSubEventListenerStats stats) {
        this.config = requireNonNull(config, "config is null");
        this.publisher = requireNonNull(publisher, "publisher is null");
        this.stats = requireNonNull(stats, "countersPerEventType is null");
    }

    public static PubSubEventListener create(
            PubSubEventListenerConfig config, PubSubEventListenerStats stats) throws IOException {
        var publisher =
                PubSubPublisher.create(
                        config.projectId(),
                        config.topicId(),
                        config.encoding(),
                        config.credentialsFilePath(),
                        config.batching());
        return create(config, publisher, stats);
    }

    public static PubSubEventListener create(
            PubSubEventListenerConfig config, Publisher publisher, PubSubEventListenerStats stats) {
        return new PubSubEventListener(config, publisher, stats);
    }

    @Override
    public void queryCreated(QueryCreatedEvent event) {
        if (config.trackQueryCreatedEvent()) {
            publish(QueryEvent.queryCreated(event), stats.getQueryCreated());
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        if (config.trackQueryCompletedEvent()) {
            publish(QueryEvent.queryCompleted(event), stats.getQueryCompleted());
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent event) {
        if (config.trackSplitCompletedEvent()) {
            publish(QueryEvent.splitCompleted(event), stats.getSplitCompleted());
        }
    }

    void publish(QueryEvent event, EventCounters counters) {
        try {
            var future = publisher.publish(event);

            future.whenComplete(
                    (id, t) -> {
                        if (t == null) {
                            counters.published().update(1);
                            LOG.log(Level.ALL, "published event with id: " + id);
                        } else {
                            counters.failed().update(1);
                            LOG.log(Level.SEVERE, "Failed to publish event", t);
                        }
                    });
        } catch (Exception e) {
            counters.failed().update(1);
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
