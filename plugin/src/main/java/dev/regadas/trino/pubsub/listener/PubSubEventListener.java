package dev.regadas.trino.pubsub.listener;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.Message;
import dev.regadas.trino.pubsub.listener.metrics.PubSubCounters;
import dev.regadas.trino.pubsub.listener.metrics.PubSubInfo;
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
    private final PubSubInfo pubSubInfo;

    PubSubEventListener(PubSubEventListenerConfig config, Publisher publisher) {
        this.config = requireNonNull(config, "config is null");
        this.publisher = requireNonNull(publisher, "publisher is null");
        this.pubSubInfo = new PubSubInfo(config.projectId(), config.topicId());
    }

    public static PubSubEventListener create(PubSubEventListenerConfig config) throws IOException {
        var publisher =
                PubSubPublisher.create(
                        config.projectId(),
                        config.topicId(),
                        config.encoding(),
                        config.credentialsFilePath());
        return new PubSubEventListener(config, publisher);
    }

    @Override
    public void queryCreated(QueryCreatedEvent event) {
        if (config.trackQueryCreatedEvent()) {
            publish(SchemaHelpers.from(event), pubSubInfo.queryCreated());
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        if (config.trackQueryCompletedEvent()) {
            publish(SchemaHelpers.from(event), pubSubInfo.queryCompleted());
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent event) {
        if (config.trackSplitCompletedEvent()) {
            publish(SchemaHelpers.from(event), pubSubInfo.splitCompleted());
        }
    }

    void publish(Message event, PubSubCounters counters) {
        try {
            counters.attempts().incrementAndGet();
            var future = publisher.publish(event);

            future.whenComplete(
                    (id, t) -> {
                        if (t == null) {
                            counters.successful().incrementAndGet();
                            LOG.log(Level.ALL, "published event with id: " + id);
                        } else {
                            counters.failure().incrementAndGet();
                            LOG.log(Level.SEVERE, "Failed to publish event", t);
                        }
                    });
        } catch (Exception e) {
            counters.failure().incrementAndGet();
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

    public PubSubInfo getPubSubInfo() {
        return pubSubInfo;
    }
}
