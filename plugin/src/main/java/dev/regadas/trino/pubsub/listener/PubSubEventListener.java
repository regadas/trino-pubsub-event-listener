package dev.regadas.trino.pubsub.listener;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Logger;

public class PubSubEventListener implements EventListener, AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PubSubEventListenerConfig config;
    private final Publisher publisher;
    private final Logger logger;

    PubSubEventListener(PubSubEventListenerConfig config, Publisher publisher, Logger logger) {
        this.config = requireNonNull(config, "config is null");
        this.publisher = requireNonNull(publisher, "publisher is null");
        this.logger = requireNonNull(logger, "logger is null");
    }

    public static PubSubEventListener create(PubSubEventListenerConfig config) throws IOException {
        var logger = config.loggerContext().getLogger(PubSubEventListener.class);

        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
        if (config.credentialsFilePath() != null) {
            credentials =
                    GoogleCredentials.fromStream(new FileInputStream(config.credentialsFilePath()));
        }

        var publisher =
                Publisher.newBuilder(config.getTopicName())
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                        .setEnableCompression(true)
                        .build();

        return new PubSubEventListener(config, publisher, logger);
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        if (config.trackQueryCreatedEvent()) {
            publish(queryCreatedEvent);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        if (config.trackQueryCompletedEvent()) {
            publish(queryCompletedEvent);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        if (config.trackSplitCompletedEvent()) {
            publish(splitCompletedEvent);
        }
    }

    <T> PubsubMessage buildMessage(T event) throws JsonProcessingException {
        var eventBytes = MAPPER.writeValueAsBytes(event);
        return PubsubMessage.newBuilder().setData(ByteString.copyFrom(eventBytes)).build();
    }

    <T> void publish(T event) {
        try {
            var future = publisher.publish(buildMessage(event));
            ApiFutures.addCallback(
                    future,
                    new ApiFutureCallback<String>() {
                        public void onSuccess(String id) {
                            logger.debug("published event with id: " + id);
                        }

                        public void onFailure(Throwable t) {
                            logger.error("Failed to publish event", t);
                        }
                    },
                    MoreExecutors.directExecutor());

        } catch (Exception e) {
            logger.error("Failed to publish", e);
        }
    }

    @Override
    public void close() {
        if (publisher != null) {
            publisher.shutdown();

            try {
                publisher.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Failed to shutdown publisher", e);
            }
        }
    }
}
