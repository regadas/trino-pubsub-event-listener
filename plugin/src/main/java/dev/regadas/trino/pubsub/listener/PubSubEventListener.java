package dev.regadas.trino.pubsub.listener;

import static java.util.Objects.requireNonNull;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.v1.PubsubMessage;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubEventListener implements EventListener, AutoCloseable {
    private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
    private static final Logger LOG =
            Logger.getLogger(PubSubEventListener.class.getPackage().getName());

    private final PubSubEventListenerConfig config;
    private final Publisher publisher;

    PubSubEventListener(PubSubEventListenerConfig config, Publisher publisher) {
        this.config = requireNonNull(config, "config is null");
        this.publisher = requireNonNull(publisher, "publisher is null");
    }

    public static PubSubEventListener create(PubSubEventListenerConfig config) throws IOException {
        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
        if (config.credentialsFilePath() != null) {
            credentials =
                    GoogleCredentials.fromStream(new FileInputStream(config.credentialsFilePath()));
        }

        var publisher =
                Publisher.newBuilder(config.topicName())
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                        .setEnableCompression(true)
                        .build();

        return new PubSubEventListener(config, publisher);
    }

    @Override
    public void queryCreated(QueryCreatedEvent event) {
        if (config.trackQueryCreatedEvent()) {
            publish(SchemaHelpers.from(event));
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        if (config.trackQueryCompletedEvent()) {
            publish(SchemaHelpers.from(event));
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent event) {
        if (config.trackSplitCompletedEvent()) {
            publish(SchemaHelpers.from(event));
        }
    }

    void publish(Message event) {
        try {
            ByteString eventByteString;

            switch (config.messageFormat()) {
                case JSON:
                    eventByteString = ByteString.copyFromUtf8(JSON_PRINTER.print(event));
                    break;
                case PROTO:
                    eventByteString = event.toByteString();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown message format");
            }

            var message = PubsubMessage.newBuilder().setData(eventByteString).build();
            var future = publisher.publish(message);
            ApiFutures.addCallback(
                    future,
                    new ApiFutureCallback<String>() {
                        public void onSuccess(String id) {
                            LOG.log(Level.ALL, "published event with id: " + id);
                        }

                        public void onFailure(Throwable t) {
                            LOG.log(Level.SEVERE, "Failed to publish event", t);
                        }
                    },
                    MoreExecutors.directExecutor());

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Failed to publish", e);
        }
    }

    @Override
    public void close() {
        if (publisher != null) {
            publisher.shutdown();

            try {
                publisher.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.log(Level.SEVERE, "Failed to shutdown publisher", e);
            }
        }
    }
}
