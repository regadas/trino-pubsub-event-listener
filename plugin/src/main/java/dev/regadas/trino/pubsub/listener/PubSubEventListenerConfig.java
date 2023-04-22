package dev.regadas.trino.pubsub.listener;

import static java.util.Objects.requireNonNull;

import com.google.auto.value.AutoBuilder;
import com.google.pubsub.v1.TopicName;

import java.util.Map;
import java.util.Optional;

public record PubSubEventListenerConfig(
        boolean trackQueryCreatedEvent,
        boolean trackQueryCompletedEvent,
        boolean trackSplitCompletedEvent,
        String projectId,
        String topicId,
        String credentialsFilePath,
        PubSubEventListenerConfig.Encoding encoding) {
    private static final String PUBSUB_CREDENTIALS_FILE = "pubsub-event-listener.credentials-file";
    private static final String PUBSUB_TRACK_CREATED = "pubsub-event-listener.log-created";
    private static final String PUBSUB_TRACK_COMPLETED = "pubsub-event-listener.log-completed";
    private static final String PUBSUB_TRACK_COMPLETED_SPLIT = "pubsub-event-listener.log-split";
    private static final String PUBSUB_PROJECT_ID = "pubsub-event-listener.project-id";
    private static final String PUBSUB_TOPIC_ID = "pubsub-event-listener.topic-id";
    private static final String PUBSUB_ENCODING = "pubsub-event-listener.encoding";

    enum Encoding {
        JSON,
        PROTO;

        public static Optional<Encoding> from(String encoding) {
            if (encoding == null) {
                return Optional.empty();
            }

            return Optional.of(Encoding.valueOf(encoding.toUpperCase()));
        }

        public static Encoding fromOrDefault(String encoding, Encoding defaultEncoding) {
            return from(encoding).orElse(defaultEncoding);
        }
    }

    public PubSubEventListenerConfig {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(topicId, "topicId is null");
    }

    public TopicName topicName() {
        return TopicName.of(projectId(), topicId());
    }

    @AutoBuilder
    public interface Builder {
        Builder trackQueryCreatedEvent(boolean trackQueryCreatedEvent);

        Builder trackQueryCompletedEvent(boolean trackQueryCompletedEvent);

        Builder trackSplitCompletedEvent(boolean trackSplitCompletedEvent);

        Builder projectId(String projectId);

        Builder topicId(String topicId);

        Builder credentialsFilePath(String credentialsFilePath);

        Builder encoding(Encoding encoding);

        PubSubEventListenerConfig build();
    }

    public static final Builder builder() {
        return new AutoBuilder_PubSubEventListenerConfig_Builder();
    }

    public static final PubSubEventListenerConfig create(Map<String, String> config) {
        var trackQueryCreatedEvent = getBooleanConfig(config, PUBSUB_TRACK_CREATED).orElse(false);
        var trackQueryCompletedEvent =
                getBooleanConfig(config, PUBSUB_TRACK_COMPLETED).orElse(false);
        var trackSplitCompletedEvent =
                getBooleanConfig(config, PUBSUB_TRACK_COMPLETED_SPLIT).orElse(false);
        var projectId = config.get(PUBSUB_PROJECT_ID);
        var topicId = config.get(PUBSUB_TOPIC_ID);
        var credentialsFilePath = config.get(PUBSUB_CREDENTIALS_FILE);
        var encodingConfig = config.get(PUBSUB_ENCODING);
        var encoding = Encoding.fromOrDefault(encodingConfig, Encoding.JSON);

        return builder()
                .trackQueryCreatedEvent(trackQueryCreatedEvent)
                .trackQueryCompletedEvent(trackQueryCompletedEvent)
                .trackSplitCompletedEvent(trackSplitCompletedEvent)
                .projectId(projectId)
                .topicId(topicId)
                .credentialsFilePath(credentialsFilePath)
                .encoding(encoding)
                .build();
    }

    private static final Optional<Boolean> getBooleanConfig(
            Map<String, String> params, String paramName) {
        return Optional.ofNullable(params.get(paramName))
                .filter(v -> !v.trim().isEmpty())
                .map(Boolean::parseBoolean);
    }
}
