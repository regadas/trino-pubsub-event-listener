package dev.regadas.trino.pubsub.listener;

import static java.util.Objects.requireNonNull;

import com.google.auto.value.AutoValue;
import com.google.pubsub.v1.TopicName;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class PubSubEventListenerConfig {
    private static final String PUBSUB_CREDENTIALS_FILE = "pubsub-event-listener.credentials-file";
    private static final String PUBSUB_TRACK_CREATED = "pubsub-event-listener.log-created";
    private static final String PUBSUB_TRACK_COMPLETED = "pubsub-event-listener.log-completed";
    private static final String PUBSUB_TRACK_COMPLETED_SPLIT = "pubsub-event-listener.log-split";
    private static final String PUBSUB_PROJECT_ID = "pubsub-event-listener.project-id";
    private static final String PUBSUB_TOPIC_ID = "pubsub-event-listener.topic-id";
    private static final String PUBSUB_FORMAT = "pubsub-event-listener.message-format";

    enum MessageFormat {
        JSON,
        PROTO
    }

    public abstract boolean trackQueryCreatedEvent();

    public abstract boolean trackQueryCompletedEvent();

    public abstract boolean trackSplitCompletedEvent();

    public abstract String projectId();

    public abstract String topicId();

    @Nullable public abstract String credentialsFilePath();

    public abstract MessageFormat messageFormat();

    public TopicName getTopicName() {
        return TopicName.of(projectId(), topicId());
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder trackQueryCreatedEvent(boolean trackQueryCreatedEvent);

        public abstract Builder trackQueryCompletedEvent(boolean trackQueryCompletedEvent);

        public abstract Builder trackSplitCompletedEvent(boolean trackSplitCompletedEvent);

        public abstract Builder projectId(String projectId);

        public abstract Builder topicId(String topicId);

        public abstract Builder credentialsFilePath(@Nullable String credentialsFilePath);

        public abstract Builder messageFormat(MessageFormat messageFormat);

        public abstract PubSubEventListenerConfig build();
    }

    public static final Builder builder() {
        return new AutoValue_PubSubEventListenerConfig.Builder();
    }

    public static final PubSubEventListenerConfig create(Map<String, String> config) {
        var trackQueryCreatedEvent = getBooleanConfig(config, PUBSUB_TRACK_CREATED).orElse(false);
        var trackQueryCompletedEvent =
                getBooleanConfig(config, PUBSUB_TRACK_COMPLETED).orElse(false);
        var trackSplitCompletedEvent =
                getBooleanConfig(config, PUBSUB_TRACK_COMPLETED_SPLIT).orElse(false);
        var projectId = requireNonNull(config.get(PUBSUB_PROJECT_ID));
        var topicId = requireNonNull(config.get(PUBSUB_TOPIC_ID));
        var credentialsFilePath = config.get(PUBSUB_CREDENTIALS_FILE);
        var messageFormatConfig = config.getOrDefault(PUBSUB_FORMAT, MessageFormat.JSON.name());
        var messageFormat = MessageFormat.valueOf(messageFormatConfig.toUpperCase());

        return builder()
                .trackQueryCreatedEvent(trackQueryCreatedEvent)
                .trackQueryCompletedEvent(trackQueryCompletedEvent)
                .trackSplitCompletedEvent(trackSplitCompletedEvent)
                .projectId(projectId)
                .topicId(topicId)
                .credentialsFilePath(credentialsFilePath)
                .messageFormat(messageFormat)
                .build();
    }

    private static final Optional<Boolean> getBooleanConfig(
            Map<String, String> params, String paramName) {
        return Optional.ofNullable(params.get(paramName))
                .filter(v -> !v.trim().isEmpty())
                .map(Boolean::parseBoolean);
    }
}
