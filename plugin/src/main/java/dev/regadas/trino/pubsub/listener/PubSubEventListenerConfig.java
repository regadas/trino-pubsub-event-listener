package dev.regadas.trino.pubsub.listener;

import static java.util.Objects.requireNonNull;

import com.google.api.gax.batching.BatchingSettings;
import com.google.auto.value.AutoBuilder;
import com.google.pubsub.v1.TopicName;
import dev.regadas.trino.pubsub.listener.encoder.Encoding;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.threeten.bp.Duration;

public record PubSubEventListenerConfig(
        boolean trackQueryCreatedEvent,
        boolean trackQueryCompletedEvent,
        boolean trackSplitCompletedEvent,
        String projectId,
        String topicId,
        @Nullable String credentialsFilePath,
        Encoding encoding,
        BatchingSettings batching) {
    private static final String PUBSUB_CREDENTIALS_FILE = "pubsub-event-listener.credentials-file";
    private static final String PUBSUB_TRACK_CREATED = "pubsub-event-listener.log-created";
    private static final String PUBSUB_TRACK_COMPLETED = "pubsub-event-listener.log-completed";
    private static final String PUBSUB_TRACK_COMPLETED_SPLIT = "pubsub-event-listener.log-split";
    private static final String PUBSUB_PROJECT_ID = "pubsub-event-listener.project-id";
    private static final String PUBSUB_TOPIC_ID = "pubsub-event-listener.topic-id";
    private static final String PUBSUB_ENCODING = "pubsub-event-listener.encoding";
    private static final String PUBUSUB_BATCHING_DELAY_THRESHOLD =
            "pubsub-event-listener.batching.delay-threshold";
    private static final String PUBUSUB_BATCHING_REQUEST_BYTE_THRESHOLD =
            "pubsub-event-listener.batching.request-byte-threshold";
    private static final String PUBUSUB_BATCHING_ELEMENT_COUNT_THRESHOLD =
            "pubsub-event-listener.batching.element-count-threshold";

    private static final boolean PUBSUB_TRACK_CREATED_DEFAULT = false;
    private static final boolean PUBSUB_TRACK_COMPLETED_DEFAULT = false;
    private static final boolean PUBSUB_TRACK_COMPLETED_SPLIT_DEFAULT = false;
    private static final Encoding PUBSUB_ENCODING_DEFAULT = Encoding.JSON;
    private static final Duration PUBSUB_BATCHING_DELAY_THRESHOLD_DEFAULT = Duration.ofMillis(1);
    private static final Long PUBSUB_BATCHING_REQUEST_BYTE_THRESHOLD_DEFAULT = 1000L;
    private static final Long PUBSUB_BATCHING_ELEMENT_COUNT_THRESHOLD_DEFAULT = 100L;

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

        Builder credentialsFilePath(@Nullable String credentialsFilePath);

        Builder encoding(Encoding encoding);

        Builder batching(BatchingSettings batching);

        PubSubEventListenerConfig build();
    }

    public static Builder builder() {
        return new AutoBuilder_PubSubEventListenerConfig_Builder()
                .trackQueryCreatedEvent(PUBSUB_TRACK_CREATED_DEFAULT)
                .trackQueryCompletedEvent(PUBSUB_TRACK_COMPLETED_DEFAULT)
                .trackSplitCompletedEvent(PUBSUB_TRACK_COMPLETED_SPLIT_DEFAULT)
                .encoding(PUBSUB_ENCODING_DEFAULT)
                .batching(
                        BatchingSettings.newBuilder()
                                .setDelayThreshold(PUBSUB_BATCHING_DELAY_THRESHOLD_DEFAULT)
                                .setRequestByteThreshold(
                                        PUBSUB_BATCHING_REQUEST_BYTE_THRESHOLD_DEFAULT)
                                .setElementCountThreshold(
                                        PUBSUB_BATCHING_ELEMENT_COUNT_THRESHOLD_DEFAULT)
                                .build());
    }

    public static PubSubEventListenerConfig create(Map<String, String> config) {
        var trackQueryCreatedEvent =
                getConfigValue(config, PUBSUB_TRACK_CREATED)
                        .map(Boolean::parseBoolean)
                        .orElse(PUBSUB_TRACK_CREATED_DEFAULT);
        var trackQueryCompletedEvent =
                getConfigValue(config, PUBSUB_TRACK_COMPLETED)
                        .map(Boolean::parseBoolean)
                        .orElse(PUBSUB_TRACK_COMPLETED_DEFAULT);
        var trackSplitCompletedEvent =
                getConfigValue(config, PUBSUB_TRACK_COMPLETED_SPLIT)
                        .map(Boolean::parseBoolean)
                        .orElse(PUBSUB_TRACK_COMPLETED_SPLIT_DEFAULT);
        var projectId = config.get(PUBSUB_PROJECT_ID);
        var topicId = config.get(PUBSUB_TOPIC_ID);
        var credentialsFilePath = config.get(PUBSUB_CREDENTIALS_FILE);
        var encodingConfig = config.get(PUBSUB_ENCODING);
        var encoding = Encoding.fromOrDefault(encodingConfig, PUBSUB_ENCODING_DEFAULT);
        var batchingDelayThreshold =
                getConfigValue(config, PUBUSUB_BATCHING_DELAY_THRESHOLD)
                        .map(s -> Duration.ofMillis(Long.parseLong(s)))
                        .orElse(PUBSUB_BATCHING_DELAY_THRESHOLD_DEFAULT);
        var batchingRequestByteThreshold =
                getConfigValue(config, PUBUSUB_BATCHING_REQUEST_BYTE_THRESHOLD)
                        .map(Long::parseLong)
                        .orElse(PUBSUB_BATCHING_REQUEST_BYTE_THRESHOLD_DEFAULT);
        var batchingElementCountThreshold =
                getConfigValue(config, PUBUSUB_BATCHING_ELEMENT_COUNT_THRESHOLD)
                        .map(Long::parseLong)
                        .orElse(PUBSUB_BATCHING_ELEMENT_COUNT_THRESHOLD_DEFAULT);

        return builder()
                .trackQueryCreatedEvent(trackQueryCreatedEvent)
                .trackQueryCompletedEvent(trackQueryCompletedEvent)
                .trackSplitCompletedEvent(trackSplitCompletedEvent)
                .projectId(projectId)
                .topicId(topicId)
                .credentialsFilePath(credentialsFilePath)
                .encoding(encoding)
                .batching(
                        BatchingSettings.newBuilder()
                                .setDelayThreshold(batchingDelayThreshold)
                                .setRequestByteThreshold(batchingRequestByteThreshold)
                                .setElementCountThreshold(batchingElementCountThreshold)
                                .build())
                .build();
    }

    private static Optional<String> getConfigValue(Map<String, String> params, String paramName) {
        return Optional.ofNullable(params.get(paramName)).filter(v -> !v.trim().isEmpty());
    }
}
