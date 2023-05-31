package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.*;

public record PubSubInfo(
        String projectId,
        String topicId,
        PubSubCounters queryCreated,
        PubSubCounters queryCompleted,
        PubSubCounters splitCompleted)
        implements PubSubInfoMBean {

    public PubSubInfo(String projectId, String topicId) {
        this(projectId, topicId, new PubSubCounters(), new PubSubCounters(), new PubSubCounters());
    }

    public PubSubInfo {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(topicId, "topicId is null");
        requireNonNull(queryCreated, "queryCreated is null");
        requireNonNull(queryCompleted, "queryCompleted is null");
        requireNonNull(splitCompleted, "splitCompleted is null");
    }

    @Override
    public String getProjectId() {
        return this.projectId;
    }

    @Override
    public String getTopicId() {
        return this.topicId;
    }

    @Override
    public Long getQueryCreatedPublicationAttempts() {
        return queryCreated.attempts().get();
    }

    @Override
    public Long getQueryCreatedPublishedSuccessfully() {
        return queryCreated.successful().get();
    }

    @Override
    public Long getQueryCreatedPublicationFailed() {
        return queryCreated.failure().get();
    }

    @Override
    public Long getQueryCompletedPublicationAttempts() {
        return queryCompleted.attempts().get();
    }

    @Override
    public Long getQueryCompletedPublishedSuccessfully() {
        return queryCompleted.successful().get();
    }

    @Override
    public Long getQueryCompletedPublicationFailed() {
        return queryCompleted.failure().get();
    }

    @Override
    public Long getSplitCompletedPublicationAttempts() {
        return splitCompleted.attempts().get();
    }

    @Override
    public Long getSplitCompletedPublishedSuccessfully() {
        return splitCompleted.successful().get();
    }

    @Override
    public Long getSplitCompletedPublicationFailed() {
        return splitCompleted.failure().get();
    }
}
