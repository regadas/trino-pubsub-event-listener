package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

public class PubSubInfo implements PubSubInfoMBean {

    private final String projectId;
    private final String topicId;
    private final PubSubCounters queryCreated;
    private final PubSubCounters queryCompleted;
    private final PubSubCounters splitCompleted;

    public PubSubInfo(String projectId, String topicId) {
        this.projectId = requireNonNull(projectId, "projectId is null");
        this.topicId = requireNonNull(topicId, "topicId is null");
        this.queryCreated = new PubSubCounters();
        this.queryCompleted = new PubSubCounters();
        this.splitCompleted = new PubSubCounters();
    }

    @Override
    public String getProjectId() {
        return this.projectId;
    }

    @Override
    public String getTopicId() {
        return this.topicId;
    }

    public PubSubCounters getQueryCreated() {
        return queryCreated;
    }

    public PubSubCounters getQueryCompleted() {
        return queryCompleted;
    }

    public PubSubCounters getSplitCompleted() {
        return splitCompleted;
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
