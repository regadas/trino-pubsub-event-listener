package dev.regadas.trino.pubsub.listener.metrics;

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class PubSubInfo implements PubSubInfoMBean {

    public static final PubSubInfo create(String projectId, String topicId) {
        return new AutoValue_PubSubInfo.Builder()
                .setProjectId(projectId)
                .setTopicId(topicId)
                .setQueryCreated(new PubSubCounters())
                .setQueryCompleted(new PubSubCounters())
                .setSplitCompleted(new PubSubCounters())
                .build();
    }

    @Override
    public abstract String getProjectId();

    @Override
    public abstract String getTopicId();

    public abstract PubSubCounters getQueryCreated();

    public abstract PubSubCounters getQueryCompleted();

    public abstract PubSubCounters getSplitCompleted();

    @Override
    public Long getQueryCreatedPublicationAttempts() {
        return getQueryCreated().attempts().get();
    }

    @Override
    public Long getQueryCreatedPublishedSuccessfully() {
        return getQueryCreated().successful().get();
    }

    @Override
    public Long getQueryCreatedPublicationFailed() {
        return getQueryCreated().failure().get();
    }

    @Override
    public Long getQueryCompletedPublicationAttempts() {
        return getQueryCompleted().attempts().get();
    }

    @Override
    public Long getQueryCompletedPublishedSuccessfully() {
        return getQueryCompleted().successful().get();
    }

    @Override
    public Long getQueryCompletedPublicationFailed() {
        return getQueryCompleted().failure().get();
    }

    @Override
    public Long getSplitCompletedPublicationAttempts() {
        return getSplitCompleted().attempts().get();
    }

    @Override
    public Long getSplitCompletedPublishedSuccessfully() {
        return getSplitCompleted().successful().get();
    }

    @Override
    public Long getSplitCompletedPublicationFailed() {
        return getSplitCompleted().failure().get();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setProjectId(String projectId);

        public abstract Builder setTopicId(String topicId);

        public abstract Builder setQueryCreated(PubSubCounters queryCreated);

        public abstract Builder setQueryCompleted(PubSubCounters queryCompleted);

        public abstract Builder setSplitCompleted(PubSubCounters splitCompleted);

        public abstract PubSubInfo build();
    }
}
