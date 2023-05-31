package dev.regadas.trino.pubsub.listener.metrics;

public interface PubSubInfoMBean {
    String getProjectId();

    String getTopicId();

    Long getQueryCreatedPublicationAttempts();

    Long getQueryCreatedPublishedSuccessfully();

    Long getQueryCreatedPublicationFailed();

    Long getQueryCompletedPublicationAttempts();

    Long getQueryCompletedPublishedSuccessfully();

    Long getQueryCompletedPublicationFailed();

    Long getSplitCompletedPublicationAttempts();

    Long getSplitCompletedPublishedSuccessfully();

    Long getSplitCompletedPublicationFailed();
}
