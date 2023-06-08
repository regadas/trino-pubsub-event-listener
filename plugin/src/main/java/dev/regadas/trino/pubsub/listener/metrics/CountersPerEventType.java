package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

public record CountersPerEventType(
        EventCounters queryCreated, EventCounters queryCompleted, EventCounters splitCompleted) {

    private static final String QUERY_CREATED = "QueryCreated";
    private static final String QUERY_COMPLETED = "QueryCompleted";
    private static final String SPLIT_CREATED = "SplitCreated";

    public CountersPerEventType {
        requireNonNull(queryCreated, "queryCreated is null");
        requireNonNull(queryCompleted, "queryCompleted is null");
        requireNonNull(splitCompleted, "splitCompleted is null");
    }

    public static CountersPerEventType create() {
        return new CountersPerEventType(
                EventCounters.create(QUERY_CREATED),
                EventCounters.create(QUERY_COMPLETED),
                EventCounters.create(SPLIT_CREATED));
    }
}
