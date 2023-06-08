package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

public record EventCounters(String eventType, Counter succeeded, Counter failed) {

    private static final String SUCCEEDED = "succeeded";
    private static final String FAILED = "failed";

    public EventCounters {
        requireNonNull(succeeded, "succeeded is null");
        requireNonNull(failed, "failed is null");
    }

    public static EventCounters create(String eventType) {
        return new EventCounters(eventType, new Counter(SUCCEEDED), new Counter(FAILED));
    }
}
