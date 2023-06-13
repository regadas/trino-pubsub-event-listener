package dev.regadas.trino.pubsub.listener.metrics;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class PubSubEventListenerStats {
    private final EventCounters queryCreated = EventCounters.create();
    private final EventCounters queryCompleted = EventCounters.create();
    private final EventCounters splitCompleted = EventCounters.create();

    @Managed()
    @Nested
    public CounterStat getPublishedQueryCreated() {
        return queryCreated.published();
    }

    @Managed
    @Nested
    public CounterStat getFailedQueryCreated() {
        return queryCreated.failed();
    }

    @Managed
    @Nested
    public CounterStat getPublishedQueryCompleted() {
        return queryCompleted.published();
    }

    @Managed
    @Nested
    public CounterStat getFailedQueryCompleted() {
        return queryCompleted.failed();
    }

    @Managed
    @Nested
    public CounterStat getPublishedSplitCompleted() {
        return splitCompleted.published();
    }

    @Managed
    @Nested
    public CounterStat getFailedSplitCompleted() {
        return splitCompleted.failed();
    }

    public EventCounters getQueryCreated() {
        return queryCreated;
    }

    public EventCounters getQueryCompleted() {
        return queryCompleted;
    }

    public EventCounters getSplitCompleted() {
        return splitCompleted;
    }
}
