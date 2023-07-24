package dev.regadas.trino.pubsub.listener.metrics;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class PubSubEventListenerStats {
    private final EventCounters queryCreated;
    private final EventCounters queryCompleted;
    private final EventCounters splitCompleted;
    private final EventCounters queryError;
    private final EventCounters queryStages;


    private PubSubEventListenerStats(
            EventCounters queryCreated,
            EventCounters queryCompleted,
            EventCounters splitCompleted,
            EventCounters queryError,
            EventCounters queryStages
            ) {
        this.queryCreated = queryCreated;
        this.queryCompleted = queryCompleted;
        this.splitCompleted = splitCompleted;
        this.queryError = queryError;
        this.queryStages = queryStages;
    }

    public static final PubSubEventListenerStats init() {
        return new PubSubEventListenerStats(
                EventCounters.create(), EventCounters.create(), EventCounters.create(),
                EventCounters.create(), EventCounters.create());
    }

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

    @Managed()
    @Nested
    public RatioStat getFailedRatioQueryCreated() {
        return queryCreated.failedRatio();
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

    @Managed()
    @Nested
    public RatioStat getFailedRatioQueryCompleted() {
        return queryCompleted.failedRatio();
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

    @Managed()
    @Nested
    public RatioStat getFailedRatioSplitCompleted() {
        return splitCompleted.failedRatio();
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

    public EventCounters getQueryError() {
        return queryError;
    }

    public EventCounters getQueryStages() {
        return queryStages;
    }
}
