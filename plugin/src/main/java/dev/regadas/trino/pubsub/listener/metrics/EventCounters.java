package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

import io.airlift.stats.CounterStat;

public record EventCounters(CounterStat published, CounterStat failed, RatioStat failedRatio) {
    public EventCounters {
        requireNonNull(published, "published is null");
        requireNonNull(failed, "failed is null");
        requireNonNull(failedRatio, "failedRatio is null");
    }

    public static EventCounters create() {
        CounterStat published = new CounterStat();
        CounterStat failed = new CounterStat();
        return new EventCounters(published, failed, new RatioStat(failed, published));
    }
}
