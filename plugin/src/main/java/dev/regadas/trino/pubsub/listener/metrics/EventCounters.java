package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

import io.airlift.stats.CounterStat;

public record EventCounters(CounterStat published, CounterStat failed) {
    public EventCounters {
        requireNonNull(published, "published is null");
        requireNonNull(failed, "failed is null");
    }

    public static EventCounters create() {
        return new EventCounters(new CounterStat(), new CounterStat());
    }
}
