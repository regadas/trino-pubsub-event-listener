package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

import io.airlift.stats.CounterStat;
import io.airlift.stats.DecayCounter;
import java.util.List;
import java.util.function.Function;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class RatioStat {
    private final CounterStat specialCounter;
    private final List<CounterStat> restCounters;
    private final DecayRatio oneMinute;
    private final DecayRatio fiveMinute;
    private final DecayRatio fifteenMinute;

    public RatioStat(CounterStat specialCounter, CounterStat restCounter) {
        this(specialCounter, List.of(restCounter));
    }

    public RatioStat(CounterStat specialCounter, List<CounterStat> restCounters) {
        this.specialCounter = requireNonNull(specialCounter);
        this.restCounters = List.copyOf(requireNonNull(restCounters));
        this.oneMinute = getDecayCounters(specialCounter, restCounters, CounterStat::getOneMinute);
        this.fiveMinute =
                getDecayCounters(specialCounter, restCounters, CounterStat::getFiveMinute);
        this.fifteenMinute =
                getDecayCounters(specialCounter, restCounters, CounterStat::getFifteenMinute);
    }

    private static DecayRatio getDecayCounters(
            CounterStat specialCounter,
            List<CounterStat> restCounters,
            Function<CounterStat, DecayCounter> toDecayCounter) {
        return new DecayRatio(
                toDecayCounter.apply(specialCounter),
                restCounters.stream().map(toDecayCounter).toList());
    }

    @Managed
    public double getTotalRatio() {
        long special = specialCounter.getTotalCount();
        long total = restCounters.stream().mapToLong(CounterStat::getTotalCount).sum() + special;
        return special / (double) total;
    }

    @Managed
    @Nested
    public DecayRatio getOneMinute() {
        return oneMinute;
    }

    @Managed
    @Nested
    public DecayRatio getFiveMinute() {
        return fiveMinute;
    }

    @Managed
    @Nested
    public DecayRatio getFifteenMinute() {
        return fifteenMinute;
    }
}
