package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

import io.airlift.stats.CounterStat;
import io.airlift.stats.DecayCounter;
import java.util.List;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

@ThreadSafe
public class RatioStat {
    private final CounterStat numeratorCounter;
    private final List<CounterStat> denominatorCounters;
    private final DecayRatio oneMinute;
    private final DecayRatio fiveMinute;
    private final DecayRatio fifteenMinute;

    public RatioStat(CounterStat numeratorCounter, CounterStat denominatorCounter) {
        this(numeratorCounter, List.of(denominatorCounter));
    }

    public RatioStat(CounterStat numeratorCounter, List<CounterStat> denominatorCounters) {
        this.numeratorCounter = requireNonNull(numeratorCounter);
        this.denominatorCounters = List.copyOf(requireNonNull(denominatorCounters));
        this.oneMinute =
                getDecayCounters(numeratorCounter, denominatorCounters, CounterStat::getOneMinute);
        this.fiveMinute =
                getDecayCounters(numeratorCounter, denominatorCounters, CounterStat::getFiveMinute);
        this.fifteenMinute =
                getDecayCounters(
                        numeratorCounter, denominatorCounters, CounterStat::getFifteenMinute);
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
        long numerator = numeratorCounter.getTotalCount();
        long total =
                denominatorCounters.stream().mapToLong(CounterStat::getTotalCount).sum()
                        + numerator;
        return numerator / (double) total;
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
