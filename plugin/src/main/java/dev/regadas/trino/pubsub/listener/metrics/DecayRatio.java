package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

import io.airlift.stats.DecayCounter;
import java.util.List;
import java.util.function.Function;
import org.weakref.jmx.Managed;

public final class DecayRatio {
    private final DecayCounter numeratorCounter;
    private final List<DecayCounter> denominatorCounters;

    public DecayRatio(DecayCounter numeratorCounter, DecayCounter denominatorCounter) {
        this(numeratorCounter, List.of(denominatorCounter));
    }

    public DecayRatio(DecayCounter numeratorCounter, List<DecayCounter> denominatorCounters) {
        this.numeratorCounter = requireNonNull(numeratorCounter);
        this.denominatorCounters = requireNonNull(denominatorCounters);
    }

    @Managed
    public synchronized double getCount() {
        return computeRatio(DecayCounter::getCount);
    }

    @Managed
    public synchronized double getRate() {
        return computeRatio(DecayCounter::getRate);
    }

    private double computeRatio(Function<DecayCounter, Double> toDouble) {
        double numerator = toDouble.apply(numeratorCounter);
        double total = denominatorCounters.stream().mapToDouble(toDouble::apply).sum() + numerator;
        return numerator / total;
    }
}
