package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

import io.airlift.stats.DecayCounter;
import java.util.List;
import org.weakref.jmx.Managed;

public final class DecayRatio {
    private final DecayCounter specialCounter;
    private final List<DecayCounter> restCounters;

    public DecayRatio(DecayCounter specialCounter, DecayCounter restCounter) {
        this(specialCounter, List.of(restCounter));
    }

    public DecayRatio(DecayCounter specialCounter, List<DecayCounter> restCounters) {
        this.specialCounter = requireNonNull(specialCounter);
        this.restCounters = requireNonNull(restCounters);
    }

    @Managed
    public synchronized double getCount() {
        double special = specialCounter.getCount();
        double total = restCounters.stream().mapToDouble(DecayCounter::getCount).sum() + special;
        return special / total;
    }

    @Managed
    public synchronized double getRate() {
        double special = specialCounter.getRate();
        double total = restCounters.stream().mapToDouble(DecayCounter::getRate).sum() + special;
        return special / total;
    }
}
