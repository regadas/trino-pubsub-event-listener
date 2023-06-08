package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.AtomicLong;

public class Counter implements CounterMBean {

    private final AtomicLong counter;
    private final String kind;

    public Counter(String kind) {
        this.kind = requireNonNull(kind, "kind is null");
        this.counter = new AtomicLong();
    }

    @Override
    public long getCounter() {
        return counter.get();
    }

    public void increment() {
        counter.incrementAndGet();
    }

    public String kind() {
        return kind;
    }
}
