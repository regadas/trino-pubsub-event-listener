package dev.regadas.trino.pubsub.listener.metrics;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.AtomicLong;

public record PubSubCounters(AtomicLong attempts, AtomicLong successful, AtomicLong failure) {

    public PubSubCounters() {
        this(new AtomicLong(), new AtomicLong(), new AtomicLong());
    }

    public PubSubCounters {
        requireNonNull(attempts, "attempts is null");
        requireNonNull(successful, "successful is null");
        requireNonNull(failure, "failure is null");
    }
}
