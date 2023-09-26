package dev.regadas.trino.pubsub.listener.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import java.util.Arrays;
import java.util.Optional;

// Jackson doesn't play well with @AutoOneOf
@AutoValue
public abstract class QueryEvent {
    public enum Kind {
        QUERY_CREATED,
        QUERY_COMPLETED,
        SPLIT_COMPLETED
    }

    public abstract Kind kind();

    @JsonProperty(value = "queryCreated", defaultValue = "null")
    public abstract Optional<QueryCreatedEvent> queryCreated();

    @JsonProperty(value = "queryCompleted", defaultValue = "null")
    public abstract Optional<QueryCompletedEvent> queryCompleted();

    @JsonProperty(value = "splitCompleted", defaultValue = "null")
    public abstract Optional<SplitCompletedEvent> splitCompleted();

    public static QueryEvent queryCreated(QueryCreatedEvent event) {
        return new AutoValue_QueryEvent.Builder()
                .setKind(Kind.QUERY_CREATED)
                .setQueryCreated(event)
                .build();
    }

    public static QueryEvent queryCompleted(QueryCompletedEvent event) {
        return new AutoValue_QueryEvent.Builder()
                .setKind(Kind.QUERY_COMPLETED)
                .setQueryCompleted(event)
                .build();
    }

    public static QueryEvent splitCompleted(SplitCompletedEvent event) {
        return new AutoValue_QueryEvent.Builder()
                .setKind(Kind.SPLIT_COMPLETED)
                .setSplitCompleted(event)
                .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setKind(Kind kind);

        abstract Builder setQueryCreated(QueryCreatedEvent event);

        abstract Builder setQueryCompleted(QueryCompletedEvent event);

        abstract Builder setSplitCompleted(SplitCompletedEvent event);

        abstract QueryEvent autoBuild();

        final QueryEvent build() {
            var event = autoBuild();
            switch (event.kind()) {
                case QUERY_CREATED -> ensureOneOf(
                        event.queryCreated(), event.queryCompleted(), event.splitCompleted());
                case QUERY_COMPLETED -> ensureOneOf(
                        event.queryCompleted(), event.queryCreated(), event.splitCompleted());
                case SPLIT_COMPLETED -> ensureOneOf(
                        event.splitCompleted(), event.queryCreated(), event.queryCompleted());
            }
            return event;
        }

        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        private void ensureOneOf(Optional<?> mustBePresent, Optional<?>... mustNotBePresent) {
            if (mustBePresent.isEmpty()
                    || Arrays.stream(mustNotBePresent).anyMatch(Optional::isPresent)) {
                throw new IllegalArgumentException("invalid one of");
            }
        }
    }
}
