package dev.regadas.trino.pubsub.listener.encoder;

import dev.regadas.trino.pubsub.listener.event.QueryEvent;

@FunctionalInterface
public interface QueryEventEncoder extends Encoder<QueryEvent> {
    static QueryEventEncoder create(Encoding encoding) {
        return switch (encoding) {
            case JSON -> new JsonQueryEventEncoder();
            case PROTO -> new ProtoQueryEventEncoder();
            case AVRO -> throw new AssertionError("not yet implemented");
        };
    }
}
