package dev.regadas.trino.pubsub.listener.encoder;

import dev.regadas.trino.pubsub.listener.event.QueryEvent;

public final class QueryEventEncoders {
    private QueryEventEncoders() {
        // prevent instantiation
    }

    public static Encoder<QueryEvent> create(Encoding encoding) {
        return switch (encoding) {
            case JSON -> new JsonQueryEventEncoder();
            case PROTO -> new ProtoQueryEventEncoder();
            case AVRO -> new AvroQueryEventEncoder();
        };
    }
}
