package dev.regadas.trino.pubsub.listener.encoder;

import java.util.Optional;

public enum Encoding {
    JSON,
    PROTO;

    public static Optional<Encoding> from(String encoding) {
        if (encoding == null) {
            return Optional.empty();
        }

        return Optional.of(Encoding.valueOf(encoding.toUpperCase()));
    }

    public static Encoding fromOrDefault(String encoding, Encoding defaultEncoding) {
        return from(encoding).orElse(defaultEncoding);
    }
}
