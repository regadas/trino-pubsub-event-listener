package dev.regadas.trino.pubsub.listener;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.util.Optional;

@FunctionalInterface
public interface Encoder<T> {

    byte[] encode(T value) throws Exception;

    enum Encoding {
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

    @FunctionalInterface
    interface MessageEncoder extends Encoder<Message> {
        JsonFormat.Printer JSON_PRINTER =
                JsonFormat.printer()
                        .preservingProtoFieldNames()
                        .omittingInsignificantWhitespace()
                        .includingDefaultValueFields();

        static MessageEncoder create(Encoding encoding) {
            return switch (encoding) {
                case JSON -> value -> JSON_PRINTER.print(value).getBytes(UTF_8);
                case PROTO -> Message::toByteArray;
            };
        }
    }
}
