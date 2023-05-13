package dev.regadas.trino.pubsub.listener;

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
        static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

        static MessageEncoder create(Encoding encoding) {
            return new MessageEncoder() {
                @Override
                public byte[] encode(Message value) throws Exception {
                    switch (encoding) {
                        case JSON:
                            return JSON_PRINTER.print(value).getBytes("UTF-8");
                        case PROTO:
                            return value.toByteArray();
                        default:
                            throw new IllegalArgumentException("Unknown encoding: " + encoding);
                    }
                }
            };
        }
    }
}