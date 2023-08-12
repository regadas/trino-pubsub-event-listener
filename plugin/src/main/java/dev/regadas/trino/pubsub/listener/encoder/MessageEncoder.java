package dev.regadas.trino.pubsub.listener.encoder;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

@FunctionalInterface
public interface MessageEncoder extends Encoder<Message> {
    JsonFormat.Printer JSON_PRINTER =
            JsonFormat.printer().preservingProtoFieldNames().omittingInsignificantWhitespace();

    static MessageEncoder create(Encoding encoding) {
        return switch (encoding) {
            case JSON -> value -> JSON_PRINTER.print(value).getBytes(UTF_8);
            case PROTO -> Message::toByteArray;
        };
    }
}
