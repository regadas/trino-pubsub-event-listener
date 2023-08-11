package dev.regadas.trino.pubsub.listener;

import com.google.protobuf.Message;
import dev.regadas.trino.pubsub.listener.Encoder.MessageEncoder;
import io.airlift.compress.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Objects;

public class CompressingMessageEncoder implements MessageEncoder {

    private final MessageEncoder delegate;

    public CompressingMessageEncoder(MessageEncoder delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public byte[] encode(Message value) throws Exception {
        var uncompressedBytes = delegate.encode(value);
        try (var in = new ByteArrayInputStream(uncompressedBytes);
                var bao = new ByteArrayOutputStream()) {
            // ZstdOutputStream compress and flushes on close,
            // so we wrap it on its own try with resources
            try (var zout = new ZstdOutputStream(bao)) {
                in.transferTo(zout);
            }
            return bao.toByteArray();
        }
    }
}
