package dev.regadas.trino.pubsub.listener.encoder;

import io.airlift.compress.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Objects;

@FunctionalInterface
public interface CompressionEncoder<T> extends Encoder<T> {

    public static <T> CompressionEncoder<T> create(Encoder<T> delegate) {
        Objects.requireNonNull(delegate);

        return value -> {
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
        };
    }
}
