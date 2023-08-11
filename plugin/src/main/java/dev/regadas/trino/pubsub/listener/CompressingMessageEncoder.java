package dev.regadas.trino.pubsub.listener;

import com.google.protobuf.Message;
import dev.regadas.trino.pubsub.listener.Encoder.MessageEncoder;
import java.io.ByteArrayOutputStream;
import java.util.Objects;
import java.util.zip.Deflater;

public class CompressingMessageEncoder implements MessageEncoder {

    private static final int BUFFER_SIZE = 32 * 1024;
    private static final int COMPRESSION_LEVEL = Deflater.BEST_COMPRESSION;
    private final MessageEncoder delegate;

    public CompressingMessageEncoder(MessageEncoder delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public byte[] encode(Message value) throws Exception {
        var uncompressedBytes = delegate.encode(value);
        var compressor = new Deflater(COMPRESSION_LEVEL);
        try {
            compressor.setInput(uncompressedBytes);
            compressor.finish();

            var bao = new ByteArrayOutputStream();
            var readBuffer = new byte[BUFFER_SIZE];
            while (!compressor.finished()) {
                var readCount = compressor.deflate(readBuffer);
                if (readCount > 0) {
                    bao.write(readBuffer, 0, readCount);
                }
            }
            return bao.toByteArray();
        } finally {
            compressor.end();
        }
    }
}
