package dev.regadas.trino.pubsub.listener;

import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

import com.google.protobuf.Message;
import dev.regadas.trino.pubsub.listener.Encoder.MessageEncoder;
import dev.regadas.trino.pubsub.listener.proto.Test.TestMessage;
import io.airlift.compress.zstd.ZstdInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CompressingMessageEncoderTest {

    private static final String TEXT = Stream.generate(() -> "a").limit(1000).collect(joining());
    private static final TestMessage MESSAGE = TestMessage.newBuilder().setText(TEXT).build();
    private static final ProtoMessageEncoder DELEGATE = new ProtoMessageEncoder();
    private CompressingMessageEncoder encoder;

    @BeforeEach
    void setUp() {
        encoder = new CompressingMessageEncoder(DELEGATE);
    }

    @Test
    void testEncodeActuallyCompress() throws Exception {
        byte[] uncompressed = DELEGATE.encode(MESSAGE);

        byte[] compressed = encoder.encode(MESSAGE);

        assertThat(compressed.length, lessThan(uncompressed.length));
    }

    @Test
    void testEncodeCompressionRoundTrip() throws Exception {
        byte[] compressed = encoder.encode(MESSAGE);

        byte[] decompressed = decompress(compressed);
        assertThat(TestMessage.parseFrom(decompressed), equalTo(MESSAGE));
    }

    public static byte[] decompress(byte[] uncompressedBytes) throws IOException {
        try (var zin = new ZstdInputStream(new ByteArrayInputStream(uncompressedBytes));
                var bao = new ByteArrayOutputStream()) {
            zin.transferTo(bao);
            return bao.toByteArray();
        }
    }

    static class ProtoMessageEncoder implements MessageEncoder {

        @Override
        public byte[] encode(Message value) {
            return value.toByteArray();
        }
    }
}
