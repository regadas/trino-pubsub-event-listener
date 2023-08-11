package dev.regadas.trino.pubsub.listener;

import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.Message;
import dev.regadas.trino.pubsub.listener.Encoder.MessageEncoder;
import dev.regadas.trino.pubsub.listener.proto.Test.TestMessage;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
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
        byte[] uncompressed = DELEGATE.encode(MESSAGE);
        byte[] compressed = encoder.encode(MESSAGE);
        byte[] decompressed = decompress(compressed, uncompressed.length);

        assertThat(TestMessage.parseFrom(decompressed), equalTo(MESSAGE));
    }

    public static byte[] decompress(byte[] input, int expectedLength) throws DataFormatException {
        var decompressor = new Inflater();
        decompressor.setInput(input);

        try {
            var decompressed = new byte[expectedLength];
            decompressor.inflate(decompressed);
            if (!decompressor.finished()) {
                fail("decompressed buffer bigger than expected");
                return null;
            }
            return decompressed;
        } finally {
            decompressor.end();
        }
    }

    static class ProtoMessageEncoder implements MessageEncoder {

        @Override
        public byte[] encode(Message value) {
            return value.toByteArray();
        }
    }
}
