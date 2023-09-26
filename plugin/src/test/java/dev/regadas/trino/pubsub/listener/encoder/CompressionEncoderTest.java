package dev.regadas.trino.pubsub.listener.encoder;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

import io.airlift.compress.zstd.ZstdInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CompressionEncoderTest {

    private static final String TEXT = Stream.generate(() -> "a").limit(1000).collect(joining());
    private static final Encoder<String> DELEGATE = str -> str.getBytes(UTF_8);
    private CompressionEncoder<String> encoder;

    @BeforeEach
    void setUp() {
        encoder = CompressionEncoder.create(DELEGATE);
    }

    @Test
    void testEncodeActuallyCompress() throws Exception {
        byte[] uncompressed = DELEGATE.encode(TEXT);

        byte[] compressed = encoder.encode(TEXT);

        assertThat(compressed.length, lessThan(uncompressed.length));
    }

    @Test
    void testEncodeCompressionRoundTrip() throws Exception {
        byte[] compressed = encoder.encode(TEXT);

        byte[] decompressed = decompress(compressed);
        assertThat(new String(decompressed), equalTo(TEXT));
    }

    public static byte[] decompress(byte[] uncompressedBytes) throws IOException {
        try (var zin = new ZstdInputStream(new ByteArrayInputStream(uncompressedBytes));
                var bao = new ByteArrayOutputStream()) {
            zin.transferTo(bao);
            return bao.toByteArray();
        }
    }
}
