package dev.regadas.trino.pubsub.listener.encoder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import dev.regadas.trino.pubsub.listener.SchemaHelpers;
import dev.regadas.trino.pubsub.listener.TestData;
import dev.regadas.trino.pubsub.listener.proto.Schema.QueryCompletedEvent;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EncoderTest {

    @ParameterizedTest
    @MethodSource("queryCompletedEventProvider")
    void testQueryCompletedEventEncoder(QueryCompletedEvent event) throws Exception {
        var encoded = MessageEncoder.create(Encoding.JSON).encode(event);
        assertTrue(encoded.length > 0);

        var data = ByteString.copyFrom(encoded);
        var builder = QueryCompletedEvent.newBuilder();
        JsonFormat.parser().merge(data.toStringUtf8(), builder);
        assertEquals(event, builder.build());
    }

    private static Stream<Arguments> queryCompletedEventProvider() {
        return Stream.of(
                Arguments.of(SchemaHelpers.from(TestData.MINIMAL_QUERY_COMPLETED_EVENT)),
                Arguments.of(SchemaHelpers.from(TestData.FULL_QUERY_COMPLETED_EVENT)));
    }
}
