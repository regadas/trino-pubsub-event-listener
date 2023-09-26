package dev.regadas.trino.pubsub.listener.encoder.databinding;

import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.type.LogicalType;
import java.io.IOException;
import java.io.Serial;
import java.time.Duration;

public class DurationDeserializer extends StdScalarDeserializer<Duration> {
    @Serial private static final long serialVersionUID = 2943940363883858390L;

    public static final DurationDeserializer INSTANCE = new DurationDeserializer();

    private DurationDeserializer() {
        super(Duration.class);
    }

    @Override
    public LogicalType logicalType() {
        return LogicalType.DateTime;
    }

    @Override
    public Duration deserialize(JsonParser p, DeserializationContext context) throws IOException {
        if (p.getCurrentToken() == VALUE_NUMBER_INT) {
            return Duration.ofMillis(p.getLongValue());
        } else {
            return (Duration) context.handleUnexpectedToken(_valueClass, p);
        }
    }
}
