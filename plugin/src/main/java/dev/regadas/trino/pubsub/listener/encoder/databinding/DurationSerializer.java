package dev.regadas.trino.pubsub.listener.encoder.databinding;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonIntegerFormatVisitor;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import java.io.IOException;
import java.io.Serial;
import java.time.Duration;

public class DurationSerializer extends StdScalarSerializer<Duration> {

    @Serial private static final long serialVersionUID = -7605884206539509514L;

    public static final DurationSerializer INSTANCE = new DurationSerializer();

    private DurationSerializer() {
        super(Duration.class);
    }

    @Override
    public void serialize(Duration value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {
        long millis = value.toMillis();
        gen.writeNumber(millis);
    }

    @Override
    public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType typeHint)
            throws JsonMappingException {
        JsonIntegerFormatVisitor v2 = visitor.expectIntegerFormat(typeHint);
        if (v2 != null) {
            v2.numberType(JsonParser.NumberType.LONG);
        }
    }
}
