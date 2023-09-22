package dev.regadas.trino.pubsub.listener.encoder.databinding;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.metrics.Metric;
import java.time.Duration;

public class PatchSchemaModule {
    public static Module create() {
        var simpleModule = new SimpleModule();
        // supporting custom serialization / deserialization for Duration
        simpleModule.addDeserializer(Duration.class, DurationDeserializer.INSTANCE);
        simpleModule.addSerializer(Duration.class, DurationSerializer.INSTANCE);

        // supporting custom serialization / deserialization
        simpleModule.setMixInAnnotation(QueryInputMetadata.class, Mixins.QueryInputMetadata.class);
        simpleModule.setMixInAnnotation(Metric.class, Mixins.Metric.class);

        return simpleModule;
    }
}
