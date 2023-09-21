package dev.regadas.trino.pubsub.listener.encoder.databinding;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.metrics.Metric;
import java.time.Duration;

public class PatchSchemaModule {
    public static Module create() {
        var simpleModule = new SimpleModule();
        simpleModule.setMixInAnnotation(QueryInputMetadata.class, QueryInputMetadataMixin.class);
        simpleModule.setMixInAnnotation(Metric.class, MetricMixin.class);
        simpleModule.addDeserializer(Duration.class, DurationDeserializer.INSTANCE);
        simpleModule.addSerializer(Duration.class, DurationSerializer.INSTANCE);
        return simpleModule;
    }
}
