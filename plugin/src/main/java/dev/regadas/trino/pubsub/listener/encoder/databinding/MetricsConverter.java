package dev.regadas.trino.pubsub.listener.encoder.databinding;

import static dev.regadas.trino.pubsub.listener.encoder.databinding.SafeJsonifier.jsonifyUnquoted;

import com.fasterxml.jackson.databind.util.StdConverter;
import io.trino.spi.metrics.Metrics;

public class MetricsConverter extends StdConverter<Metrics, String> {

    @Override
    public String convert(Metrics value) {
        return jsonifyUnquoted(value.getMetrics(), "connectorMetrics").orElse(null);
    }
}
