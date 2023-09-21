package dev.regadas.trino.pubsub.listener.encoder.databinding;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.trino.spi.metrics.Metrics;
import java.util.Optional;

public abstract class QueryInputMetadataMixin {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @JsonSerialize(converter = ConnectorInfoConverter.class)
    public Optional<Object> connectorInfo;

    @JsonSerialize(converter = MetricsConverter.class)
    public Metrics connectorMetrics;
}
