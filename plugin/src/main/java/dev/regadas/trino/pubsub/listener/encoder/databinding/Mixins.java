package dev.regadas.trino.pubsub.listener.encoder.databinding;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.avro.annotation.AvroNamespace;
import io.trino.spi.metrics.Metrics;
import java.util.Optional;

public class Mixins {
    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryCompletedEvent {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryInputMetadata {
        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        @JsonSerialize(converter = ConnectorInfoConverter.class)
        public Optional<Object> connectorInfo;

        @JsonSerialize(converter = MetricsConverter.class)
        public Metrics connectorMetrics;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NONE, property = "type")
    public abstract static class Metric {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryContext {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryType {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class ResourceEstimates {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryFailureInfo {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class ErrorCode {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryIOMetadata {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryMetadata {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class ErrorType {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryOutputMetadata {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class RoutineInfo {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class TableInfo {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryStatistics {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class OutputColumnMetadata {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class ColumnInfo {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class StageCpuDistribution {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryPlanOptimizerStatistics {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class StageOutputBufferUtilization {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class StageGcStatistics {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class TrinoWarning {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class ColumnDetail {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class WarningCode {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class QueryCreatedEvent {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class SplitCompletedEvent {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class SplitFailureInfo {}

    @AvroNamespace("dev.regadas.trino.pubsub.listener.event")
    public abstract static class SplitStatistics {}
}
