package dev.regadas.trino.pubsub.listener.encoder.databinding;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.spi.eventlistener.ColumnDetail;
import io.trino.spi.eventlistener.ColumnInfo;
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryPlanOptimizerStatistics;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitFailureInfo;
import io.trino.spi.eventlistener.SplitStatistics;
import io.trino.spi.eventlistener.StageCpuDistribution;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.eventlistener.StageOutputBufferUtilization;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.metrics.Metric;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.session.ResourceEstimates;
import java.time.Duration;

public class PatchSchemaModule {
    public static Module create() {
        var simpleModule = new SimpleModule();
        // supporting custom serialization / deserialization for Duration
        simpleModule.addDeserializer(Duration.class, DurationDeserializer.INSTANCE);
        simpleModule.addSerializer(Duration.class, DurationSerializer.INSTANCE);

        // fixing the namespace and custom serialization / deserialization
        simpleModule.setMixInAnnotation(QueryInputMetadata.class, Mixins.QueryInputMetadata.class);
        simpleModule.setMixInAnnotation(Metric.class, Mixins.Metric.class);

        // only for fixing the namespace
        simpleModule.setMixInAnnotation(
                QueryCompletedEvent.class, Mixins.QueryCompletedEvent.class);
        simpleModule.setMixInAnnotation(QueryContext.class, Mixins.QueryContext.class);
        simpleModule.setMixInAnnotation(QueryType.class, Mixins.QueryType.class);
        simpleModule.setMixInAnnotation(ResourceEstimates.class, Mixins.ResourceEstimates.class);
        simpleModule.setMixInAnnotation(QueryFailureInfo.class, Mixins.QueryFailureInfo.class);
        simpleModule.setMixInAnnotation(ErrorCode.class, Mixins.ErrorCode.class);
        simpleModule.setMixInAnnotation(QueryIOMetadata.class, Mixins.QueryIOMetadata.class);
        simpleModule.setMixInAnnotation(QueryMetadata.class, Mixins.QueryMetadata.class);
        simpleModule.setMixInAnnotation(ErrorType.class, Mixins.ErrorType.class);
        simpleModule.setMixInAnnotation(
                QueryOutputMetadata.class, Mixins.QueryOutputMetadata.class);
        simpleModule.setMixInAnnotation(RoutineInfo.class, Mixins.RoutineInfo.class);
        simpleModule.setMixInAnnotation(TableInfo.class, Mixins.TableInfo.class);
        simpleModule.setMixInAnnotation(QueryStatistics.class, Mixins.QueryStatistics.class);
        simpleModule.setMixInAnnotation(
                OutputColumnMetadata.class, Mixins.OutputColumnMetadata.class);
        simpleModule.setMixInAnnotation(ColumnInfo.class, Mixins.ColumnInfo.class);
        simpleModule.setMixInAnnotation(
                StageCpuDistribution.class, Mixins.StageCpuDistribution.class);
        simpleModule.setMixInAnnotation(
                QueryPlanOptimizerStatistics.class, Mixins.QueryPlanOptimizerStatistics.class);
        simpleModule.setMixInAnnotation(
                StageOutputBufferUtilization.class, Mixins.StageOutputBufferUtilization.class);
        simpleModule.setMixInAnnotation(StageGcStatistics.class, Mixins.StageGcStatistics.class);
        simpleModule.setMixInAnnotation(TrinoWarning.class, Mixins.TrinoWarning.class);
        simpleModule.setMixInAnnotation(ColumnDetail.class, Mixins.ColumnDetail.class);
        simpleModule.setMixInAnnotation(WarningCode.class, Mixins.WarningCode.class);
        simpleModule.setMixInAnnotation(QueryCreatedEvent.class, Mixins.QueryCreatedEvent.class);
        simpleModule.setMixInAnnotation(SplitFailureInfo.class, Mixins.SplitFailureInfo.class);
        simpleModule.setMixInAnnotation(SplitStatistics.class, Mixins.SplitStatistics.class);
        simpleModule.setMixInAnnotation(
                SplitCompletedEvent.class, Mixins.SplitCompletedEvent.class);

        return simpleModule;
    }
}
