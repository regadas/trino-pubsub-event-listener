package dev.regadas.trino.pubsub.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.regadas.trino.pubsub.listener.proto.Schema;
import dev.regadas.trino.pubsub.listener.proto.Schema.TrinoWarning.Code;
import io.trino.spi.WarningCode;
import io.trino.spi.eventlistener.ColumnDetail;
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitFailureInfo;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SchemaHelpers {

    private static ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = Logger.getLogger(SchemaHelpers.class.getPackage().getName());

    private static Optional<String> jsonify(Object obj, String prop) {
        try {
            return Optional.of(mapper.writeValueAsString(obj));
        } catch (JsonProcessingException exc) {
            LOG.log(
                    Level.WARNING,
                    "Could not serialize property: "
                            + prop
                            + " with type: "
                            + obj.getClass().getCanonicalName());
            return Optional.empty();
        }
    }

    static Schema.Duration from(java.time.Duration duration) {
        return Schema.Duration.newBuilder()
                .setSeconds(duration.getSeconds())
                .setNanos(duration.getNano())
                .build();
    }

    static Schema.Timestamp from(java.time.Instant instant) {
        return Schema.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    static Schema.QueryContext from(QueryContext context) {
        var contextBuilder =
                Schema.QueryContext.newBuilder()
                        .setUser(context.getUser())
                        .addAllClientTags(context.getClientTags())
                        .addAllClientCapabilities(context.getClientCapabilities())
                        .putAllSessionProperties(context.getSessionProperties())
                        .setServerAddress(context.getServerAddress())
                        .setServerVersion(context.getServerVersion())
                        .setEnvironment(context.getEnvironment())
                        .setRetryPolicy(context.getRetryPolicy());

        context.getPrincipal().ifPresent(contextBuilder::setPrincipal);
        context.getSource().ifPresent(contextBuilder::setSource);
        context.getCatalog().ifPresent(contextBuilder::setCatalog);
        context.getSchema().ifPresent(contextBuilder::setSchema);
        context.getTraceToken().ifPresent(contextBuilder::setTraceToken);
        context.getRemoteClientAddress().ifPresent(contextBuilder::setRemoteClientAddress);
        context.getUserAgent().ifPresent(contextBuilder::setUserAgent);
        context.getClientInfo().ifPresent(contextBuilder::setClientInfo);
        context.getResourceGroupId()
                .map(Object::toString)
                .ifPresent(contextBuilder::setResourceGroupId);
        context.getQueryType().map(Object::toString).ifPresent(contextBuilder::setQueryType);

        return contextBuilder.build();
    }

    static Schema.QueryIOMetadata from(QueryIOMetadata ioMetadata) {
        var inputs = ioMetadata.getInputs().stream().map(SchemaHelpers::from).toList();
        var output = ioMetadata.getOutput().map(SchemaHelpers::from);
        var ioMeta = Schema.QueryIOMetadata.newBuilder().addAllInputs(inputs);
        output.ifPresent(ioMeta::setOutput);
        return ioMeta.build();
    }

    static Schema.QueryInputMetadata from(QueryInputMetadata inputMetadata) {
        var inputMetadataBuilder =
                Schema.QueryInputMetadata.newBuilder()
                        .setCatalogName(inputMetadata.getCatalogName())
                        .setSchema(inputMetadata.getSchema())
                        .setTable(inputMetadata.getTable())
                        .addAllColumns(inputMetadata.getColumns());

        inputMetadata
                .getConnectorInfo()
                .flatMap(connInfo -> SchemaHelpers.jsonify(connInfo, "connectorInfo"))
                .ifPresent(inputMetadataBuilder::setConnectorInfo);

        jsonify(inputMetadata.getConnectorMetrics(), "connectorMetrics")
                .ifPresent(inputMetadataBuilder::setConnectorMetrics);
        inputMetadata.getPhysicalInputRows().ifPresent(inputMetadataBuilder::setPhysicalInputRows);
        inputMetadata
                .getPhysicalInputBytes()
                .ifPresent(inputMetadataBuilder::setPhysicalInputBytes);

        return inputMetadataBuilder.build();
    }

    static Schema.ColumnDetail from(ColumnDetail columnDetail) {
        return Schema.ColumnDetail.newBuilder()
                .setCatalog(columnDetail.getCatalog())
                .setSchema(columnDetail.getSchema())
                .setTable(columnDetail.getTable())
                .setColumnName(columnDetail.getColumnName())
                .build();
    }

    static Schema.OutputColumnMetadata from(OutputColumnMetadata columnMetadata) {
        return Schema.OutputColumnMetadata.newBuilder()
                .setColumnName(columnMetadata.getColumnName())
                .setColumnType(columnMetadata.getColumnType())
                .addAllSourceColumns(
                        columnMetadata.getSourceColumns().stream()
                                .map(SchemaHelpers::from)
                                .toList())
                .build();
    }

    static Schema.QueryOutputMetadata from(QueryOutputMetadata output) {
        var outputMetaBuilder =
                Schema.QueryOutputMetadata.newBuilder()
                        .setCatalogName(output.getCatalogName())
                        .setSchema(output.getSchema())
                        .setTable(output.getTable());

        output.getColumns()
                .map(List::stream)
                .map(s -> s.map(SchemaHelpers::from).toList())
                .ifPresent(outputMetaBuilder::addAllColumns);

        output.getJsonLengthLimitExceeded()
                .ifPresent(outputMetaBuilder::setJsonLengthLimitExceeded);

        return outputMetaBuilder.build();
    }

    static Schema.QueryMetadata from(QueryMetadata metadata) {
        var metadataBuilder =
                Schema.QueryMetadata.newBuilder()
                        .setQueryId(metadata.getQueryId())
                        .setQuery(metadata.getQuery())
                        .setQueryState(metadata.getQueryState())
                        .setUri(metadata.getUri().toString());

        metadataBuilder.addAllTables(
                metadata.getTables().stream()
                        .map(
                                t -> {
                                    var columInfos =
                                            t.getColumns().stream()
                                                    .map(
                                                            c -> {
                                                                var builder =
                                                                        Schema.ColumnInfo
                                                                                .newBuilder()
                                                                                .setColumn(
                                                                                        c
                                                                                                .getColumn());
                                                                c.getMask()
                                                                        .ifPresent(
                                                                                builder::setMask);
                                                                return builder.build();
                                                            })
                                                    .toList();
                                    return Schema.TableInfo.newBuilder()
                                            .setAuthorization(t.getAuthorization())
                                            .setTable(t.getTable())
                                            .setSchema(t.getSchema())
                                            .setCatalog(t.getCatalog())
                                            .addAllFilters(t.getFilters())
                                            .addAllColumns(columInfos)
                                            .setDirectlyReferenced(t.isDirectlyReferenced())
                                            .build();
                                })
                        .toList());
        metadataBuilder.addAllRoutines(
                metadata.getRoutines().stream()
                        .map(
                                r ->
                                        Schema.RoutineInfo.newBuilder()
                                                .setAuthorization(r.getAuthorization())
                                                .setRoutine(r.getRoutine())
                                                .build())
                        .toList());

        metadata.getTransactionId().ifPresent(metadataBuilder::setTransactionId);
        metadata.getUpdateType().ifPresent(metadataBuilder::setUpdateType);
        metadata.getPreparedQuery().ifPresent(metadataBuilder::setPreparedQuery);
        metadata.getPlan().ifPresent(metadataBuilder::setPlan);
        metadata.getJsonPlan().ifPresent(metadataBuilder::setJsonPlan);
        metadata.getPayload().ifPresent(metadataBuilder::setPayload);

        return metadataBuilder.build();
    }

    static Schema.QueryFailureInfo from(QueryFailureInfo info) {
        var errorCode =
                Schema.ErrorCode.newBuilder()
                        .setType(info.getErrorCode().getType().name())
                        .setCode(info.getErrorCode().getCode())
                        .setName(info.getErrorCode().getName())
                        .build();
        var builder =
                Schema.QueryFailureInfo.newBuilder()
                        .setErrorCode(errorCode)
                        .setFailuresJson(info.getFailuresJson());

        info.getFailureType().ifPresent(builder::setFailureType);
        info.getFailureMessage().ifPresent(builder::setFailureMessage);
        info.getFailureTask().ifPresent(builder::setFailureTask);
        info.getFailureHost().ifPresent(builder::setFailureHost);

        return builder.build();
    }

    static Schema.QueryCreatedEvent from(QueryCreatedEvent event) {

        return Schema.QueryCreatedEvent.newBuilder()
                .setCreateTime(from(event.getCreateTime()))
                .setMetadata(from(event.getMetadata()))
                .setContext(from(event.getContext()))
                .build();
    }

    static Schema.QueryCompletedEvent from(QueryCompletedEvent event) {
        var stats = event.getStatistics();
        var gcStats =
                stats.getStageGcStatistics().stream()
                        .map(
                                s ->
                                        Schema.StageGcStatistics.newBuilder()
                                                .setStageId(s.getStageId())
                                                .setTasks(s.getTasks())
                                                .setFullGcTasks(s.getFullGcTasks())
                                                .setMinFullGcSec(s.getMinFullGcSec())
                                                .setMaxFullGcSec(s.getMaxFullGcSec())
                                                .setTotalFullGcSec(s.getTotalFullGcSec())
                                                .setAverageFullGcSec(s.getAverageFullGcSec())
                                                .build())
                        .toList();
        var cpuDistribution =
                stats.getCpuTimeDistribution().stream()
                        .map(
                                d ->
                                        Schema.CpuTimeDistribution.newBuilder()
                                                .setStageId(d.getStageId())
                                                .setTasks(d.getTasks())
                                                .setP25(d.getP25())
                                                .setP50(d.getP50())
                                                .setP75(d.getP75())
                                                .setP90(d.getP90())
                                                .setP95(d.getP95())
                                                .setP99(d.getP99())
                                                .setMin(d.getMin())
                                                .setMax(d.getMax())
                                                .setTotal(d.getTotal())
                                                .setAverage(d.getAverage())
                                                .build())
                        .toList();

        var outputUtilization =
                stats.getOutputBufferUtilization().stream()
                        .map(
                                d ->
                                        Schema.OutputBufferUtilization.newBuilder()
                                                .setStageId(d.getStageId())
                                                .setTasks(d.getTasks())
                                                .setP25(d.getP25())
                                                .setP50(d.getP50())
                                                .setP75(d.getP75())
                                                .setP90(d.getP90())
                                                .setP95(d.getP95())
                                                .setP99(d.getP99())
                                                .setMin(d.getMin())
                                                .setMax(d.getMax())
                                                .setDuration(from(d.getDuration()))
                                                .build())
                        .toList();

        var statsBuilder =
                Schema.QueryStatistics.newBuilder()
                        .setCpuTime(from(stats.getCpuTime()))
                        .setFailedCpuTime(from(stats.getFailedCpuTime()))
                        .setWallTime(from(stats.getWallTime()))
                        .setQueuedTime(from(stats.getQueuedTime()))
                        .setFailedCpuTime(from(stats.getFailedCpuTime()))
                        .setPeakUserMemoryBytes(stats.getPeakUserMemoryBytes())
                        .setPeakTaskUserMemory(stats.getPeakTaskUserMemory())
                        .setPeakTaskTotalMemory(stats.getPeakTaskTotalMemory())
                        .setPhysicalInputBytes(stats.getPhysicalInputBytes())
                        .setPhysicalInputRows(stats.getPhysicalInputRows())
                        .setInternalNetworkBytes(stats.getInternalNetworkBytes())
                        .setInternalNetworkRows(stats.getInternalNetworkRows())
                        .setTotalBytes(stats.getTotalBytes())
                        .setTotalRows(stats.getTotalRows())
                        .setOutputBytes(stats.getOutputBytes())
                        .setOutputRows(stats.getOutputRows())
                        .setWrittenBytes(stats.getWrittenBytes())
                        .setWrittenRows(stats.getWrittenRows())
                        .setCumulativeMemory(stats.getCumulativeMemory())
                        .setFailedCumulativeMemory(stats.getFailedCumulativeMemory())
                        .setCompletedSplits(stats.getCompletedSplits())
                        .setComplete(stats.isComplete())
                        .addAllOperatorSummaries(stats.getOperatorSummaries())
                        .addAllStageGcStatistics(gcStats)
                        .addAllCpuTimeDistribution(cpuDistribution)
                        .addAllOutputBufferUtilization(outputUtilization);

        stats.getScheduledTime().map(SchemaHelpers::from).ifPresent(statsBuilder::setScheduledTime);
        stats.getResourceWaitingTime()
                .map(SchemaHelpers::from)
                .ifPresent(statsBuilder::setResourceWaitingTime);
        stats.getAnalysisTime().map(SchemaHelpers::from).ifPresent(statsBuilder::setAnalysisTime);
        stats.getPlanningTime().map(SchemaHelpers::from).ifPresent(statsBuilder::setPlanningTime);
        stats.getPlanningCpuTime()
                .map(SchemaHelpers::from)
                .ifPresent(statsBuilder::setPlanningCpuTime);
        stats.getExecutionTime().map(SchemaHelpers::from).ifPresent(statsBuilder::setExecutionTime);
        stats.getInputBlockedTime()
                .map(SchemaHelpers::from)
                .ifPresent(statsBuilder::setInputBlockedTime);
        stats.getOutputBlockedTime()
                .map(SchemaHelpers::from)
                .ifPresent(statsBuilder::setOutputBlockedTime);
        stats.getFailedInputBlockedTime()
                .map(SchemaHelpers::from)
                .ifPresent(statsBuilder::setFailedInputBlockedTime);
        stats.getFailedOutputBlockedTime()
                .map(SchemaHelpers::from)
                .ifPresent(statsBuilder::setFailedOutputBlockedTime);
        stats.getPhysicalInputReadTime()
                .map(SchemaHelpers::from)
                .ifPresent(statsBuilder::setPhysicalInputReadTime);

        var warnings =
                event.getWarnings().stream()
                        .map(
                                tw ->
                                        Schema.TrinoWarning.newBuilder()
                                                .setMessage(tw.getMessage())
                                                .setWarningCode(
                                                        toSchemaWarningCode(tw.getWarningCode()))
                                                .build())
                        .toList();

        var ioMeta = SchemaHelpers.from(event.getIoMetadata());

        var builder =
                Schema.QueryCompletedEvent.newBuilder()
                        .setMetadata(from(event.getMetadata()))
                        .setContext(from(event.getContext()))
                        .setStatistics(statsBuilder)
                        .setCreateTime(from(event.getCreateTime()))
                        .setExecutionStartTime(from(event.getExecutionStartTime()))
                        .setEndTime(from(event.getEndTime()))
                        .addAllWarnings(warnings)
                        .setIoMetadata(ioMeta);
        event.getFailureInfo().map(SchemaHelpers::from).ifPresent(builder::setFailureInfo);

        return builder.build();
    }

    private static Code toSchemaWarningCode(WarningCode warningCode) {
        return Code.newBuilder()
                .setCode(warningCode.getCode())
                .setName(warningCode.getName())
                .build();
    }

    @SuppressWarnings("deprecation")
    static Schema.SplitCompletedEvent from(SplitCompletedEvent event) {
        var stats = event.getStatistics();
        var statsBuilder =
                Schema.SplitStatistics.newBuilder()
                        .setCpuTime(from(stats.getCpuTime()))
                        .setWallTime(from(stats.getWallTime()))
                        .setQueuedTime(from(stats.getQueuedTime()))
                        .setCompletedReadTime(from(stats.getCompletedReadTime()))
                        .setCompletedPositions(stats.getCompletedPositions())
                        .setCompletedDataSizeBytes(stats.getCompletedDataSizeBytes());

        stats.getTimeToFirstByte()
                .map(SchemaHelpers::from)
                .ifPresent(statsBuilder::setTimeToFirstByte);
        stats.getTimeToLastByte()
                .map(SchemaHelpers::from)
                .ifPresent(statsBuilder::setTimeToLastByte);

        var failureInfoBuilder = Schema.SplitFailureInfo.newBuilder();
        event.getFailureInfo()
                .map(SplitFailureInfo::getFailureType)
                .ifPresent(failureInfoBuilder::setFailureType);
        event.getFailureInfo()
                .map(SplitFailureInfo::getFailureMessage)
                .ifPresent(failureInfoBuilder::setFailureMessage);

        var builder =
                Schema.SplitCompletedEvent.newBuilder()
                        .setQueryId(event.getQueryId())
                        .setStageId(event.getStageId())
                        .setTaskId(event.getTaskId())
                        .setPayload(event.getPayload())
                        .setStatistics(statsBuilder)
                        .setCreateTime(from(event.getCreateTime()))
                        .setFailureInfo(failureInfoBuilder);

        event.getEndTime().map(SchemaHelpers::from).ifPresent(builder::setEndTime);
        event.getStartTime().map(SchemaHelpers::from).ifPresent(builder::setStartTime);
        event.getCatalogName().ifPresent(builder::setCatalogName);

        return builder.build();
    }
}
