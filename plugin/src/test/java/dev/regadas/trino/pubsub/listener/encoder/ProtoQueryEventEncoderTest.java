package dev.regadas.trino.pubsub.listener.encoder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import dev.regadas.trino.pubsub.listener.TestData;
import dev.regadas.trino.pubsub.listener.proto.Schema;
import io.trino.spi.eventlistener.*;
import java.time.Instant;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;

class ProtoQueryEventEncoderTest {

    private void fromQueryContext(QueryContext context) {
        var schema = ProtoQueryEventEncoder.from(context);

        assertThat(schema.getSchema(), equalTo(context.getSchema().orElse("")));
        assertThat(schema.getCatalog(), equalTo(context.getCatalog().orElse("")));
        assertThat(schema.getPrincipal(), equalTo(context.getPrincipal().orElse("")));
        assertThat(schema.getSource(), equalTo(context.getSource().orElse("")));
        assertThat(
                schema.getRemoteClientAddress(),
                equalTo(context.getRemoteClientAddress().orElse("")));
        assertThat(schema.getUserAgent(), equalTo(context.getUserAgent().orElse("")));
        assertThat(schema.getTraceToken(), equalTo(context.getTraceToken().orElse("")));
    }

    private void fromQueryMetadata(QueryMetadata metadata) {
        var schema = ProtoQueryEventEncoder.from(metadata);

        assertThat(schema.getQueryId(), is(metadata.getQueryId()));
        assertThat(schema.getQuery(), is(metadata.getQuery()));
        assertThat(schema.getQueryState(), is(metadata.getQueryState()));
    }

    private void fromQueryInputMetadata(QueryInputMetadata inputMetadata) {
        var schema = ProtoQueryEventEncoder.from(inputMetadata);
        assertThat(schema.getSchema(), is(inputMetadata.getSchema()));
        assertThat(schema.getCatalogName(), is(inputMetadata.getCatalogName()));

        assertThat(schema.getConnectorMetrics(), is(notNullValue()));

        inputMetadata
                .getConnectorInfo()
                .ifPresent(expected -> assertThat(schema.getConnectorInfo(), is(notNullValue())));
        inputMetadata
                .getPhysicalInputBytes()
                .ifPresent(expected -> assertThat(schema.getPhysicalInputBytes(), is(expected)));
        inputMetadata
                .getPhysicalInputRows()
                .ifPresent(expected -> assertThat(schema.getPhysicalInputRows(), is(expected)));
    }

    private void fromOutputColumnMetadata(OutputColumnMetadata outputColumnMetadata) {
        var schema = ProtoQueryEventEncoder.from(outputColumnMetadata);

        assertThat(schema.getColumnName(), is(outputColumnMetadata.getColumnName()));
        assertThat(schema.getColumnType(), is(outputColumnMetadata.getColumnType()));
        outputColumnMetadata.getSourceColumns().forEach(this::fromColumnDetail);
    }

    private void fromQueryOutputMetadata(QueryOutputMetadata outputMetadata) {
        var schema = ProtoQueryEventEncoder.from(outputMetadata);

        assertThat(outputMetadata.getSchema(), is(schema.getSchema()));
        assertThat(outputMetadata.getTable(), is(schema.getTable()));
        assertThat(outputMetadata.getCatalogName(), is(schema.getCatalogName()));

        outputMetadata.getColumns().ifPresent(cols -> cols.forEach(this::fromOutputColumnMetadata));
        outputMetadata
                .getConnectorOutputMetadata()
                .ifPresent(
                        expected ->
                                assertThat(
                                        schema.getConnectorOutputMetadata(), is(notNullValue())));
        outputMetadata
                .getJsonLengthLimitExceeded()
                .ifPresent(
                        expected -> assertThat(schema.getJsonLengthLimitExceeded(), is(expected)));
    }

    private void fromColumnDetail(ColumnDetail columnDetail) {
        var schema = ProtoQueryEventEncoder.from(columnDetail);
        assertThat(schema.getSchema(), is(columnDetail.getSchema()));
        assertThat(schema.getCatalog(), is(columnDetail.getCatalog()));
        assertThat(schema.getTable(), is(columnDetail.getTable()));
        assertThat(schema.getColumnName(), is(columnDetail.getColumnName()));
    }

    private void fromQueryIoMetadata(QueryIOMetadata metadata) {
        var schema = ProtoQueryEventEncoder.from(metadata);

        assertThat(schema.getInputsCount(), is(metadata.getInputs().size()));
        metadata.getInputs().forEach(this::fromQueryInputMetadata);
        metadata.getOutput().ifPresent(this::fromQueryOutputMetadata);
    }

    private void fromQueryFailureInfo(QueryFailureInfo info) {
        var schema = ProtoQueryEventEncoder.from(info);

        assertThat(schema.getErrorCode().getCode(), is(info.getErrorCode().getCode()));
        assertThat(schema.getErrorCode().getName(), is(info.getErrorCode().getName()));
        assertThat(schema.getErrorCode().getType(), is(info.getErrorCode().getType().name()));
        assertThat(schema.getFailureType(), equalTo(info.getFailureType().orElse("")));
        assertThat(schema.getFailureMessage(), equalTo(info.getFailureMessage().orElse("")));
        assertThat(schema.getFailureTask(), equalTo(info.getFailureTask().orElse("")));
        assertThat(schema.getFailureHost(), equalTo(info.getFailureHost().orElse("")));
        assertThat(schema.getFailuresJson(), is(info.getFailuresJson()));
    }

    private void fromQueryCompletedEvent(QueryCompletedEvent event) {
        var schema = ProtoQueryEventEncoder.from(event);

        assertThat(schema.getCreateTime(), timestampEqualTo(event.getCreateTime()));
        assertThat(schema.getExecutionStartTime(), timestampEqualTo(event.getExecutionStartTime()));
        assertThat(schema.getEndTime(), timestampEqualTo(event.getEndTime()));
        assertThat(schema.getWarningsCount(), is(event.getWarnings().size()));

        fromQueryContext(event.getContext());
        fromQueryMetadata(event.getMetadata());
        fromQueryIoMetadata(event.getIoMetadata());
        event.getFailureInfo().ifPresent(this::fromQueryFailureInfo);
    }

    @SuppressWarnings("deprecation")
    private void fromSplitCompletedEvent(SplitCompletedEvent event) {
        var schema = ProtoQueryEventEncoder.from(event);

        assertThat(schema.getQueryId(), is(event.getQueryId()));
        assertThat(schema.getStageId(), is(event.getStageId()));
        assertThat(schema.getTaskId(), is(event.getTaskId()));
        assertThat(schema.getCatalogName(), equalTo(event.getCatalogName().orElse("")));

        assertThat(schema.getCreateTime(), timestampEqualTo(event.getCreateTime()));
        assertThat(
                schema.getStartTime(),
                timestampEqualTo(event.getStartTime().orElse(Instant.EPOCH)));
        assertThat(schema.getEndTime(), timestampEqualTo(event.getEndTime().orElse(Instant.EPOCH)));

        assertThat(
                schema.getStatistics().getCpuTime(),
                durationEqualTo(event.getStatistics().getCpuTime()));
        assertThat(
                schema.getStatistics().getWallTime(),
                durationEqualTo(event.getStatistics().getWallTime()));
        assertThat(
                schema.getStatistics().getQueuedTime(),
                durationEqualTo(event.getStatistics().getQueuedTime()));
        assertThat(
                schema.getStatistics().getCompletedReadTime(),
                durationEqualTo(event.getStatistics().getCompletedReadTime()));

        assertThat(
                schema.getStatistics().getCompletedPositions(),
                is(event.getStatistics().getCompletedPositions()));
        assertThat(
                schema.getStatistics().getCompletedDataSizeBytes(),
                is(event.getStatistics().getCompletedDataSizeBytes()));

        assertThat(
                schema.getStatistics().getTimeToFirstByte(),
                durationEqualTo(
                        event.getStatistics()
                                .getTimeToFirstByte()
                                .orElse(java.time.Duration.ZERO)));
        assertThat(
                schema.getStatistics().getTimeToLastByte(),
                durationEqualTo(
                        event.getStatistics().getTimeToLastByte().orElse(java.time.Duration.ZERO)));

        assertThat(
                schema.getFailureInfo().getFailureType(),
                equalTo(event.getFailureInfo().map(SplitFailureInfo::getFailureType).orElse("")));
        assertThat(
                schema.getFailureInfo().getFailureMessage(),
                equalTo(
                        event.getFailureInfo()
                                .map(SplitFailureInfo::getFailureMessage)
                                .orElse("")));

        assertThat(schema.getPayload(), is(event.getPayload()));
    }

    private void fromQueryCreatedEvent(QueryCreatedEvent event) {
        var schema = ProtoQueryEventEncoder.from(event);

        assertThat(schema.getCreateTime(), timestampEqualTo(event.getCreateTime()));
        fromQueryContext(event.getContext());
        fromQueryMetadata(event.getMetadata());
    }

    @Test
    void fromQueryContext() {
        fromQueryContext(TestData.MINIMAL_QUERY_CONTEXT);
        fromQueryContext(TestData.FULL_QUERY_CONTEXT);
    }

    @Test
    void fromQueryMetadata() {
        fromQueryMetadata(TestData.MINIMAL_QUERY_METADATA);
        fromQueryMetadata(TestData.FULL_QUERY_METADATA);
    }

    @Test
    void fromQueryCompletedEvent() {
        fromQueryCompletedEvent(TestData.MINIMAL_QUERY_COMPLETED_EVENT);
        fromQueryCompletedEvent(TestData.FULL_QUERY_COMPLETED_EVENT);
    }

    @Test
    void fromSplitCompletedEvent() {
        fromSplitCompletedEvent(TestData.MINIMAL_SPLIT_COMPLETED_EVENT);
        fromSplitCompletedEvent(TestData.FULL_SPLIT_COMPLETED_EVENT);
    }

    @Test
    void fromQueryCreatedEvent() {
        fromQueryCreatedEvent(TestData.FULL_QUERY_CREATED_EVENT);
    }

    static Matcher<Schema.Duration> durationEqualTo(java.time.Duration expectedDuration) {
        return new TypeSafeMatcher<>(Schema.Duration.class) {

            @Override
            public void describeTo(Description description) {
                description
                        .appendText("A Schema.Duration equals to:")
                        .appendValue(expectedDuration);
            }

            @Override
            protected boolean matchesSafely(Schema.Duration duration) {
                return expectedDuration.getSeconds() == duration.getSeconds()
                        && expectedDuration.getNano() == duration.getNanos();
            }
        };
    }

    static Matcher<Schema.Timestamp> timestampEqualTo(Instant instant) {
        return new TypeSafeMatcher<>(Schema.Timestamp.class) {

            @Override
            public void describeTo(Description description) {
                description.appendText("A Schema.Timestamp equals to:").appendValue(instant);
            }

            @Override
            protected boolean matchesSafely(Schema.Timestamp timestamp) {
                return instant.getEpochSecond() == timestamp.getSeconds()
                        && instant.getNano() == timestamp.getNanos();
            }
        };
    }
}
