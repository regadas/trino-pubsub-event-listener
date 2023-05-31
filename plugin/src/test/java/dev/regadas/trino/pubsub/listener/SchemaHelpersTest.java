package dev.regadas.trino.pubsub.listener;

import static dev.regadas.trino.pubsub.listener.SchemaMatchers.durationEqualTo;
import static dev.regadas.trino.pubsub.listener.SchemaMatchers.timestampEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitFailureInfo;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SchemaHelpersTest {

    private void fromQueryContext(QueryContext context) {
        var schema = SchemaHelpers.from(context);

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
        var schema = SchemaHelpers.from(metadata);

        assertThat(schema.getQueryId(), is(metadata.getQueryId()));
        assertThat(schema.getQuery(), is(metadata.getQuery()));
        assertThat(schema.getQueryState(), is(metadata.getQueryState()));
    }

    private void fromQueryFailureInfo(QueryFailureInfo info) {
        var schema = SchemaHelpers.from(info);

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
        var schema = SchemaHelpers.from(event);

        assertThat(schema.getCreateTime(), timestampEqualTo(event.getCreateTime()));
        assertThat(schema.getExecutionStartTime(), timestampEqualTo(event.getExecutionStartTime()));
        assertThat(schema.getEndTime(), timestampEqualTo(event.getEndTime()));
        assertThat(schema.getWarningsCount(), is(event.getWarnings().size()));

        fromQueryContext(event.getContext());
        fromQueryMetadata(event.getMetadata());
        event.getFailureInfo().ifPresent(this::fromQueryFailureInfo);
    }

    @SuppressWarnings("deprecation")
    private void fromSplitCompletedEvent(SplitCompletedEvent event) {
        var schema = SchemaHelpers.from(event);

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
        var schema = SchemaHelpers.from(event);

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
}
