package dev.regadas.trino.pubsub.listener;

import static dev.regadas.trino.pubsub.listener.SchemaMatchers.equalToOrEmpty;
import static dev.regadas.trino.pubsub.listener.SchemaMatchers.schemaDurationEqualToOrEmpty;
import static dev.regadas.trino.pubsub.listener.SchemaMatchers.schemaEqualTo;
import static dev.regadas.trino.pubsub.listener.SchemaMatchers.schemaTimestampEqualToOrEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitFailureInfo;
import org.junit.jupiter.api.Test;

class SchemaHelpersTest {

    private void fromQueryContext(QueryContext context) {
        var schema = SchemaHelpers.from(context);

        assertThat(schema.getSchema(), equalToOrEmpty(context.getSchema()));
        assertThat(schema.getCatalog(), equalToOrEmpty(context.getCatalog()));
        assertThat(schema.getPrincipal(), equalToOrEmpty(context.getPrincipal()));
        assertThat(schema.getSource(), equalToOrEmpty(context.getSource()));
        assertThat(
                schema.getRemoteClientAddress(), equalToOrEmpty(context.getRemoteClientAddress()));
        assertThat(schema.getUserAgent(), equalToOrEmpty(context.getUserAgent()));
        assertThat(schema.getTraceToken(), equalToOrEmpty(context.getTraceToken()));
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
        assertThat(schema.getFailureType(), equalToOrEmpty(info.getFailureType()));
        assertThat(schema.getFailureMessage(), equalToOrEmpty(info.getFailureMessage()));
        assertThat(schema.getFailureTask(), equalToOrEmpty(info.getFailureTask()));
        assertThat(schema.getFailureHost(), equalToOrEmpty(info.getFailureHost()));
        assertThat(schema.getFailuresJson(), is(info.getFailuresJson()));
    }

    private void fromQueryCompletedEvent(QueryCompletedEvent event) {
        var schema = SchemaHelpers.from(event);

        assertThat(schema.getCreateTime(), schemaEqualTo(event.getCreateTime()));
        assertThat(schema.getExecutionStartTime(), schemaEqualTo(event.getExecutionStartTime()));
        assertThat(schema.getEndTime(), schemaEqualTo(event.getEndTime()));
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
        assertThat(schema.getCatalogName(), equalToOrEmpty(event.getCatalogName()));

        assertThat(schema.getCreateTime(), schemaEqualTo(event.getCreateTime()));
        assertThat(schema.getStartTime(), schemaTimestampEqualToOrEmpty(event.getStartTime()));
        assertThat(schema.getEndTime(), schemaTimestampEqualToOrEmpty(event.getEndTime()));

        assertThat(
                schema.getStatistics().getCpuTime(),
                schemaEqualTo(event.getStatistics().getCpuTime()));
        assertThat(
                schema.getStatistics().getWallTime(),
                schemaEqualTo(event.getStatistics().getWallTime()));
        assertThat(
                schema.getStatistics().getQueuedTime(),
                schemaEqualTo(event.getStatistics().getQueuedTime()));
        assertThat(
                schema.getStatistics().getCompletedReadTime(),
                schemaEqualTo(event.getStatistics().getCompletedReadTime()));

        assertThat(
                schema.getStatistics().getCompletedPositions(),
                is(event.getStatistics().getCompletedPositions()));
        assertThat(
                schema.getStatistics().getCompletedDataSizeBytes(),
                is(event.getStatistics().getCompletedDataSizeBytes()));

        assertThat(
                schema.getStatistics().getTimeToFirstByte(),
                schemaDurationEqualToOrEmpty(event.getStatistics().getTimeToFirstByte()));
        assertThat(
                schema.getStatistics().getTimeToLastByte(),
                schemaDurationEqualToOrEmpty(event.getStatistics().getTimeToLastByte()));

        assertThat(
                schema.getFailureInfo().getFailureType(),
                equalToOrEmpty(event.getFailureInfo().map(SplitFailureInfo::getFailureType)));
        assertThat(
                schema.getFailureInfo().getFailureMessage(),
                equalToOrEmpty(event.getFailureInfo().map(SplitFailureInfo::getFailureMessage)));

        assertThat(schema.getPayload(), is(event.getPayload()));
    }

    private void fromQueryCreatedEvent(QueryCreatedEvent event) {
        var schema = SchemaHelpers.from(event);

        assertThat(schema.getCreateTime(), schemaEqualTo(event.getCreateTime()));
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
