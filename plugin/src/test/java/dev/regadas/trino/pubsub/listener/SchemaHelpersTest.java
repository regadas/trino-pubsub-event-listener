package dev.regadas.trino.pubsub.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryMetadata;
import org.junit.jupiter.api.Test;

class SchemaHelpersTest {

    private void fromQueryContext(QueryContext context) {
        var schema = SchemaHelpers.from(context);

        assertEquals(schema.getSchema(), context.getSchema().orElse(""));
        assertEquals(schema.getCatalog(), context.getCatalog().orElse(""));
        assertEquals(schema.getPrincipal(), context.getPrincipal().orElse(""));
        assertEquals(schema.getSource(), context.getSource().orElse(""));
        assertEquals(schema.getRemoteClientAddress(), context.getRemoteClientAddress().orElse(""));
        assertEquals(schema.getUserAgent(), context.getUserAgent().orElse(""));
        assertEquals(schema.getTraceToken(), context.getTraceToken().orElse(""));
    }

    private void fromQueryMetadata(QueryMetadata metadata) {
        var schema = SchemaHelpers.from(metadata);

        assertEquals(schema.getQueryId(), metadata.getQueryId());
        assertEquals(schema.getQuery(), metadata.getQuery());
        assertEquals(schema.getQueryState(), metadata.getQueryState());
    }

    private void fromQueryCompletedEvent(QueryCompletedEvent event) {
        var schema = SchemaHelpers.from(event);

        assertEquals(schema.getCreateTime().getSeconds(), event.getCreateTime().getEpochSecond());
        assertEquals(
                schema.getExecutionStartTime().getSeconds(),
                event.getExecutionStartTime().getEpochSecond());
        assertEquals(schema.getEndTime().getSeconds(), event.getEndTime().getEpochSecond());
        assertEquals(schema.getWarningsCount(), event.getWarnings().size());

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
    }
}
