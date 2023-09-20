package dev.regadas.trino.pubsub.listener.encoder;

import static org.junit.jupiter.api.Assertions.*;

import dev.regadas.trino.pubsub.listener.TestData;
import dev.regadas.trino.pubsub.listener.event.QueryEvent;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

class AvroQueryEventEncoderTest {
    @Test
    void shouldEncodeQueryEvent() throws Exception {
        var event = QueryEvent.queryCreated(TestData.FULL_QUERY_CREATED_EVENT);

        var queyEventSchema = AvroQueryEventEncoder.avroSchema.getAvroSchema();
        var queryCreatedSchema =
                queyEventSchema.getField("queryCreated").schema().getTypes().get(1);
        var contextSchema = queryCreatedSchema.getField("context").schema().getTypes().get(1);
        var stringArraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        var resourceEstimatesSchema =
                contextSchema.getField("resourceEstimates").schema().getTypes().get(1);
        var queryTypeSchema = contextSchema.getField("queryType").schema().getTypes().get(1);
        var metadataSchema = queryCreatedSchema.getField("metadata").schema().getTypes().get(1);
        var tablesSchema =
                metadataSchema.getField("tables").schema().getTypes().get(1).getElementType();
        var columnInfoSchema =
                tablesSchema.getField("columns").schema().getTypes().get(1).getElementType();
        var routinesSchema =
                metadataSchema.getField("routines").schema().getTypes().get(1).getElementType();

        var queryContext = new Record(contextSchema);
        queryContext.put("user", "user");
        queryContext.put("principal", "principal");
        queryContext.put("groups", new Array<>(stringArraySchema, List.of("group1", "group2")));
        queryContext.put("traceToken", "traceToken");
        queryContext.put("remoteClientAddress", "remoteAddress");
        queryContext.put("userAgent", "userAgent");
        queryContext.put("clientInfo", "clientInfo");
        queryContext.put(
                "clientTags", new Array<>(stringArraySchema, List.of("tag1", "tag2", "tag3")));
        queryContext.put("clientCapabilities", new Array<>(stringArraySchema, List.of()));
        queryContext.put("source", "source");
        queryContext.put("catalog", "catalog");
        queryContext.put("schema", "schema");
        queryContext.put(
                "resourceGroupId", new Array<>(stringArraySchema, List.of("resourceGroup")));
        var sessionProperties = new LinkedHashMap<String, String>();
        sessionProperties.put("property2", "value2");
        sessionProperties.put("property1", "value1");
        queryContext.put("sessionProperties", sessionProperties);
        queryContext.put("resourceEstimates", new Record(resourceEstimatesSchema));
        queryContext.put("serverVersion", "serverVersion");
        queryContext.put("serverAddress", "serverAddress");
        queryContext.put("environment", "environment");
        queryContext.put("queryType", new EnumSymbol(queryTypeSchema, "SELECT"));
        queryContext.put("retryPolicy", "TASK");

        var metadata = new Record(metadataSchema);
        metadata.put("queryId", "full_query");
        metadata.put("transactionId", "transactionId");
        metadata.put("query", "query");
        metadata.put("updateType", "updateType");
        metadata.put("preparedQuery", "preparedQuery");
        metadata.put("queryState", "queryState");
        metadata.put("uri", "http://localhost");

        var columnInfo = new Record(columnInfoSchema);
        columnInfo.put("column", "column");
        columnInfo.put("mask", null);
        var table = new Record(tablesSchema);
        table.put("catalog", "catalog");
        table.put("schema", "schema");
        table.put("table", "table");
        table.put("authorization", "sa");
        table.put("filters", new Array<>(stringArraySchema, List.of("column>5")));
        table.put(
                "columns", new Array<>(Schema.createArray(columnInfoSchema), List.of(columnInfo)));
        table.put("directlyReferenced", true);
        metadata.put("tables", new Array<>(Schema.createArray(tablesSchema), List.of(table)));

        var routine = new Record(routinesSchema);
        routine.put("routine", "routine");
        routine.put("authorization", "routineSA");
        metadata.put("routines", new Array<>(Schema.createArray(routinesSchema), List.of(routine)));

        metadata.put("plan", "plan");
        metadata.put("jsonPlan", "jsonplan");
        metadata.put("payload", "stageInfo");

        var queryCreated = new Record(queryCreatedSchema);
        queryCreated.put("createTime", 1693526400000L);
        queryCreated.put("context", queryContext);
        queryCreated.put("metadata", metadata);

        var expectedEvent = new Record(queyEventSchema);
        expectedEvent.put("queryCreated", queryCreated);

        var bytes = new AvroQueryEventEncoder().encode(event);
        var reader = new GenericDatumReader<GenericRecord>(queyEventSchema);
        var deserializedEvent = reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        // need to compare strings as avro maps are not truly comparable
        assertEquals(expectedEvent.toString(), deserializedEvent.toString());
    }
}
