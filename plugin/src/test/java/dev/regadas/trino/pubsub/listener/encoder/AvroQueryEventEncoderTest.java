package dev.regadas.trino.pubsub.listener.encoder;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import dev.regadas.trino.pubsub.listener.TestData;
import dev.regadas.trino.pubsub.listener.event.QueryEvent;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.session.ResourceEstimates;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AvroQueryEventEncoderTest {

    public static final Schema STRING_ARRAY_SCHEMA =
            Schema.createArray(Schema.create(Schema.Type.STRING));
    private AvroQueryEventEncoder encoder;

    @BeforeEach
    void setUp() {
        encoder = new AvroQueryEventEncoder();
    }

    @Test
    void shouldEncodeQueryEvent() throws Exception {
        var event = QueryEvent.queryCreated(TestData.FULL_QUERY_CREATED_EVENT);

        var queyEventSchema = AvroQueryEventEncoder.getAvroSchema();
        var queryCreatedSchema =
                queyEventSchema.getField("queryCreated").schema().getTypes().get(1);
        var contextSchema = queryCreatedSchema.getField("context").schema().getTypes().get(1);
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
        var referenceChainSchema =
                tablesSchema.getField("referenceChain").schema().getTypes().get(1).getElementType();

        var queryContext = new Record(contextSchema);
        queryContext.put("user", "user");
        queryContext.put("principal", "principal");
        queryContext.put("groups", new Array<>(STRING_ARRAY_SCHEMA, List.of("group1", "group2")));
        queryContext.put("traceToken", "traceToken");
        queryContext.put("remoteClientAddress", "remoteAddress");
        queryContext.put("userAgent", "userAgent");
        queryContext.put("clientInfo", "clientInfo");
        queryContext.put(
                "clientTags", new Array<>(STRING_ARRAY_SCHEMA, List.of("tag1", "tag2", "tag3")));
        queryContext.put("clientCapabilities", new Array<>(STRING_ARRAY_SCHEMA, List.of()));
        queryContext.put("source", "source");
        queryContext.put("catalog", "catalog");
        queryContext.put("schema", "schema");
        queryContext.put(
                "resourceGroupId", new Array<>(STRING_ARRAY_SCHEMA, List.of("resourceGroup")));
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
        queryContext.put("originalUser", "originalUser");
        queryContext.put("timezone", "UTC");
        queryContext.put(
                "enabledRoles", new Array<>(STRING_ARRAY_SCHEMA, List.of("role1", "role2")));

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
        table.put("filters", new Array<>(STRING_ARRAY_SCHEMA, List.of("column>5")));
        table.put(
                "columns", new Array<>(Schema.createArray(columnInfoSchema), List.of(columnInfo)));
        table.put("directlyReferenced", true);
        table.put("viewText", null);
        table.put(
                "referenceChain", new Array<>(Schema.createArray(referenceChainSchema), List.of()));
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

        var bytes = encoder.encode(event);
        var reader = new GenericDatumReader<GenericRecord>(queyEventSchema);
        var deserializedEvent = reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        // need to compare strings as avro maps are not truly comparable
        assertEquals(expectedEvent.toString(), deserializedEvent.toString());
    }

    @Test
    void shouldEncodeTypesWithJsonfiedFields() throws Exception {
        var queyEventSchema = AvroQueryEventEncoder.getAvroSchema();
        var queryCompletedSchema =
                queyEventSchema.getField("queryCompleted").schema().getTypes().get(1);
        var ioMetadataSchema =
                queryCompletedSchema.getField("ioMetadata").schema().getTypes().get(1);
        var inputsSchema =
                ioMetadataSchema.getField("inputs").schema().getTypes().get(1).getElementType();

        var expectedInput = new Record(inputsSchema);
        expectedInput.put("catalogName", "catalogName");
        expectedInput.put("catalogVersion", "1");
        expectedInput.put("schema", "schema");
        expectedInput.put("table", "table");
        expectedInput.put(
                "columns", new Array<>(STRING_ARRAY_SCHEMA, List.of("column1", "column2")));
        // jsonified field, connectorInfo defined as Optional<Object>
        expectedInput.put("connectorInfo", "1");
        // jsonified field, connectorMetrics defined as Metrics that contains a Map<String, Metric>
        expectedInput.put("connectorMetrics", """
                {"foo":"{"total":12}"}""");
        expectedInput.put("physicalInputBytes", 2L);
        expectedInput.put("physicalInputRows", 3L);

        var inputMetadata =
                new QueryInputMetadata(
                        "catalogName",
                        new CatalogHandle.CatalogVersion("1"),
                        "schema",
                        "table",
                        List.of("column1", "column2"),
                        Optional.of(1),
                        new Metrics(Map.of("foo", new TestData.SimpleCount(12))),
                        OptionalLong.of(2),
                        OptionalLong.of(3));

        var bytes = encoder.encode(inputMetadata, new AvroSchema(inputsSchema));

        var reader = new GenericDatumReader<GenericRecord>(inputsSchema);
        var deserialized = reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        assertEquals(expectedInput, deserialized);
    }

    @Test
    void shouldEncodeTypesWithDuration() throws Exception {
        var queyEventSchema = AvroQueryEventEncoder.getAvroSchema();
        var queryCompletedSchema =
                queyEventSchema.getField("queryCompleted").schema().getTypes().get(1);
        var contextSchema = queryCompletedSchema.getField("context").schema().getTypes().get(1);
        var estimatesSchema =
                contextSchema.getField("resourceEstimates").schema().getTypes().get(1);

        var expectedInput = new Record(estimatesSchema);
        expectedInput.put("executionTime", 1L);
        expectedInput.put("cpuTime", 2 * 60 * 1000L);
        expectedInput.put("peakMemoryBytes", 3L);

        var resourceEstimates =
                new ResourceEstimates(
                        Optional.of(Duration.ofMillis(1)),
                        Optional.of(Duration.ofMinutes(2)),
                        Optional.of(3L));

        var bytes = encoder.encode(resourceEstimates, new AvroSchema(estimatesSchema));

        var reader = new GenericDatumReader<GenericRecord>(estimatesSchema);
        var deserialized = reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        assertEquals(expectedInput, deserialized);
    }
}
