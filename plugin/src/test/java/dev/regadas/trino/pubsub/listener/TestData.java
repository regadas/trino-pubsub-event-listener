package dev.regadas.trino.pubsub.listener;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

import static java.lang.Boolean.TRUE;
import static java.time.Duration.ofMillis;

import com.google.common.collect.ImmutableMap;

import io.trino.spi.TrinoWarning;
import io.trino.spi.connector.StandardWarningCode;
import io.trino.spi.eventlistener.ColumnDetail;
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

class TestData {
    public static final QueryMetadata FULL_QUERY_METADATA =
            new QueryMetadata(
                    "full_query",
                    Optional.of("transactionId"),
                    "query",
                    Optional.of("updateType"),
                    Optional.of("preparedQuery"),
                    "queryState",
                    List.of(),
                    List.of(),
                    URI.create("http://localhost"),
                    Optional.of("plan"),
                    Optional.of("jsonplan"),
                    Optional.of("stageInfo"));

    public static final QueryStatistics FULL_QUERY_STATISTICS =
            new QueryStatistics(
                    ofMillis(101),
                    ofMillis(102),
                    ofMillis(103),
                    ofMillis(104),
                    Optional.of(ofMillis(105)),
                    Optional.of(ofMillis(106)),
                    Optional.of(ofMillis(107)),
                    Optional.of(ofMillis(108)),
                    Optional.of(ofMillis(109)),
                    Optional.of(ofMillis(110)),
                    Optional.of(ofMillis(111)),
                    Optional.of(ofMillis(112)),
                    Optional.of(ofMillis(113)),
                    Optional.of(ofMillis(114)),
                    Optional.of(ofMillis(115)),
                    115L,
                    116L,
                    117L,
                    118L,
                    119L,
                    1191L,
                    1192L,
                    120L,
                    121L,
                    122L,
                    123L,
                    124L,
                    125L,
                    126L,
                    127L,
                    1271L,
                    128.0,
                    129.0,
                    Collections.emptyList(),
                    130,
                    true,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    List.of("{operator: \"operator1\"}", "{operator: \"operator2\"}"),
                    Collections.emptyList(),
                    Optional.empty());

    public static final QueryContext FULL_QUERY_CONTEXT =
            new QueryContext(
                    "user",
                    Optional.of("principal"),
                    Set.of("group1", "group2"),
                    Optional.of("traceToken"),
                    Optional.of("remoteAddress"),
                    Optional.of("userAgent"),
                    Optional.of("clientInfo"),
                    Set.of("tag1", "tag2", "tag3"),
                    Set.of(),
                    Optional.of("source"),
                    Optional.of("catalog"),
                    Optional.of("schema"),
                    Optional.of(new ResourceGroupId("resourceGroup")),
                    Map.of("property1", "value1", "property2", "value2"),
                    new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
                    "serverAddress",
                    "serverVersion",
                    "environment",
                    Optional.of(QueryType.SELECT),
                    "TASK");

    public static final QueryIOMetadata FULL_QUERY_IO_METADATA =
            new QueryIOMetadata(
                    List.of(
                            new QueryInputMetadata(
                                    "catalog1",
                                    "schema1",
                                    "table1",
                                    List.of("column1", "column2"),
                                    Optional.of("connectorInfo1"),
                                    new Metrics(ImmutableMap.of()),
                                    OptionalLong.of(201),
                                    OptionalLong.of(202)),
                            new QueryInputMetadata(
                                    "catalog2",
                                    "schema2",
                                    "table2",
                                    List.of("column3", "column4"),
                                    Optional.of("connectorInfo2"),
                                    new Metrics(ImmutableMap.of()),
                                    OptionalLong.of(203),
                                    OptionalLong.of(204))),
                    Optional.of(
                            new QueryOutputMetadata(
                                    "catalog3",
                                    "schema3",
                                    "table3",
                                    Optional.of(
                                            List.of(
                                                    new OutputColumnMetadata(
                                                            "column5",
                                                            "BIGINT",
                                                            Set.of(
                                                                    new ColumnDetail(
                                                                            "catalog4",
                                                                            "schema4",
                                                                            "table4",
                                                                            "column6"))),
                                                    new OutputColumnMetadata(
                                                            "column6", "VARCHAR", Set.of()))),
                                    Optional.of("outputMetadata"),
                                    Optional.of(TRUE))));

    public static final QueryFailureInfo FULL_FAILURE_INFO =
            new QueryFailureInfo(
                    GENERIC_INTERNAL_ERROR.toErrorCode(),
                    Optional.of("failureType"),
                    Optional.of("failureMessage"),
                    Optional.of("failureTask"),
                    Optional.of("failureHost"),
                    "failureJson");

    public static final QueryCompletedEvent FULL_QUERY_COMPLETED_EVENT =
            new QueryCompletedEvent(
                    FULL_QUERY_METADATA,
                    FULL_QUERY_STATISTICS,
                    FULL_QUERY_CONTEXT,
                    FULL_QUERY_IO_METADATA,
                    Optional.of(FULL_FAILURE_INFO),
                    List.of(
                            new TrinoWarning(
                                    StandardWarningCode.TOO_MANY_STAGES, "too many stages")),
                    Instant.now(),
                    Instant.now(),
                    Instant.now());

    public static final QueryMetadata MINIMAL_QUERY_METADATA =
            new QueryMetadata(
                    "minimal_query",
                    Optional.empty(),
                    "query",
                    Optional.empty(),
                    Optional.empty(),
                    "queryState",
                    List.of(),
                    List.of(),
                    URI.create("http://localhost"),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

    public static final QueryStatistics MINIMAL_QUERY_STATISTICS =
            new QueryStatistics(
                    ofMillis(101),
                    ofMillis(102),
                    ofMillis(103),
                    ofMillis(104),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    115L,
                    116L,
                    117L,
                    118L,
                    119L,
                    1191L,
                    1192L,
                    120L,
                    121L,
                    122L,
                    123L,
                    124L,
                    125L,
                    126L,
                    127L,
                    1271L,
                    128.0,
                    129.0,
                    Collections.emptyList(),
                    130,
                    false,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Optional.empty());

    public static final QueryContext MINIMAL_QUERY_CONTEXT =
            new QueryContext(
                    "user",
                    Optional.empty(),
                    Set.of(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Set.of(),
                    Set.of(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Map.of(),
                    new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
                    "serverAddress",
                    "serverVersion",
                    "environment",
                    Optional.empty(),
                    "NONE");

    public static final QueryIOMetadata MINIMAL_QUERY_IO_METADATA =
            new QueryIOMetadata(List.of(), Optional.empty());

    public static final QueryCompletedEvent MINIMAL_QUERY_COMPLETED_EVENT =
            new QueryCompletedEvent(
                    MINIMAL_QUERY_METADATA,
                    MINIMAL_QUERY_STATISTICS,
                    MINIMAL_QUERY_CONTEXT,
                    MINIMAL_QUERY_IO_METADATA,
                    Optional.empty(),
                    List.of(),
                    Instant.now(),
                    Instant.now(),
                    Instant.now());
}
