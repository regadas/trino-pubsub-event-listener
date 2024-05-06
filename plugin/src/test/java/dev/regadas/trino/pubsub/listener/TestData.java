package dev.regadas.trino.pubsub.listener;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Boolean.TRUE;
import static java.time.Duration.ofMillis;
import static java.time.ZoneOffset.UTC;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoWarning;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.connector.StandardWarningCode;
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
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class TestData {
    public static final QueryMetadata FULL_QUERY_METADATA =
            new QueryMetadata(
                    "full_query",
                    Optional.of("transactionId"),
                    "query",
                    Optional.of("updateType"),
                    Optional.of("preparedQuery"),
                    "queryState",
                    List.of(
                            new TableInfo(
                                    "catalog",
                                    "schema",
                                    "table",
                                    "sa",
                                    List.of("column>5"),
                                    List.of(new ColumnInfo("column", Optional.empty())),
                                    true,
                                    Optional.empty(),
                                    List.of())),
                    List.of(new RoutineInfo("routine", "routineSA")),
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
                    Optional.of(ofMillis(1091)),
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
                    List.of(new StageGcStatistics(0, 1, 42, 1, 3, 84, 2)),
                    130,
                    true,
                    List.of(
                            new StageCpuDistribution(
                                    0, 1, 25, 50, 75, 90, 95, 99, 10, 100, 1000, 50.0)),
                    List.of(
                            new StageOutputBufferUtilization(
                                    0,
                                    1,
                                    101,
                                    105,
                                    110,
                                    125,
                                    150,
                                    175,
                                    190,
                                    195,
                                    199,
                                    100,
                                    200,
                                    Duration.ofSeconds(1500))),
                    List.of(),
                    List.of("{operator: \"operator1\"}", "{operator: \"operator2\"}"),
                    List.of(new QueryPlanOptimizerStatistics("tablescan", 3, 5, 6, 0)),
                    Optional.of("statsAndCost"));

    public static final QueryContext FULL_QUERY_CONTEXT =
            new QueryContext(
                    "user",
                    "originalUser",
                    Optional.of("principal"),
                    new TreeSet<>(List.of("role1", "role2")),
                    new TreeSet<>(List.of("group1", "group2")),
                    Optional.of("traceToken"),
                    Optional.of("remoteAddress"),
                    Optional.of("userAgent"),
                    Optional.of("clientInfo"),
                    new TreeSet<>(List.of("tag1", "tag2", "tag3")),
                    Set.of(),
                    Optional.of("source"),
                    "UTC",
                    Optional.of("catalog"),
                    Optional.of("schema"),
                    Optional.of(new ResourceGroupId("resourceGroup")),
                    new TreeMap<>(Map.of("property1", "value1", "property2", "value2")),
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
                                    new CatalogVersion("v1"),
                                    "schema1",
                                    "table1",
                                    List.of("column1", "column2"),
                                    Optional.of("connectorInfo1"),
                                    new Metrics(ImmutableMap.of("count", new SimpleCount(47))),
                                    OptionalLong.of(201),
                                    OptionalLong.of(202)),
                            new QueryInputMetadata(
                                    "catalog2",
                                    new CatalogVersion("v1"),
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
                                    new CatalogVersion("v1"),
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

    public static final Instant BASE_INSTANT =
            LocalDate.of(2023, 9, 1).atStartOfDay(UTC).toInstant();
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
                    BASE_INSTANT,
                    BASE_INSTANT,
                    BASE_INSTANT);

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
                    Collections.emptyList(),
                    Optional.empty());

    public static final QueryContext MINIMAL_QUERY_CONTEXT =
            new QueryContext(
                    "user",
                    "originalUser",
                    Optional.empty(),
                    Set.of(),
                    Set.of(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Set.of(),
                    Set.of(),
                    Optional.empty(),
                    "UTC",
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
                    BASE_INSTANT,
                    BASE_INSTANT,
                    BASE_INSTANT);

    private static final SplitStatistics SPLIT_STATS =
            new SplitStatistics(
                    Duration.ofSeconds(3),
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    5L,
                    10L,
                    Optional.of(Duration.ofMillis(50L)),
                    Optional.of(Duration.ofMillis(100L)));

    public static final SplitCompletedEvent MINIMAL_SPLIT_COMPLETED_EVENT =
            new SplitCompletedEvent(
                    "query1",
                    "stageA",
                    "task01",
                    Optional.empty(),
                    BASE_INSTANT,
                    Optional.empty(),
                    Optional.empty(),
                    SPLIT_STATS,
                    Optional.empty(),
                    "split payload");

    public static final SplitCompletedEvent FULL_SPLIT_COMPLETED_EVENT =
            new SplitCompletedEvent(
                    "query1",
                    "stageA",
                    "task01",
                    Optional.of("catalog"),
                    BASE_INSTANT,
                    Optional.of(BASE_INSTANT.minus(Duration.ofMillis(50))),
                    Optional.of(BASE_INSTANT),
                    SPLIT_STATS,
                    Optional.of(new SplitFailureInfo("fatal", "boom")),
                    "split payload");

    public static final QueryCreatedEvent FULL_QUERY_CREATED_EVENT =
            new QueryCreatedEvent(BASE_INSTANT, FULL_QUERY_CONTEXT, FULL_QUERY_METADATA);

    public static class SimpleCount implements Count<Long> {
        private final long value;

        public SimpleCount(long initialValue) {
            this.value = initialValue;
        }

        @Override
        public Long mergeWith(List<Long> others) {
            return others.stream().mapToLong(i -> i).sum() + value;
        }

        @Override
        public long getTotal() {
            return value;
        }

        @Override
        public Long mergeWith(Long other) {
            return value + other;
        }
    }
}
