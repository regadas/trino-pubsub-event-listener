package dev.regadas.trino.pubsub.listener.encoder;

import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.*;

import dev.regadas.trino.pubsub.listener.TestData;
import dev.regadas.trino.pubsub.listener.event.QueryEvent;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class JsonQueryEventEncoderTest {
    @Test
    void shouldConvertQueriesIntoJsonBytes() throws Exception {
        QueryEvent event = QueryEvent.queryCreated(TestData.FULL_QUERY_CREATED_EVENT);
        var expectedString =
                """
                {
                  "queryCreated":{
                    "createTime":1693526400.000000000,
                    "context":{
                      "user":"user",
                      "originalUser":"originalUser",
                      "originalRoles":["role1","role2"],
                      "principal":"principal",
                      "enabledRoles":["role1","role2"],
                      "groups":["group1","group2"],
                      "traceToken":"traceToken",
                      "remoteClientAddress":"remoteAddress",
                      "userAgent":"userAgent",
                      "clientInfo":"clientInfo",
                      "clientTags":["tag1","tag2","tag3"],
                      "clientCapabilities":[],
                      "source":"source",
                      "timezone":"UTC",
                      "catalog":"catalog",
                      "schema":"schema",
                      "resourceGroupId":["resourceGroup"],
                      "sessionProperties":{
                        "property1":"value1",
                        "property2":"value2"
                      },
                      "resourceEstimates":{
                        "executionTime":null,
                        "cpuTime":null,
                        "peakMemoryBytes":null
                      },
                      "serverAddress":"serverAddress",
                      "serverVersion":"serverVersion",
                      "environment":"environment",
                      "queryType":"SELECT",
                      "retryPolicy":"TASK"
                    },
                    "metadata":{
                      "queryId":"full_query",
                      "transactionId":"transactionId",
                      "encoding":"querySpan",
                      "query":"query",
                      "updateType":"updateType",
                      "preparedQuery":"preparedQuery",
                      "queryState":"queryState",
                      "uri":"http://localhost",
                      "tables":[
                        {
                          "catalog":"catalog",
                          "schema":"schema",
                          "table":"table",
                          "authorization":"sa",
                          "filters":["column>5"],
                          "columns":[{"column":"column","mask":null}],
                          "directlyReferenced":true,
                          "viewText":null,
                          "referenceChain":[]
                        }
                      ],
                      "routines":[
                        {
                          "routine":"routine",
                          "authorization":"routineSA"
                        }
                      ],
                      "plan":"plan",
                      "jsonPlan":"jsonplan",
                      "payload":"stageInfo"
                    }
                  },
                  "queryCompleted":null,
                  "splitCompleted":null
                }
                """
                        .lines()
                        .map(String::trim)
                        .collect(joining(""));

        var bytes = new JsonQueryEventEncoder().encode(event);
        assertEquals(expectedString, new String(bytes, StandardCharsets.UTF_8));
    }
}
