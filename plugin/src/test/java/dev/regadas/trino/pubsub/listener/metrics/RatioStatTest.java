package dev.regadas.trino.pubsub.listener.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airlift.stats.CounterStat;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RatioStatTest {

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
                    0, 10, 0.0
                    1, 9,  0.1
                    5, 5,  0.5
                    10, 0, 1.0""")
    void testRatioCounters(int failedCount, int successCount, double expectedRatio) {
        var failed = new CounterStat();
        var success = new CounterStat();
        var ratioStats = new RatioStat(failed, success);

        failed.update(failedCount);
        success.update(successCount);

        assertEquals(expectedRatio, ratioStats.getTotalRatio());
        assertEquals(expectedRatio, ratioStats.getOneMinute().getCount());
        assertEquals(expectedRatio, ratioStats.getFiveMinute().getCount());
        assertEquals(expectedRatio, ratioStats.getFifteenMinute().getCount());
    }
}
