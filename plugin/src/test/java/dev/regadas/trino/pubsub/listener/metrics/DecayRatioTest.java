package dev.regadas.trino.pubsub.listener.metrics;

import static org.junit.jupiter.api.Assertions.*;

import io.airlift.stats.DecayCounter;
import io.airlift.stats.ExponentialDecay;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DecayRatioTest {

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
                    0, 10, 0.0
                    1, 9,  0.1
                    5, 5,  0.5
                    10, 0, 1.0""")
    void testRatioCounters(int failedCount, int successCount, double expectedRatio) {
        var failed = new DecayCounter(ExponentialDecay.oneMinute());
        var success = new DecayCounter(ExponentialDecay.oneMinute());
        var ratioStats = new DecayRatio(failed, success);

        failed.add(failedCount);
        success.add(successCount);

        assertEquals(expectedRatio, ratioStats.getCount());
        assertEquals(expectedRatio, ratioStats.getRate());
    }
}
