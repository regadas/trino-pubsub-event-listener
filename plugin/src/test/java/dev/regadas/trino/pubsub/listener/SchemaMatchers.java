package dev.regadas.trino.pubsub.listener;

import dev.regadas.trino.pubsub.listener.proto.Schema.Duration;
import dev.regadas.trino.pubsub.listener.proto.Schema.Timestamp;
import java.time.Instant;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class SchemaMatchers {
    static Matcher<Duration> durationEqualTo(java.time.Duration expectedDuration) {
        return new TypeSafeMatcher<>(Duration.class) {

            @Override
            public void describeTo(Description description) {
                description
                        .appendText("A Schema.Duration equals to:")
                        .appendValue(expectedDuration);
            }

            @Override
            protected boolean matchesSafely(Duration duration) {
                return expectedDuration.getSeconds() == duration.getSeconds()
                        && expectedDuration.getNano() == duration.getNanos();
            }
        };
    }

    static Matcher<Timestamp> timestampEqualTo(Instant instant) {
        return new TypeSafeMatcher<>(Timestamp.class) {

            @Override
            public void describeTo(Description description) {
                description.appendText("A Schema.Timestamp equals to:").appendValue(instant);
            }

            @Override
            protected boolean matchesSafely(Timestamp timestamp) {
                return instant.getEpochSecond() == timestamp.getSeconds()
                        && instant.getNano() == timestamp.getNanos();
            }
        };
    }
}
