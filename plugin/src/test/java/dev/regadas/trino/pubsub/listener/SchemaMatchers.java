package dev.regadas.trino.pubsub.listener;

import dev.regadas.trino.pubsub.listener.proto.Schema.Duration;
import dev.regadas.trino.pubsub.listener.proto.Schema.Timestamp;
import java.time.Instant;
import java.util.Optional;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

public class SchemaMatchers {
    static Matcher<Duration> schemaEqualTo(java.time.Duration expectedDuration) {
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

    static Matcher<Timestamp> schemaEqualTo(Instant instant) {
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

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    static Matcher<String> equalToOrEmpty(Optional<String> expected) {
        return expected.map(Matchers::equalTo).orElse(Matchers.emptyString());
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    static Matcher<Duration> schemaDurationEqualToOrEmpty(Optional<java.time.Duration> expected) {
        return expected.map(SchemaMatchers::schemaEqualTo)
                .orElse(schemaEqualTo(java.time.Duration.ZERO));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    static Matcher<Timestamp> schemaTimestampEqualToOrEmpty(Optional<Instant> expected) {
        return expected.map(SchemaMatchers::schemaEqualTo).orElse(schemaEqualTo(Instant.EPOCH));
    }
}
