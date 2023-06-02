package dev.regadas.trino.pubsub.listener;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.protobuf.Message;
import dev.regadas.trino.pubsub.listener.Encoder.Encoding;
import dev.regadas.trino.pubsub.listener.proto.Schema;
import dev.regadas.trino.pubsub.listener.pubsub.Publisher;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("ALL")
class PubSubEventListenerTest {

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
      # trackEvent,  pubResult, pubThrow, expAttempt, expSuccess, expFail
      true,          true,      true,     1,          0,          1
      true,          true,      false,    1,          1,          0
      true,          false,     true,     1,          0,          1
      true,          false,     false,    1,          0,          1
      false,         true,      true,     0,          0,          0
      false,         true,      false,    0,          0,          0
      false,         false,     true,     0,          0,          0
      false,         false,     false,    0,          0,          0
      """)
    void testCounterForQueryCreated(
            boolean trackEvent,
            boolean pubResult,
            boolean pubThrow,
            long expAttempt,
            long expSuccess,
            long expFail)
            throws InterruptedException {
        var publisher = new TestPublisher(pubResult, true, pubThrow);
        var config =
                new PubSubEventListenerConfig(
                        trackEvent, false, false, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.queryCreated(TestData.FULL_QUERY_CREATED_EVENT);

        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCreated().attempts().get(),
                equalTo(expAttempt));
        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCreated().successful().get(),
                equalTo(expSuccess));
        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCreated().failure().get(), equalTo(expFail));
    }

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
      # trackEvent,  pubResult, pubThrow, expAttempt, expSuccess, expFail
      true,          true,      true,     1,          0,          1
      true,          true,      false,    1,          1,          0
      true,          false,     true,     1,          0,          1
      true,          false,     false,    1,          0,          1
      false,         true,      true,     0,          0,          0
      false,         true,      false,    0,          0,          0
      false,         false,     true,     0,          0,          0
      false,         false,     false,    0,          0,          0
      """)
    void testCounterForQueryCompleted(
            boolean trackEvent,
            boolean pubResult,
            boolean pubThrow,
            long expAttempt,
            long expSuccess,
            long expFail)
            throws InterruptedException {
        var publisher = new TestPublisher(pubResult, true, pubThrow);
        var config =
                new PubSubEventListenerConfig(
                        false, trackEvent, false, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.queryCompleted(TestData.FULL_QUERY_COMPLETED_EVENT);

        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCompleted().attempts().get(),
                equalTo(expAttempt));
        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCompleted().successful().get(),
                equalTo(expSuccess));
        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCompleted().failure().get(),
                equalTo(expFail));
    }

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
      # trackEvent,  pubResult, pubThrow, expAttempt, expSuccess, expFail
      true,          true,      true,     1,          0,          1
      true,          true,      false,    1,          1,          0
      true,          false,     true,     1,          0,          1
      true,          false,     false,    1,          0,          1
      false,         true,      true,     0,          0,          0
      false,         true,      false,    0,          0,          0
      false,         false,     true,     0,          0,          0
      false,         false,     false,    0,          0,          0
      """)
    void testCounterForSplitCompleted(
            boolean trackEvent,
            boolean pubResult,
            boolean pubThrow,
            long expAttempt,
            long expSuccess,
            long expFail)
            throws InterruptedException {
        var publisher = new TestPublisher(pubResult, true, pubThrow);
        var config =
                new PubSubEventListenerConfig(
                        false, false, trackEvent, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.splitCompleted(TestData.FULL_SPLIT_COMPLETED_EVENT);

        assertThatEventually(
                eventListener.getPubSubInfo().getSplitCompleted().attempts().get(),
                equalTo(expAttempt));
        assertThatEventually(
                eventListener.getPubSubInfo().getSplitCompleted().successful().get(),
                equalTo(expSuccess));
        assertThatEventually(
                eventListener.getPubSubInfo().getSplitCompleted().failure().get(),
                equalTo(expFail));
    }

    @Test
    void testQueryCreatedPublishCorrespondingMessage() {
        var publisher = new TestPublisher(true, true, false);
        var config = new PubSubEventListenerConfig(true, true, true, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.queryCreated(TestData.FULL_QUERY_CREATED_EVENT);

        assertThat(publisher.lastPublishedMessage, instanceOf(Schema.QueryCreatedEvent.class));
    }

    @Test
    void testQueryCompletedPublishCorrespondingMessage() {
        var publisher = new TestPublisher(true, true, false);
        var config = new PubSubEventListenerConfig(true, true, true, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.queryCompleted(TestData.FULL_QUERY_COMPLETED_EVENT);

        assertThat(publisher.lastPublishedMessage, instanceOf(Schema.QueryCompletedEvent.class));
    }

    @Test
    void testSplitCompletedPublishCorrespondingMessage() {
        var publisher = new TestPublisher(true, true, false);
        var config = new PubSubEventListenerConfig(true, true, true, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.splitCompleted(TestData.FULL_SPLIT_COMPLETED_EVENT);

        assertThat(publisher.lastPublishedMessage, instanceOf(Schema.SplitCompletedEvent.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testShutdownNormally(boolean timeout) {
        var publisher = new TestPublisher(true, timeout, false);
        var config = new PubSubEventListenerConfig(true, true, true, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.close();

        assertThat(publisher.shutdownCalled, is(true));
    }

    public static <T> void assertThatEventually(T actual, Matcher<? super T> matcher)
            throws InterruptedException {
        int tries = 5;
        while (--tries > 0 && !matcher.matches(actual)) {
            Thread.sleep(20);
        }
        assertThat(actual, matcher);
    }

    static class TestPublisher implements Publisher {

        private final boolean pubResult;
        private final boolean shutdownResult;
        private final boolean throwExec;
        private Message lastPublishedMessage;
        private boolean shutdownCalled;

        TestPublisher(boolean pubResult, boolean shutdownResult, boolean throwExec) {
            this.pubResult = pubResult;
            this.shutdownResult = shutdownResult;
            this.throwExec = throwExec;
        }

        @Override
        public CompletableFuture<String> publish(Message message) {
            this.lastPublishedMessage = message;
            if (throwExec) {
                throw new UncheckedIOException("cannot send message", new IOException());
            }
            return (pubResult)
                    ? CompletableFuture.completedFuture("1")
                    : CompletableFuture.failedFuture(new IOException());
        }

        @Override
        public void close() throws InterruptedException {
            this.shutdownCalled = true;
            if (shutdownResult) {
                throw new InterruptedException("timeout");
            }
        }
    }
}
