package dev.regadas.trino.pubsub.listener;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.protobuf.Message;
import dev.regadas.trino.pubsub.listener.Encoder.Encoding;
import dev.regadas.trino.pubsub.listener.proto.Schema;
import dev.regadas.trino.pubsub.listener.pubsub.Publisher;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("ALL")
class PubSubEventListenerTest {

    private static final int RETRY_DELAY = 20;
    private static final int RETRY_COUNT = 5;

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
      # trackEvent,  pubType,         expAttempt, expSuccess, expFail
      true,          success,         1,          1,          0
      true,          returnFailure,   1,          0,          1
      true,          throwOnPublish,  1,          0,          1
      false,         success,         0,          0,          0
      false,         returnFailure,   0,          0,          0
      false,         throwOnPublish,  0,          0,          0
      """)
    void testCounterForQueryCreated(
            boolean trackEvent, String pubType, long expAttempt, long expSuccess, long expFail)
            throws InterruptedException {
        var publisher = TestPublisher.from(pubType);
        var config =
                new PubSubEventListenerConfig(
                        trackEvent, false, false, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.queryCreated(TestData.FULL_QUERY_CREATED_EVENT);

        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCreated().attempts()::get,
                equalTo(expAttempt));
        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCreated().successful()::get,
                equalTo(expSuccess));
        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCreated().failure()::get, equalTo(expFail));
    }

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
      # trackEvent,  pubType,         expAttempt, expSuccess, expFail
      true,          success,         1,          1,          0
      true,          returnFailure,   1,          0,          1
      true,          throwOnPublish,  1,          0,          1
      false,         success,         0,          0,          0
      false,         returnFailure,   0,          0,          0
      false,         throwOnPublish,  0,          0,          0
      """)
    void testCounterForQueryCompleted(
            boolean trackEvent, String pubType, long expAttempt, long expSuccess, long expFail)
            throws InterruptedException {
        var publisher = TestPublisher.from(pubType);
        var config =
                new PubSubEventListenerConfig(
                        false, trackEvent, false, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.queryCompleted(TestData.FULL_QUERY_COMPLETED_EVENT);

        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCompleted().attempts()::get,
                equalTo(expAttempt));
        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCompleted().successful()::get,
                equalTo(expSuccess));
        assertThatEventually(
                eventListener.getPubSubInfo().getQueryCompleted().failure()::get, equalTo(expFail));
    }

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
      # trackEvent,  pubType,         expAttempt, expSuccess, expFail
      true,          success,         1,          1,          0
      true,          returnFailure,   1,          0,          1
      true,          throwOnPublish,  1,          0,          1
      false,         success,         0,          0,          0
      false,         returnFailure,   0,          0,          0
      false,         throwOnPublish,  0,          0,          0
      """)
    void testCounterForSplitCompleted(
            boolean trackEvent, String pubType, long expAttempt, long expSuccess, long expFail)
            throws InterruptedException {
        var publisher = TestPublisher.from(pubType);
        var config =
                new PubSubEventListenerConfig(
                        false, false, trackEvent, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.splitCompleted(TestData.FULL_SPLIT_COMPLETED_EVENT);

        assertThatEventually(
                eventListener.getPubSubInfo().getSplitCompleted().attempts()::get,
                equalTo(expAttempt));
        assertThatEventually(
                eventListener.getPubSubInfo().getSplitCompleted().successful()::get,
                equalTo(expSuccess));
        assertThatEventually(
                eventListener.getPubSubInfo().getSplitCompleted().failure()::get, equalTo(expFail));
    }

    @Test
    void testQueryCreatedPublishCorrespondingMessage() {
        var publisher = new SuccessPublisher();
        var config = new PubSubEventListenerConfig(true, true, true, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.queryCreated(TestData.FULL_QUERY_CREATED_EVENT);

        assertThat(publisher.lastPublishedMessage, instanceOf(Schema.QueryCreatedEvent.class));
    }

    @Test
    void testQueryCompletedPublishCorrespondingMessage() {
        var publisher = new SuccessPublisher();
        var config = new PubSubEventListenerConfig(true, true, true, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.queryCompleted(TestData.FULL_QUERY_COMPLETED_EVENT);

        assertThat(publisher.lastPublishedMessage, instanceOf(Schema.QueryCompletedEvent.class));
    }

    @Test
    void testSplitCompletedPublishCorrespondingMessage() {
        var publisher = new SuccessPublisher();
        var config = new PubSubEventListenerConfig(true, true, true, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        eventListener.splitCompleted(TestData.FULL_SPLIT_COMPLETED_EVENT);

        assertThat(publisher.lastPublishedMessage, instanceOf(Schema.SplitCompletedEvent.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "throwOnClose"})
    void testClosedNormally(String pubType) {
        var publisher = TestPublisher.from(pubType);
        var config = new PubSubEventListenerConfig(true, true, true, "", "", null, Encoding.JSON);
        var eventListener = new PubSubEventListener(config, publisher);

        assertDoesNotThrow(() -> eventListener.close());
        assertThat(publisher.closeCalled, is(true));
    }

    public static <T> void assertThatEventually(Supplier<T> actual, Matcher<? super T> matcher)
            throws InterruptedException {
        int tries = RETRY_COUNT;
        while (--tries > 0 && !matcher.matches(actual.get())) {
            Thread.sleep(RETRY_DELAY);
        }
        assertThat(actual.get(), matcher);
    }

    abstract static class TestPublisher implements Publisher {
        Message lastPublishedMessage;
        boolean closeCalled;

        @Override
        public CompletableFuture<String> publish(Message message) {
            lastPublishedMessage = message;
            return doPublish(message);
        }

        @Override
        public void close() throws Exception {
            closeCalled = true;
            doClose();
        }

        static TestPublisher from(String pubType) {
            return switch (pubType) {
                case "success" -> new SuccessPublisher();
                case "returnFailure" -> new ReturnFailurePublisher();
                case "throwOnPublish" -> new ThrowOnPublishPublisher();
                case "throwOnClose" -> new ThrowOnClosePublisher();
                default -> throw new IllegalArgumentException("invalid pudType: " + pubType);
            };
        }

        protected CompletableFuture<String> doPublish(Message message) {
            return null;
        }

        protected void doClose() throws Exception {}
    }

    static class SuccessPublisher extends TestPublisher {
        @Override
        public CompletableFuture<String> doPublish(Message message) {
            return CompletableFuture.completedFuture("1");
        }
    }

    static class ReturnFailurePublisher extends TestPublisher {
        @Override
        public CompletableFuture<String> doPublish(Message message) {
            return CompletableFuture.failedFuture(new IOException());
        }
    }

    static class ThrowOnPublishPublisher extends TestPublisher {
        @Override
        public CompletableFuture<String> doPublish(Message message) {
            throw new UncheckedIOException(new IOException());
        }
    }

    static class ThrowOnClosePublisher extends TestPublisher {
        @Override
        public void doClose() throws Exception {
            throw new InterruptedException();
        }
    }
}
