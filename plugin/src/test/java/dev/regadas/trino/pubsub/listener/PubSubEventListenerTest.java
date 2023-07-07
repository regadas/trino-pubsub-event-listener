package dev.regadas.trino.pubsub.listener;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.protobuf.Message;
import dev.regadas.trino.pubsub.listener.Encoder.Encoding;
import dev.regadas.trino.pubsub.listener.metrics.PubSubEventListenerStats;
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
      # trackEvent,  pubType,          expSuccess, expFail
      true,          success,          1,          0
      true,          return_failure,   0,          1
      true,          throw_on_publish, 0,          1
      false,         success,          0,          0
      false,         return_failure,   0,          0
      false,         throw_on_publish, 0,          0
      """)
    void testCounterForQueryCreated(
            boolean trackEvent, String pubType, long expSuccess, long expFail)
            throws InterruptedException {
        var publisher = TestPublisher.from(pubType);
        var config =
                PubSubEventListenerConfig.builder()
                        .trackQueryCreatedEvent(trackEvent)
                        .trackQueryCompletedEvent(false)
                        .trackSplitCompletedEvent(false)
                        .projectId("")
                        .topicId("")
                        .encoding(Encoding.JSON)
                        .build();
        var countersPerEventType = PubSubEventListenerStats.init();
        var eventListener = PubSubEventListener.create(config, publisher, countersPerEventType);

        eventListener.queryCreated(TestData.FULL_QUERY_CREATED_EVENT);

        assertThatEventually(
                countersPerEventType.getQueryCreated().published()::getTotalCount,
                equalTo(expSuccess));
        assertThatEventually(
                countersPerEventType.getQueryCreated().failed()::getTotalCount, equalTo(expFail));
    }

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
      # trackEvent,  pubType,           expSuccess, expFail
      true,          success,           1,          0
      true,          return_failure,    0,          1
      true,          throw_on_publish,  0,          1
      false,         success,           0,          0
      false,         return_failure,    0,          0
      false,         throw_on_publish,  0,          0
      """)
    void testCounterForQueryCompleted(
            boolean trackEvent, String pubType, long expSuccess, long expFail)
            throws InterruptedException {
        var publisher = TestPublisher.from(pubType);
        var config =
                PubSubEventListenerConfig.builder()
                        .trackQueryCreatedEvent(false)
                        .trackQueryCompletedEvent(trackEvent)
                        .trackSplitCompletedEvent(false)
                        .projectId("")
                        .topicId("")
                        .encoding(Encoding.JSON)
                        .build();
        var countersPerEventType = PubSubEventListenerStats.init();
        var eventListener = PubSubEventListener.create(config, publisher, countersPerEventType);

        eventListener.queryCompleted(TestData.FULL_QUERY_COMPLETED_EVENT);

        assertThatEventually(
                countersPerEventType.getQueryCompleted().published()::getTotalCount,
                equalTo(expSuccess));
        assertThatEventually(
                countersPerEventType.getQueryCompleted().failed()::getTotalCount, equalTo(expFail));
    }

    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
      # trackEvent,  pubType,           expSuccess, expFail
      true,          success,           1,          0
      true,          return_failure,    0,          1
      true,          throw_on_publish,  0,          1
      false,         success,           0,          0
      false,         return_failure,    0,          0
      false,         throw_on_publish,  0,          0
      """)
    void testCounterForSplitCompleted(
            boolean trackEvent, String pubType, long expSuccess, long expFail)
            throws InterruptedException {
        var publisher = TestPublisher.from(pubType);
        var config =
                PubSubEventListenerConfig.builder()
                        .trackQueryCreatedEvent(false)
                        .trackQueryCompletedEvent(false)
                        .trackSplitCompletedEvent(trackEvent)
                        .projectId("")
                        .topicId("")
                        .encoding(Encoding.JSON)
                        .build();
        var countersPerEventType = PubSubEventListenerStats.init();
        var eventListener = PubSubEventListener.create(config, publisher, countersPerEventType);

        eventListener.splitCompleted(TestData.FULL_SPLIT_COMPLETED_EVENT);

        assertThatEventually(
                countersPerEventType.getSplitCompleted().published()::getTotalCount,
                equalTo(expSuccess));
        assertThatEventually(
                countersPerEventType.getSplitCompleted().failed()::getTotalCount, equalTo(expFail));
    }

    @Test
    void testQueryCreatedPublishCorrespondingMessage() {
        var publisher = new SuccessPublisher();
        var config =
                PubSubEventListenerConfig.builder()
                        .trackQueryCreatedEvent(true)
                        .trackQueryCompletedEvent(true)
                        .trackSplitCompletedEvent(true)
                        .projectId("")
                        .topicId("")
                        .encoding(Encoding.JSON)
                        .build();
        var eventListener =
                PubSubEventListener.create(config, publisher, PubSubEventListenerStats.init());

        eventListener.queryCreated(TestData.FULL_QUERY_CREATED_EVENT);

        assertThat(publisher.lastPublishedMessage, instanceOf(Schema.QueryEvent.class));
    }

    @Test
    void testQueryCompletedPublishCorrespondingMessage() {
        var publisher = new SuccessPublisher();
        var config =
                PubSubEventListenerConfig.builder()
                        .trackQueryCreatedEvent(true)
                        .trackQueryCompletedEvent(true)
                        .trackSplitCompletedEvent(true)
                        .projectId("")
                        .topicId("")
                        .encoding(Encoding.JSON)
                        .build();
        var eventListener =
                PubSubEventListener.create(config, publisher, PubSubEventListenerStats.init());

        eventListener.queryCompleted(TestData.FULL_QUERY_COMPLETED_EVENT);

        assertThat(publisher.lastPublishedMessage, instanceOf(Schema.QueryEvent.class));
    }

    @Test
    void testSplitCompletedPublishCorrespondingMessage() {
        var publisher = new SuccessPublisher();
        var config =
                PubSubEventListenerConfig.builder()
                        .trackQueryCreatedEvent(true)
                        .trackQueryCompletedEvent(true)
                        .trackSplitCompletedEvent(true)
                        .projectId("")
                        .topicId("")
                        .encoding(Encoding.JSON)
                        .build();
        var eventListener =
                PubSubEventListener.create(config, publisher, PubSubEventListenerStats.init());

        eventListener.splitCompleted(TestData.FULL_SPLIT_COMPLETED_EVENT);

        assertThat(publisher.lastPublishedMessage, instanceOf(Schema.QueryEvent.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "throw_on_close"})
    void testClosedNormally(String pubType) {
        var publisher = TestPublisher.from(pubType);
        var config =
                PubSubEventListenerConfig.builder()
                        .trackQueryCreatedEvent(true)
                        .trackQueryCompletedEvent(true)
                        .trackSplitCompletedEvent(true)
                        .projectId("")
                        .topicId("")
                        .encoding(Encoding.JSON)
                        .build();
        var eventListener =
                PubSubEventListener.create(config, publisher, PubSubEventListenerStats.init());

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
        enum PubType {
            SUCCESS,
            RETURN_FAILURE,
            THROW_ON_PUBLISH,
            THROW_ON_CLOSE
        }

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
            return switch (PubType.valueOf(pubType.toUpperCase())) {
                case SUCCESS -> new SuccessPublisher();
                case RETURN_FAILURE -> new ReturnFailurePublisher();
                case THROW_ON_PUBLISH -> new ThrowOnPublishPublisher();
                case THROW_ON_CLOSE -> new ThrowOnClosePublisher();
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
