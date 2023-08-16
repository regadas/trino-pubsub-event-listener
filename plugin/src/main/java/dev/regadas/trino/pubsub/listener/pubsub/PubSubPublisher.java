package dev.regadas.trino.pubsub.listener.pubsub;

import static java.util.Objects.requireNonNull;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import dev.regadas.trino.pubsub.listener.encoder.CompressionEncoder;
import dev.regadas.trino.pubsub.listener.encoder.Encoder;
import dev.regadas.trino.pubsub.listener.encoder.Encoding;
import dev.regadas.trino.pubsub.listener.encoder.MessageEncoder;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class PubSubPublisher implements Publisher {
    private final com.google.cloud.pubsub.v1.Publisher publisher;
    private final Encoder<Message> encoder;

    public PubSubPublisher(
            com.google.cloud.pubsub.v1.Publisher publisher, Encoder<Message> encoder) {
        this.publisher = requireNonNull(publisher, "publisher is null");
        this.encoder = requireNonNull(encoder, "encoder is null");
    }

    public static PubSubPublisher create(
            String projectId,
            String topicName,
            Encoding encoding,
            @Nullable String credentialsFilePath,
            BatchingSettings batchingSettings)
            throws IOException {
        var credentials =
                (credentialsFilePath == null)
                        ? GoogleCredentials.getApplicationDefault()
                        : GoogleCredentials.fromStream(new FileInputStream(credentialsFilePath));

        var publisher =
                com.google.cloud.pubsub.v1.Publisher.newBuilder(TopicName.of(projectId, topicName))
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                        .setEnableCompression(true)
                        .setBatchingSettings(batchingSettings)
                        .build();

        var encoder = CompressionEncoder.create(MessageEncoder.create(encoding));
        return new PubSubPublisher(publisher, encoder);
    }

    @Override
    public CompletableFuture<String> publish(Message message) {
        try {
            var data = ByteString.copyFrom(encoder.encode(message));
            var pubSubMessage = PubsubMessage.newBuilder().setData(data).build();

            var future = publisher.publish(pubSubMessage);

            return toCompletableFuture(future);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close() throws InterruptedException {
        publisher.shutdown();
        publisher.awaitTermination(60, TimeUnit.SECONDS);
    }

    private <T> CompletableFuture<T> toCompletableFuture(ApiFuture<T> apiFuture) {
        var cf = new CompletableFuture<T>();
        ApiFutures.addCallback(
                apiFuture,
                new ApiFutureCallback<>() {
                    @Override
                    public void onFailure(Throwable t) {
                        cf.completeExceptionally(t);
                    }

                    @Override
                    public void onSuccess(T result) {
                        cf.complete(result);
                    }
                },
                MoreExecutors.directExecutor());
        return cf;
    }
}
