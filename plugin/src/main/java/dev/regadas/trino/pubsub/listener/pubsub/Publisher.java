package dev.regadas.trino.pubsub.listener.pubsub;

import com.google.protobuf.Message;
import java.util.concurrent.CompletableFuture;

public interface Publisher extends AutoCloseable {
    CompletableFuture<String> publish(Message message);
}
