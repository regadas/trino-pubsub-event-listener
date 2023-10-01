package dev.regadas.trino.pubsub.listener;

import dev.regadas.trino.pubsub.listener.event.QueryEvent;
import java.util.concurrent.CompletableFuture;

public interface Publisher extends AutoCloseable {
    CompletableFuture<String> publish(QueryEvent event);
}
