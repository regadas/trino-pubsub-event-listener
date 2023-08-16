package dev.regadas.trino.pubsub.listener.encoder;

@FunctionalInterface
public interface Encoder<T> {

    byte[] encode(T value) throws Exception;
}
