package dev.regadas.trino.pubsub.listener.encoder;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.regadas.trino.pubsub.listener.event.QueryEvent;

public class JsonQueryEventEncoder implements QueryEventEncoder {
    private static final ObjectMapper mapper =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .registerModule(new Jdk8Module());

    @Override
    public byte[] encode(QueryEvent event) throws Exception {
        return mapper.writeValueAsString(event).getBytes(UTF_8);
    }
}
