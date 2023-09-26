package dev.regadas.trino.pubsub.listener.encoder.databinding;

import static dev.regadas.trino.pubsub.listener.encoder.databinding.SafeJsonifier.jsonifyUnquoted;

import com.fasterxml.jackson.databind.util.StdConverter;
import java.util.Optional;

public class ConnectorInfoConverter extends StdConverter<Optional<Object>, String> {
    @Override
    public String convert(Optional<Object> value) {
        return value.flatMap(v -> jsonifyUnquoted(v, "connectorInfo")).orElse(null);
    }
}
