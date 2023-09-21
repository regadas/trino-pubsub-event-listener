package dev.regadas.trino.pubsub.listener.encoder.databinding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SafeJsonifier {
    private static final Logger LOG = Logger.getLogger(SafeJsonifier.class.getPackage().getName());
    public static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new Jdk8Module())
                    .registerModule(new JavaTimeModule())
                    .registerModule(PatchSchemaModule.create());

    static Optional<String> jsonify(Object obj, String prop) {
        try {
            return Optional.of(MAPPER.writeValueAsString(obj));
        } catch (JsonProcessingException exc) {
            LOG.log(
                    Level.WARNING,
                    "Could not serialize property: "
                            + prop
                            + " with type: "
                            + obj.getClass().getCanonicalName());
            return Optional.empty();
        }
    }

    static Optional<String> jsonifyUnquoted(Object obj, String prop) {
        return jsonify(obj, prop).map(SafeJsonifier::unquote);
    }

    static Optional<String> jsonify(Map<String, ?> map, String prop) {
        try {
            var jsonifiedMap =
                    map.entrySet().stream()
                            .map(
                                    e ->
                                            Map.entry(
                                                    e.getKey(),
                                                    jsonify(
                                                            e.getValue(),
                                                            "%s[%s]".formatted(prop, e.getKey()))))
                            .flatMap(e -> e.getValue().map(v -> Map.entry(e.getKey(), v)).stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return Optional.of(MAPPER.writeValueAsString(jsonifiedMap));
        } catch (JsonProcessingException exc) {
            LOG.log(
                    Level.WARNING,
                    "Could not serialize property: "
                            + prop
                            + " with type: "
                            + map.getClass().getCanonicalName());
            return Optional.empty();
        }
    }

    static Optional<String> jsonifyUnquoted(Map<String, ?> map, String prop) {
        return jsonify(map, prop).map(SafeJsonifier::unquote);
    }

    private static String unquote(String str) {
        return str.replaceAll("^\"", "").replaceAll("\"$", "").replaceAll("\\\\\"", "\"");
    }
}
