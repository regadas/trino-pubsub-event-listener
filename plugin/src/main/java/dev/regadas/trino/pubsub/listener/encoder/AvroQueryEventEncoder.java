package dev.regadas.trino.pubsub.listener.encoder;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.jsr310.AvroJavaTimeModule;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import dev.regadas.trino.pubsub.listener.event.QueryEvent;

public class AvroQueryEventEncoder implements Encoder<QueryEvent> {

    @VisibleForTesting static final AvroSchema avroSchema;
    private static final ObjectMapper mapper =
            AvroMapper.builder()
                    .addModule(new Jdk8Module())
                    .addModule(new JavaTimeModule())
                    .addModule(new AvroJavaTimeModule())
                    .build();

    static {
        var gen = new AvroSchemaGenerator().enableLogicalTypes();
        try {
            mapper.acceptJsonFormatVisitor(QueryEvent.class, gen);
        } catch (JsonMappingException e) {
            throw new RuntimeException("Could not generate avro schema for QueryEvent", e);
        }
        avroSchema = gen.getGeneratedSchema();
    }

    @Override
    public byte[] encode(QueryEvent event) throws Exception {
        return mapper.writer(avroSchema).writeValueAsBytes(event);
    }
}
