package dev.regadas.trino.pubsub.listener.encoder;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.jsr310.AvroJavaTimeModule;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import dev.regadas.trino.pubsub.listener.encoder.databinding.PatchSchemaModule;
import dev.regadas.trino.pubsub.listener.event.QueryEvent;

public class AvroQueryEventEncoder implements Encoder<QueryEvent> {

    @VisibleForTesting static final AvroSchema avroSchema;

    private static final AvroMapper MAPPER =
            AvroMapper.builder()
                    .addModule(new Jdk8Module())
                    .addModule(new JavaTimeModule())
                    .addModule(new AvroJavaTimeModule())
                    .addModule(PatchSchemaModule.create())
                    .build();
    private static final ObjectWriter WRITER;

    static {
        var gen = new AvroSchemaGenerator().enableLogicalTypes();
        try {
            MAPPER.acceptJsonFormatVisitor(QueryEvent.class, gen);
        } catch (JsonMappingException e) {
            throw new RuntimeException("Could not generate avro schema for QueryEvent", e);
        }
        avroSchema = gen.getGeneratedSchema();
        WRITER = MAPPER.writer(avroSchema);
    }

    @Override
    public byte[] encode(QueryEvent event) throws Exception {
        return WRITER.writeValueAsBytes(event);
    }

    @VisibleForTesting
    byte[] encode(Object obj, AvroSchema schema) throws Exception {
        return MAPPER.writer(schema).writeValueAsBytes(obj);
    }
}
