package ru.yandex.practicum.kafka.telemetry.serialization.json;

import java.io.IOException;
import java.time.Instant;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;

/** The HTTP contract requires an ISO date-time string, never an epoch number. */
public class StrictInstantDeserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        if (!parser.hasToken(JsonToken.VALUE_STRING)) {
            return (Instant) context.handleUnexpectedToken(Instant.class, parser);
        }
        return InstantDeserializer.INSTANT.deserialize(parser, context);
    }
}
