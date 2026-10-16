package ru.yandex.practicum.kafka.telemetry.serialization;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroSerializerTest {
    @Test
    void concurrentCallsDoNotShareEncodingBuffers() throws Exception {
        var serializer = new AvroSerializer();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var jobs = new ArrayList<Future<?>>();
            for (int i = 0; i < 100; i++) {
                int index = i;
                jobs.add(executor.submit(() -> {
                    var record = new SensorEventAvro("sensor-" + index, "hub-" + index,
                            Instant.ofEpochMilli(index), new LightSensorAvro(index, index * 10));
                    var bytes = serializer.serialize("topic", record);
                    var decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                    var decoded = new SpecificDatumReader<SensorEventAvro>(record.getSchema()).read(null, decoder);
                    assertEquals(record, decoded);
                    assertTrue(decoder.isEnd());
                    return null;
                }));
            }
            for (var job : jobs) {
                job.get();
            }
        }
    }
}
