package ru.yandex.practicum.collector;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;
import ru.yandex.practicum.collector.config.JsonConfiguration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.collector.mapping.SensorEventMapper;
import ru.yandex.practicum.collector.mapping.HubEventMapper;
import ru.yandex.practicum.collector.http.*;
import ru.yandex.practicum.collector.kafka.PublicationException;
import ru.yandex.practicum.collector.service.EventCollector;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.assertj.core.api.Assertions.*;
@org.junit.jupiter.api.extension.ExtendWith(org.springframework.boot.test.system.OutputCaptureExtension.class)
@WebMvcTest(EventController.class)
@Import({JsonConfiguration.class, SensorEventMapper.class, HubEventMapper.class})
class EventControllerTest {
    @Autowired MockMvc mvc;
    @MockBean EventCollector collector;

    @BeforeEach void setup() {
        when(collector.collect(any(SensorEventAvro.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(collector.collect(any(HubEventAvro.class))).thenReturn(CompletableFuture.completedFuture(null));
    }

    static Stream<EventFixtures.Event> events() { return EventFixtures.events().stream(); }

    @ParameterizedTest @MethodSource("events")
    void acceptsAllEvents(EventFixtures.Event e) throws Exception {
        var result = mvc.perform(post("/events/" + e.path()).contentType("application/json").content(e.json()))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(result)).andExpect(status().isOk()).andExpect(content().string(""));
    }

    static Stream<EventFixtures.Event> invalid() {
        var all = EventFixtures.events();
        return Stream.of(
            new EventFixtures.Event("sensors", "{}"),
            new EventFixtures.Event("sensors", all.get(3).json().replace("\"id\":\"sensor-1\"", "\"id\":123")),
            new EventFixtures.Event("sensors", all.get(3).json().replace("\"2024-08-06T15:11:24.157123Z\"", "123")),
            new EventFixtures.Event("hubs", all.get(5).json().replace("\"deviceType\":\"SWITCH_SENSOR\"", "\"deviceType\":4")),
            new EventFixtures.Event("sensors", "{"),
            new EventFixtures.Event("sensors", all.get(3).json().replace("SWITCH_SENSOR_EVENT", "UNKNOWN")),
            new EventFixtures.Event("sensors", all.get(3).json().replace("\"state\":false", "\"state\":null")),
            new EventFixtures.Event("sensors", all.get(3).json().replace("\"state\":false", "\"state\":\"false\"")),
            new EventFixtures.Event("sensors", all.get(0).json().replace("\"temperatureC\":21", "\"temperatureC\":1.5")),
            new EventFixtures.Event("sensors", all.get(0).json().replace("\"temperatureC\":21,", "")),
            new EventFixtures.Event("sensors", all.get(1).json().replace("\"linkQuality\":75", "\"linkQuality\":null")),
            new EventFixtures.Event("sensors", all.get(1).json().replace("\"2024-08-06T15:11:24.157123Z\"", "null")),
            new EventFixtures.Event("sensors", all.get(1).json().replace("2024-08-06T15:11:24.157123Z", "bad-date")),
            new EventFixtures.Event("hubs", all.get(8).json().replace("Автосвет", "ab")),
            new EventFixtures.Event("hubs", "{\"hubId\":\"h\",\"type\":\"SCENARIO_ADDED\",\"name\":\"test\",\"conditions\":[],\"actions\":[]}"),
            new EventFixtures.Event("hubs", all.get(7).json().replace("\"value\":1", "\"value\":2")),
            new EventFixtures.Event("hubs", all.get(7).json().replace("\"sensorId\":\"motion-1\"", "\"sensorId\":\"\""))
        );
    }

    @ParameterizedTest @MethodSource("invalid")
    void rejectsInvalidWithoutPublishing(EventFixtures.Event e) throws Exception {
        mvc.perform(post("/events/" + e.path()).contentType("application/json").content(e.json())).andExpect(status().isBadRequest());
        verifyNoInteractions(collector);
    }

    @Test void serializationFailureReturns500(org.springframework.boot.test.system.CapturedOutput output) throws Exception {
        when(collector.collect(any(SensorEventAvro.class))).thenReturn(CompletableFuture.failedFuture(
                new org.apache.kafka.common.errors.SerializationException("encoding failed")));
        var result = mvc.perform(post("/events/sensors").contentType("application/json")
                .content(EventFixtures.events().get(3).json())).andReturn();
        mvc.perform(asyncDispatch(result)).andExpect(status().isInternalServerError());
        assertThat(output.getOut()).contains("Unable to serialize event; returning HTTP 500", "encoding failed");
    }

    @Test void defaultsTimestampAndLightValues() throws Exception {
        var start = java.time.Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        var result = mvc.perform(post("/events/sensors").contentType("application/json")
                .content("{\"id\":\"s\",\"hubId\":\"h\",\"type\":\"LIGHT_SENSOR_EVENT\"}"))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(result)).andExpect(status().isOk());
        var captor = org.mockito.ArgumentCaptor.forClass(SensorEventAvro.class);
        verify(collector).collect(captor.capture());
        var event = captor.getValue();
        var light = (LightSensorAvro) event.getPayload();
        assertThat(event.getTimestamp()).isBetween(start, java.time.Instant.now());
        assertThat(light.getLinkQuality()).isZero(); assertThat(light.getLuminosity()).isZero();
    }

    @Test void waitsForAcknowledgementAndReturns503OnFailure(org.springframework.boot.test.system.CapturedOutput output) throws Exception {
        var pending = new CompletableFuture<Void>();
        when(collector.collect(any(SensorEventAvro.class))).thenReturn(pending);
        var result = mvc.perform(post("/events/sensors").contentType("application/json")
                .content(EventFixtures.events().get(3).json())).andExpect(request().asyncStarted()).andReturn();
        assertThat(pending).isNotDone();
        pending.completeExceptionally(new PublicationException(new RuntimeException("broker unavailable")));
        mvc.perform(asyncDispatch(result)).andExpect(status().isServiceUnavailable());
        assertThat(output.getOut()).contains("Unable to publish event to Kafka; returning HTTP 503", "broker unavailable");
    }
}
