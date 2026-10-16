package ru.yandex.practicum.collector;
import java.util.List;
public final class EventFixtures {
    private EventFixtures() { }

    public record Event(String path, String json) { }

    public static List<Event> events() {
        String sensor = "\"id\":\"sensor-1\",\"hubId\":\"hub-1\",\"timestamp\":\"2024-08-06T15:11:24.157123Z\",";
        String hub = "\"hubId\":\"hub-2\",\"timestamp\":\"2024-08-06T15:11:24.157Z\",";
        return List.of(
            new Event("sensors", "{" + sensor + "\"type\":\"CLIMATE_SENSOR_EVENT\",\"temperatureC\":21,\"humidity\":40,\"co2Level\":800}"),
            new Event("sensors", "{" + sensor + "\"type\":\"LIGHT_SENSOR_EVENT\",\"linkQuality\":75,\"luminosity\":59}"),
            new Event("sensors", "{" + sensor + "\"type\":\"MOTION_SENSOR_EVENT\",\"linkQuality\":0,\"motion\":false,\"voltage\":12}"),
            new Event("sensors", "{" + sensor + "\"type\":\"SWITCH_SENSOR_EVENT\",\"state\":false}"),
            new Event("sensors", "{" + sensor + "\"type\":\"TEMPERATURE_SENSOR_EVENT\",\"temperatureC\":0,\"temperatureF\":32}"),
            new Event("hubs", "{" + hub + "\"type\":\"DEVICE_ADDED\",\"id\":\"switch-1\",\"deviceType\":\"SWITCH_SENSOR\"}"),
            new Event("hubs", "{" + hub + "\"type\":\"DEVICE_REMOVED\",\"id\":\"switch-1\"}"),
            new Event("hubs", "{" + hub + "\"type\":\"SCENARIO_ADDED\",\"name\":\"Автосвет\",\"conditions\":["
                + "{\"sensorId\":\"motion-1\",\"type\":\"MOTION\",\"operation\":\"EQUALS\",\"value\":1},"
                + "{\"sensorId\":\"light-1\",\"type\":\"LUMINOSITY\",\"operation\":\"LOWER_THAN\",\"value\":500},"
                + "{\"sensorId\":\"switch-1\",\"type\":\"SWITCH\",\"operation\":\"EQUALS\",\"value\":0},"
                + "{\"sensorId\":\"climate-1\",\"type\":\"TEMPERATURE\",\"operation\":\"GREATER_THAN\",\"value\":null}],"
                + "\"actions\":[{\"sensorId\":\"switch-1\",\"type\":\"ACTIVATE\"},"
                + "{\"sensorId\":\"switch-2\",\"type\":\"SET_VALUE\",\"value\":5}]}"),
            new Event("hubs", "{" + hub + "\"type\":\"SCENARIO_REMOVED\",\"name\":\"Автосвет\"}")
        );
    }
}
