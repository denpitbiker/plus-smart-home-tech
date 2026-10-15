package ru.yandex.practicum.collector;
import java.net.ServerSocket;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import static org.assertj.core.api.Assertions.*;
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class UnavailableKafkaIT {
    @Autowired TestRestTemplate http;

    @DynamicPropertySource static void offline(DynamicPropertyRegistry registry) throws Exception {
        // Reserve a local non-Kafka listener for the duration of the test JVM.
        var socket = new ServerSocket(0);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { socket.close(); } catch (Exception ignored) { } }));
        registry.add("collector.kafka.bootstrap-servers", () -> "localhost:" + socket.getLocalPort());
    }

    @Test void returns503InsteadOfSuccess() {
        var headers = new HttpHeaders(); headers.setContentType(MediaType.APPLICATION_JSON);
        var response = http.postForEntity("/events/sensors", new HttpEntity<>(EventFixtures.events().get(3).json(), headers), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }
}
