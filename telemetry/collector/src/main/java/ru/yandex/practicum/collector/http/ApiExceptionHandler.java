package ru.yandex.practicum.collector.http;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.kafka.PublicationException;
@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(PublicationException.class)
    ResponseEntity<Void> unavailable(PublicationException exception) {
        log.error("Unable to publish event to Kafka; returning HTTP 503", exception);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
    }

    @ExceptionHandler(SerializationException.class)
    ResponseEntity<Void> serialization(SerializationException exception) {
        log.error("Unable to serialize event; returning HTTP 500", exception);
        return ResponseEntity.internalServerError().build();
    }
}
