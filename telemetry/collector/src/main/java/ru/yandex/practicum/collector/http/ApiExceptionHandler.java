package ru.yandex.practicum.collector.http;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.kafka.PublicationException;
@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(PublicationException.class)
    ResponseEntity<Void> unavailable(PublicationException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
    }

    @ExceptionHandler(SerializationException.class)
    ResponseEntity<Void> serialization(SerializationException exception) {
        return ResponseEntity.internalServerError().build();
    }
}
