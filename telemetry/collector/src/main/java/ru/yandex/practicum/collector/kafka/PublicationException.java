package ru.yandex.practicum.collector.kafka;
public class PublicationException extends RuntimeException {
    public PublicationException(Throwable cause) { super("Kafka publication failed", cause); }
}
