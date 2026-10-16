package ru.yandex.practicum.aggregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Aggregator {
    public static void main(String[] args) {
        try (var context = SpringApplication.run(Aggregator.class, args)) {
            context.getBean(AggregationStarter.class).start();
        }
    }
}
