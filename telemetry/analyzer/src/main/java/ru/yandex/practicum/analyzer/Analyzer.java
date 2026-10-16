package ru.yandex.practicum.analyzer;

import org.springframework.boot.SpringApplication;
import ru.yandex.practicum.analyzer.kafka.AnalysisRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {

    public static void main(String[] args) {
        try (var context = SpringApplication.run(Analyzer.class, args)) {
            context.getBean(AnalysisRunner.class).start();
        }
    }
}
