package ru.yandex.practicum.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import ru.yandex.practicum.analyzer.kafka.AnalysisRunner;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {

    public static void main(String[] args) {
        var application = new SpringApplication(Analyzer.class);
        application.setRegisterShutdownHook(false);
        var context = application.run(args);
        // Main and SIGTERM must not close Spring concurrently: its concurrent-close
        // detection can interrupt the hook while it is shutting down the database pool.
        Runnable closeContext = () -> {
            synchronized (context) {
                context.close();
            }
        };
        var shutdownHook = new Thread(closeContext, "AnalyzerShutdownHook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try {
            context.getBean(AnalysisRunner.class).start();
        } finally {
            closeContext.run();
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM shutdown is already in progress; the hook uses the same lock.
            }
        }
    }
}
