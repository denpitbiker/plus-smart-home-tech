package ru.yandex.practicum.analyzer.kafka;

import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AnalysisRunner implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AnalysisRunner.class);
    private final HubEventProcessor hubs;
    private final SnapshotProcessor snapshots;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public AnalysisRunner(HubEventProcessor hubs, SnapshotProcessor snapshots) {
        this.hubs = hubs;
        this.snapshots = snapshots;
    }

    public void start() {
        Thread hubThread = new Thread(() -> guarded(hubs), "HubEventHandlerThread");
        hubThread.start();
        try {
            guarded(snapshots::start);
        } finally {
            close();
            boolean interrupted = false;
            while (hubThread.isAlive()) {
                try {
                    hubThread.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (failure.get() != null) {
            throw new IllegalStateException("Analyzer processing failed", failure.get());
        }
    }

    private void guarded(Runnable processor) {
        try {
            processor.run();
        } catch (Throwable e) {
            failure.compareAndSet(null, e);
            log.error("Analyzer processor failed", e);
        } finally {
            hubs.loop.requestStop();
            snapshots.loop.requestStop();
        }
    }

    @Override
    public void close() {
        hubs.loop.requestStop();
        snapshots.loop.requestStop();
        try {
            hubs.loop.close();
        } finally {
            snapshots.loop.close();
        }
    }
}
