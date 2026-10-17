package ru.yandex.practicum.collector.grpc;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.practicum.collector.handler.EventDispatcher;
import ru.yandex.practicum.collector.kafka.PublicationException;
import ru.yandex.practicum.collector.service.EventCollector;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@GrpcService
public class GrpcEventController extends CollectorControllerGrpc.CollectorControllerImplBase {
    private static final Logger log = LoggerFactory.getLogger(GrpcEventController.class);
    private final EventDispatcher dispatcher;
    private final EventCollector collector;

    public GrpcEventController(EventDispatcher dispatcher, EventCollector collector) {
        this.dispatcher = dispatcher;
        this.collector = collector;
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        collect(request.getHubId(), request.getPayloadCase().name(), () -> dispatcher.map(request),
                collector::collect, responseObserver);
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        collect(request.getHubId(), request.getPayloadCase().name(), () -> dispatcher.map(request),
                collector::collect, responseObserver);
    }

    private <T> void collect(String hub, String type, Supplier<T> mapping,
                             Function<T, CompletableFuture<Void>> publish, StreamObserver<Empty> observer) {
        var reply = new Reply(observer);
        if (reply.isDone()) {
            return;
        }
        T event;
        try {
            event = mapping.get();
        } catch (IllegalArgumentException error) {
            fail(reply, hub, type, Status.INVALID_ARGUMENT.withDescription("Invalid event data"), error);
            return;
        } catch (RuntimeException error) {
            fail(reply, hub, type, Status.INTERNAL.withDescription("Unable to process event"), error);
            return;
        }
        if (reply.isDone()) {
            return;
        }
        try {
            publish.apply(event).whenComplete((ignored, error) -> {
                if (error == null) {
                    reply.complete(null);
                } else {
                    publicationFailed(reply, hub, type, error);
                }
            });
        } catch (RuntimeException error) {
            publicationFailed(reply, hub, type, error);
        }
    }

    private void publicationFailed(Reply reply, String hub, String type, Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) {
            error = error.getCause();
        }
        Status status = error instanceof PublicationException
                ? Status.UNAVAILABLE.withDescription("Unable to publish event")
                : Status.INTERNAL.withDescription("Unable to process event");
        fail(reply, hub, type, status, error);
    }

    private void fail(Reply reply, String hub, String type, Status status, Throwable error) {
        log.error("gRPC event failed: hub={}, type={}, status={}", hub, type, status.getCode(), error);
        reply.complete(status);
    }

    // Cancellation and Kafka callbacks can arrive on different threads. Serialize observer access.
    private static final class Reply {
        private final StreamObserver<Empty> observer;
        private final ServerCallStreamObserver<Empty> serverObserver;
        private boolean done;

        private Reply(StreamObserver<Empty> observer) {
            this.observer = observer;
            this.serverObserver = observer instanceof ServerCallStreamObserver<Empty> server ? server : null;
            if (serverObserver != null) {
                serverObserver.setOnCancelHandler(this::cancel);
            }
        }

        private synchronized void cancel() {
            done = true;
        }

        private synchronized boolean isDone() {
            return done || (serverObserver != null && serverObserver.isCancelled());
        }

        private synchronized void complete(Status failure) {
            if (isDone()) {
                return;
            }
            done = true;
            if (failure == null) {
                observer.onNext(Empty.getDefaultInstance());
                observer.onCompleted();
            } else {
                observer.onError(failure.asRuntimeException());
            }
        }
    }
}
