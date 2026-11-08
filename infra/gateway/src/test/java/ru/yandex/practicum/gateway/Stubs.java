package ru.yandex.practicum.gateway;

import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

final class Stubs implements AutoCloseable {
    static final String ERROR = "{\"status\":404,\"message\":\"Product not found\"}";

    final List<String> requests = new CopyOnWriteArrayList<>();
    final List<String> bodies = new CopyOnWriteArrayList<>();
    final Queue<Integer> statuses = new ConcurrentLinkedQueue<>();
    private final DisposableServer server;

    Stubs(String serviceName) {
        server = HttpServer.create().host("localhost").port(0)
                .handle((request, response) -> request.receive().aggregate().asString()
                        .defaultIfEmpty("").flatMap(body -> {
                            requests.add(request.method().name() + " " + request.uri());
                            bodies.add(body);
                            Integer status = statuses.poll();
                            int code = status == null ? 200 : status;
                            return response.status(code).header("Content-Type", "application/json")
                                    .sendString(Mono.just(code == 404 ? ERROR
                                            : "{\"service\":\"" + serviceName + "\"}")).then();
                        }))
                .bindNow();
    }

    String uri() {
        return "http://localhost:" + server.port();
    }

    void reset() {
        requests.clear();
        bodies.clear();
        statuses.clear();
    }

    @Override
    public void close() {
        server.disposeNow();
    }
}
