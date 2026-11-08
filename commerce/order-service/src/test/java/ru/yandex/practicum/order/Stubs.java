package ru.yandex.practicum.order;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Stubs implements AutoCloseable {
    static final String ORDER = """
            {"customerName":"Buyer", "customerEmail":"buyer@example.com", "items":[
              {"productId":42,"productName":"Snapshot lamp","quantity":2,"price":10.25}]}
            """;
    final List<String> requests = new CopyOnWriteArrayList<>();
    volatile int productStatus = 200;
    volatile int inventoryStatus = 200;
    volatile boolean active = true;
    volatile int available = 1000;
    volatile long delayMillis;
    private final HttpServer server;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    Stubs() {
        try {
            server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.setExecutor(executor);
        server.createContext("/api/", exchange -> {
            try (exchange) {
                String path = exchange.getRequestURI().getPath();
                requests.add(path);
                if (delayMillis > 0) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                long id = Long.parseLong(path.substring(path.lastIndexOf('/') + 1));
                boolean product = path.startsWith("/api/products/");
                int status = product ? productStatus : inventoryStatus;
                String body = status == 200
                        ? (product ? "{\"id\":" + id + ",\"active\":" + active + "}"
                        : "{\"productId\":" + id + ",\"availableQuantity\":" + available + "}")
                        : "{\"message\":\"Dependency error\"}";
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        });
        server.start();
    }

    String uri() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    void reset() {
        requests.clear();
        productStatus = 200;
        inventoryStatus = 200;
        active = true;
        available = 1000;
        delayMillis = 0;
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
