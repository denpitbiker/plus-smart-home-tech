package ru.yandex.practicum.product;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.servers.Server;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@OpenAPIDefinition(servers = @Server(url = "/"))
@SpringBootApplication
public class ProductServiceApp {

    public static void main(String[] args) {
        SpringApplication.run(ProductServiceApp.class, args);
    }
}