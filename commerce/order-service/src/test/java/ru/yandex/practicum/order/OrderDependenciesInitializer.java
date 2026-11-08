package ru.yandex.practicum.order;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.test.context.support.TestPropertySourceUtils;

public class OrderDependenciesInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext context) {
        Stubs stubs = new Stubs();
        context.getBeanFactory().registerSingleton("orderDependencies", stubs);
        context.addApplicationListener(event -> {
            if (event instanceof ContextClosedEvent closed && closed.getApplicationContext() == context) {
                stubs.close();
            }
        });
        TestPropertySourceUtils.addInlinedPropertiesToEnvironment(context,
                "spring.cloud.discovery.client.simple.instances.product-service[0].uri=" + stubs.uri(),
                "spring.cloud.discovery.client.simple.instances.inventory-service[0].uri=" + stubs.uri());
    }
}
