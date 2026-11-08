package ru.yandex.practicum.gateway;

final class GatewayTestData {
    static final String IVAN_USERNAME = "ivan";
    static final String IVAN_PASSWORD = "ivan";
    static final String ANNA_USERNAME = "anna";
    static final String ANNA_PASSWORD = "anna";
    static final String WRONG_PASSWORD = "wrong";

    static final String IVAN_PASSWORD_PROPERTY = "SECURITY_IVAN_PASSWORD=" + IVAN_PASSWORD;
    static final String ANNA_PASSWORD_PROPERTY = "SECURITY_ANNA_PASSWORD=" + ANNA_PASSWORD;

    private GatewayTestData() {
    }
}
