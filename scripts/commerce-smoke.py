#!/usr/bin/env python3
"""Check a running local commerce stack. Creates uniquely named smoke-test data."""
import json
import os
import urllib.error
import urllib.request
import uuid
from decimal import Decimal


def request(url, method="GET", body=None, expected=200):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Accept": "application/json", "Content-Type": "application/json"})
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = json.load(response, parse_float=Decimal)
        assert response.status == expected, (url, response.status, payload)
        return payload


def main():
    config = os.environ.get("CONFIG_SERVER_URL", "http://localhost:8888").rstrip("/")
    eureka = os.environ.get("EUREKA_SERVER_URL", "http://localhost:8761/eureka/").rstrip("/")
    applications = request(eureka + "/apps")["applications"]["application"]
    urls = {}
    for service in ("product", "inventory", "order"):
        name = service + "-service"
        sources = request(config + "/" + name + "/default")["propertySources"]
        assert any("config-repo/application.yml" in source["name"] for source in sources)
        assert any("commerce/" + name + ".yml" in source["name"] for source in sources)
        app = next(app for app in applications if app["name"] == name.upper())
        instance = next(instance for instance in app["instance"] if instance["status"] == "UP")
        urls[service] = f'http://{instance["hostName"]}:{instance["port"]["$"]}'
        print(name, urls[service])

    product, inventory, order = (urls[name] for name in ("product", "inventory", "order"))
    suffix = uuid.uuid4().hex[:10]
    category = request(product + "/api/categories", "POST", {"name": "Smoke " + suffix}, 201)
    item = request(product + "/api/products", "POST", {
        "name": "Smoke Lamp " + suffix, "description": "Smoke snapshot", "price": 12.3456,
        "categoryId": category["id"]}, 201)
    item_id = item["id"]
    stock = request(inventory + "/api/inventory", "POST", {"productId": item_id, "quantity": 3}, 201)
    assert stock["availableQuantity"] == 3
    created = request(order + "/api/orders", "POST", {
        "customerName": "Smoke Buyer", "customerEmail": suffix + "@example.com",
        "items": [{"productId": item_id, "productName": item["name"], "quantity": 2, "price": 12.3456}]}, 201)
    assert created["status"] == "CREATED"
    request(product + f"/api/products/{item_id}", "PATCH", {"name": "Changed " + suffix, "price": 99.99})
    stored = request(order + f'/api/orders/{created["id"]}')
    assert stored["items"][0]["productName"] == item["name"]
    assert stored["items"][0]["price"] == Decimal("12.3456")
    assert stored["totalPrice"] == Decimal("24.6912")
    assert request(inventory + f"/api/inventory/{item_id}")["reservedQuantity"] == 0
    reserved = request(inventory + "/api/inventory/reserve", "POST", {"productId": item_id, "quantity": 3})
    assert reserved["success"] and reserved["availableQuantity"] == 0
    request(inventory + "/api/inventory/reserve", "POST", {"productId": item_id, "quantity": 1}, 409)
    request(inventory + "/api/inventory", "PUT", {"productId": item_id, "quantity": 2}, 400)
    assert request(inventory + f"/api/inventory/{item_id}")["reservedQuantity"] == 3
    print("PASS: configuration, discovery, PostgreSQL snapshots, exact prices, independent orders and stock")
    print(f'Persisted IDs: product={item_id}, order={created["id"]}')


if __name__ == "__main__":
    main()
