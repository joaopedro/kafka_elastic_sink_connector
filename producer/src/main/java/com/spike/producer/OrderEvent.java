package com.spike.producer;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents an order event produced to Kafka and indexed in Elasticsearch.
 */
public class OrderEvent {

    private String orderId;
    private String customerId;
    private String product;
    private int quantity;
    private double totalPrice;
    private String status;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant createdAt;

    public OrderEvent() {}

    public static OrderEvent random() {
        String[] products = {"Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headphones"};
        String[] statuses = {"PENDING", "CONFIRMED", "SHIPPED", "DELIVERED"};

        OrderEvent e = new OrderEvent();
        e.orderId = UUID.randomUUID().toString();
        e.customerId = "customer-" + (int) (Math.random() * 100);
        e.product = products[(int) (Math.random() * products.length)];
        e.quantity = 1 + (int) (Math.random() * 5);
        e.totalPrice = Math.round((50 + Math.random() * 950) * 100.0) / 100.0;
        e.status = statuses[(int) (Math.random() * statuses.length)];
        e.createdAt = Instant.now();
        return e;
    }

    // Getters and setters

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getTotalPrice() { return totalPrice; }
    public void setTotalPrice(double totalPrice) { this.totalPrice = totalPrice; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    @Override
    public String toString() {
        return String.format("OrderEvent{orderId='%s', customer='%s', product='%s', qty=%d, total=%.2f, status='%s'}",
                orderId, customerId, product, quantity, totalPrice, status);
    }
}
