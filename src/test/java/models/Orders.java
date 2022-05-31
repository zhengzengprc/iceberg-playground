package models;

import java.util.Objects;

public class Orders {
    private String orderId;
    private String userId;

    public Orders(String orderId, String userId) {
        this.orderId = orderId;
        this.userId = userId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Orders orders = (Orders) o;

        if (!Objects.equals(orderId, orders.orderId)) return false;
        return Objects.equals(userId, orders.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, userId);
    }

    @Override
    public String toString() {
        return "Orders [orderId=" + orderId + ", userId=" + userId + "]";
    }
}
