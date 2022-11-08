package com.github.lbroudoux.tradegame.priceupdater;

import com.github.lbroudoux.tradegame.model.Order;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper type for a snapshot of list of orders.
 * @author laurent
 */
public class OrderListSnapshot {

   private Long timestamp;
   private List<Order> orders = new ArrayList<Order>();

   public OrderListSnapshot() {
   }

   public void setTimestamp(Long timestamp) {
      this.timestamp = timestamp;
   }
   public Long getTimestamp() {
      return timestamp;
   }
   public void setOrders(List<Order> orders) {
      this.orders = orders;
   }
   public List<Order> getOrders() {
      return orders;
   }

   public void addOrder(Order order) {
      if (!orders.contains(order)) {
         orders.add(order);
      }
   }
}
