package com.moagga.consumer;

import com.moagga.proto.order.Order;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ConsumerCallback {

  void onMessage(ConsumerRecords<String, Order> records);

}
