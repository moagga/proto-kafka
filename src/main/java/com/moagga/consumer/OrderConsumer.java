package com.moagga.consumer;

import com.google.protobuf.Message;
import com.moagga.proto.order.Order;
import com.moagga.serde.ProtoBufDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class OrderConsumer implements Runnable {

  private KafkaConsumer<String, Order> consumer;

  private volatile boolean running = false;

  private ConsumerCallback callback;

  public OrderConsumer(String host, ConsumerCallback callback) {
    Map<String, Class<? extends Message>> deserializerClasses = new HashMap<>();
    deserializerClasses.put("orders", Order.class);

    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtoBufDeserializer.class);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "first-consumer-group");
    config.put(ProtoBufDeserializer.TARGET_CLASSES, deserializerClasses);

    consumer = new KafkaConsumer<>(config);
    consumer.subscribe(Collections.singletonList("orders"));
    System.out.println("Consumer subscribed");

    this.callback = callback;
  }

  public void start() {
    running = true;
  }

  @Override
  public void run() {
    while (running) {
      ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(2));
      System.out.println("Consumer polled:" + records.count());
      if (!records.isEmpty()) {
        callback.onMessage(records);
      }
      consumer.commitAsync();
    }
    consumer.close();
  }

  public void stop() {
    running = false;
  }

}
