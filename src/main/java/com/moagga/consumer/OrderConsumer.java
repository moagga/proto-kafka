package com.moagga.consumer;

import com.moagga.serde.ProtoBufDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class OrderConsumer implements Runnable {

  private final KafkaConsumer<String, ?> consumer;

  private volatile boolean running = false;

  private final ConsumerCallback callback;

  public OrderConsumer(String host, ConsumerCallback callback) {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtoBufDeserializer.class);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "first-consumer-group");

    consumer = new KafkaConsumer<>(config);
    consumer.subscribe(Arrays.asList("orders", "users"));

    this.callback = callback;
  }

  public void start() {
    running = true;
  }

  @Override
  public void run() {
    while (running) {
      ConsumerRecords<String, ?> records = consumer.poll(Duration.ofSeconds(2));
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
