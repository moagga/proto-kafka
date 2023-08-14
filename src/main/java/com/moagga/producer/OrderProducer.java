package com.moagga.producer;

import com.moagga.proto.order.Item;
import com.moagga.proto.order.Order;
import com.moagga.proto.order.PaymentMethod;
import com.moagga.serde.ProtoBufSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class OrderProducer {

  private Producer<String, Order> producer;

  public OrderProducer(String host) {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoBufSerializer.class);
    producer = new KafkaProducer<>(config);
  }

  public void produceMessage(Order order) {
    ProducerRecord<String, Order> record = new ProducerRecord<>("orders", Long.toString(order.getOrderId()), order);
    producer.send(record);
    producer.flush();
    System.out.println("Message produced");
  }
}
