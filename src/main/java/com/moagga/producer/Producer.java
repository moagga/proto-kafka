package com.moagga.producer;

import com.moagga.proto.order.Order;
import com.moagga.proto.user.User;
import com.moagga.serde.ProtoBufSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

  private org.apache.kafka.clients.producer.Producer<String, Order> orderProducer;
  private org.apache.kafka.clients.producer.Producer<String, User> userProducer;

  public Producer(String host) {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoBufSerializer.class);

    orderProducer = new KafkaProducer<>(config);
    userProducer = new KafkaProducer<>(config);
  }

  public void produceOrder(Order order) {
    ProducerRecord<String, Order> record = new ProducerRecord<>("orders", Long.toString(order.getOrderId()), order);
    orderProducer.send(record);
    orderProducer.flush();
  }

  public void produceUser(User user) {
    ProducerRecord<String, User> record = new ProducerRecord<>("users", Long.toString(user.getUserId()), user);
    userProducer.send(record);
    userProducer.flush();
  }

}
