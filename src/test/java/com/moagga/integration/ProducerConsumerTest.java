package com.moagga.integration;

import com.moagga.consumer.ConsumerCallback;
import com.moagga.consumer.OrderConsumer;
import com.moagga.producer.OrderProducer;
import com.moagga.proto.order.Item;
import com.moagga.proto.order.Order;
import com.moagga.proto.order.PaymentMethod;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class ProducerConsumerTest implements ConsumerCallback {

  private KafkaContainer kafka;

  private OrderProducer producer;
  private OrderConsumer consumer;

  private Order orderProduced;
  private Order orderConsumed;

  private ExecutorService executorService;

  @BeforeEach
  void setup() throws InterruptedException {
    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
    kafka.start();
    String host = kafka.getBootstrapServers();

    producer = new OrderProducer(host);
    consumer = new OrderConsumer(host, this);
    consumer.start();

    executorService = Executors.newFixedThreadPool(1);
    executorService.submit(consumer);
    Thread.sleep(5000L);

    Random r = new Random();
    Order.Builder builder = Order.newBuilder()
        .setOrderId(r.nextLong())
        .setStatus(RandomStringUtils.randomAscii(5))
        .setPaymentStatus(RandomStringUtils.randomAscii(5));

    for (int i = 0; i < 3; i++) {
      Item item = Item.newBuilder()
          .setItemId(RandomStringUtils.randomAscii(10))
          .setPrice(r.nextDouble())
          .setBrand(RandomStringUtils.randomAscii(5))
          .setCategory(RandomStringUtils.randomAscii(9))
          .build();
      builder.addItems(item);
    }

    for (int i = 0; i < 2; i++) {
      PaymentMethod paymentMethod = PaymentMethod.newBuilder()
          .setMode(RandomStringUtils.randomAscii(3))
          .setAmount(r.nextDouble())
          .setBankCode(RandomStringUtils.randomAscii(3))
          .build();

      builder.addPaymentMethods(paymentMethod);
    }

    orderProduced = builder.build();
  }

  @AfterEach
  void tearDown() {
    consumer.stop();
    executorService.shutdown();
    kafka.stop();
  }

  @Test
  void teste2e() throws InterruptedException {
    producer.produceMessage(orderProduced);
    Thread.sleep(10000L);
    Assertions.assertEquals(orderConsumed, orderProduced);
  }

  @Override
  public void onMessage(ConsumerRecords<String, Order> records) {
    for (ConsumerRecord<String, Order> record : records) {
      orderConsumed = record.value();
      break;
    }
  }

}
