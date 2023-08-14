package com.moagga.integration;

import com.moagga.consumer.ConsumerCallback;
import com.moagga.consumer.OrderConsumer;
import com.moagga.producer.Producer;
import com.moagga.proto.order.Item;
import com.moagga.proto.order.Order;
import com.moagga.proto.user.Address;
import com.moagga.proto.user.User;
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

  private Producer producer;
  private OrderConsumer consumer;

  private Order orderProduced;
  private Order orderConsumed;

  private User userProduced;
  private User userConsumed;
  private ExecutorService executorService;

  @BeforeEach
  void setup() throws InterruptedException {
    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
    kafka.start();
    String host = kafka.getBootstrapServers();

    producer = new Producer(host);
    consumer = new OrderConsumer(host, this);
    consumer.start();

    executorService = Executors.newFixedThreadPool(1);
    executorService.submit(consumer);
    Thread.sleep(5000L);

    Random r = new Random();
    Order.Builder builder = Order.newBuilder()
        .setOrderId(r.nextLong())
        .setStatus(RandomStringUtils.randomAscii(5));

    for (int i = 0; i < 3; i++) {
      Item.Builder itemBuilder = Item.newBuilder()
          .setItemId(RandomStringUtils.randomAscii(10))
          .setPrice(r.nextDouble())
          .setBrand(RandomStringUtils.randomAscii(5));

      for (int j = 0; j < 2; j++) {
        itemBuilder.addCategories(RandomStringUtils.randomAscii(5));
      }

      builder.addItems(itemBuilder.build());
    }

    Address address = Address.newBuilder()
        .setCity("DEL")
        .setPinCode(11001)
        .build();

    userProduced = User.newBuilder()
        .setUserId(12L)
        .setName("Joe")
        .addAddresses(address)
        .build();

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
    producer.produceOrder(orderProduced);
    producer.produceUser(userProduced);
    Thread.sleep(10000L);
    Assertions.assertEquals(orderConsumed, orderProduced);
    Assertions.assertEquals(userConsumed, userProduced);
  }

  @Override
  public void onMessage(ConsumerRecords<String, ?> records) {
    for (ConsumerRecord<String, ?> record : records) {
      if (record.topic().equals("orders")) {
        orderConsumed = (Order) record.value();
      } else if (record.topic().equals("users")) {
        userConsumed = (User) record.value();
      }
    }
  }

}
