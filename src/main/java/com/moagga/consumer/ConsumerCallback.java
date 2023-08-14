package com.moagga.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ConsumerCallback {

  void onMessage(ConsumerRecords<String, ?> records);

}
