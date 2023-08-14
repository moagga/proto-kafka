package com.moagga.serde;

import com.google.protobuf.Message;
import com.moagga.proto.order.Order;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializer to convert Java protobuf types to byte array.
 * Takes a generic {@link Message} type.
 *
 */
public class ProtoBufSerializer implements Serializer<Message> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, Message data) {

    return data.toByteArray();
  }


}
