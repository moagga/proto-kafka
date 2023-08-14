package com.moagga.serde;

import com.moagga.proto.order.Order;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SerdeTest {

  @Test
  public void testSerde() {
    ProtoBufSerializer serializer = new ProtoBufSerializer();
    ProtoBufDeserializer deserializer = new ProtoBufDeserializer();
    deserializer.configure(Collections.singletonMap(ProtoBufDeserializer.TARGET_CLASSES, Order.class), false);

    Order order = Order.newBuilder()
        .setOrderId(22L)
        .setStatus("Pending Shipment")
        .setPaymentStatus("Paid")
        .build();
    byte[] data = serializer.serialize("orders", order);

    Order copy = (Order) deserializer.deserialize("orders", data);
    Assertions.assertEquals(copy, order);
  }

}
