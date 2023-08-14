package com.moagga.serde;

import com.moagga.proto.order.Order;
import com.moagga.proto.user.Address;
import com.moagga.proto.user.User;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SerdeTest {

  @Test
  public void testSerde() {
    ProtoBufSerializer serializer = new ProtoBufSerializer();
    ProtoBufDeserializer deserializer = new ProtoBufDeserializer();
    deserializer.configure(Collections.emptyMap(), false);

    Order order = Order.newBuilder()
        .setOrderId(22L)
        .setStatus("Pending Shipment")
        .build();

    Order orderCopy = (Order) deserializer.deserialize("orders",
        serializer.serialize("orders", order));
    Assertions.assertEquals(orderCopy, order);

    Address address = Address.newBuilder()
        .setCity("DEL")
        .setPinCode(11001)
        .build();

    User user = User.newBuilder()
        .setUserId(12L)
        .setName("Joe")
        .addAddresses(address)
        .build();

    User userCopy = (User) deserializer.deserialize("users",
        serializer.serialize("users", user));
    Assertions.assertEquals(userCopy, user);


  }

}
