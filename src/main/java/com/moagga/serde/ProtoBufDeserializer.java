package com.moagga.serde;

import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public class ProtoBufDeserializer implements Deserializer<Message> {

  public static final String TARGET_CLASSES = "target.classes";

  private Map<String, Class<? extends Message>> targetClasses;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (!isKey) {
      targetClasses = (Map<String, Class<? extends Message>>) configs.get(TARGET_CLASSES);
      System.out.println(targetClasses);
    }
  }

  @Override
  public Message deserialize(String topic, byte[] data) {
    if (!targetClasses.containsKey(topic)) {
        throw new IllegalArgumentException("No target clazz found for topic " + topic);
    }

    try {
      Class<? extends Message> targetClazz = targetClasses.get(topic);
      Method m = targetClazz.getMethod("parseFrom", byte[].class);
      Message msg = (Message) m.invoke(null, data);
      return msg;
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
