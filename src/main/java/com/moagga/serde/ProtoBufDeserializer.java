package com.moagga.serde;

import com.google.protobuf.Message;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Generic Kafka deserializer for protocol buffer.
 * This class uses a property file to load topic & message Java type which helps to reuse this deserializer
 * across multiple topics. The concept could be extended to load this mapping from other sources
 * like DB.
 *
 */
public class ProtoBufDeserializer implements Deserializer<Message> {

  private Map<String, Class<? extends Message>> targetClasses = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    InputStream in = ProtoBufDeserializer.class.getClassLoader().getResourceAsStream("deserializers.properties");
    Properties props = new Properties();
    try {
      props.load(in);
      for (Object prop : props.keySet()) {
        String className = props.getProperty(prop.toString());
        Class<? extends Message> clazz = (Class<? extends Message>) Class.forName(className);
        targetClasses.put(prop.toString(), clazz);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
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
