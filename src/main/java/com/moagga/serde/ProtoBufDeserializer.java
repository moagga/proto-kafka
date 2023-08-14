package com.moagga.serde;

import com.google.protobuf.Message;
import com.moagga.serde.config.PropertiesSerdeProvider;
import com.moagga.serde.config.SerdeProvider;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Generic Kafka deserializer for protocol buffer.
 *
 */
public class ProtoBufDeserializer implements Deserializer<Message> {

  private Map<String, Class<? extends Message>> targetClasses = new HashMap<>();
  private SerdeProvider serdeProvider;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serdeProvider = new PropertiesSerdeProvider();
  }

  @Override
  public Message deserialize(String topic, byte[] data) {
    try {
      Class<? extends Message> targetClazz = serdeProvider.getTargetForTopic(topic);
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
