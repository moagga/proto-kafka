package com.moagga.serde.config;

import com.google.protobuf.Message;
import com.moagga.serde.ProtoBufDeserializer;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *  This class uses a property file to load topic & message Java type.
 */
public class PropertiesSerdeProvider implements SerdeProvider {

  private Map<String, Class<? extends Message>> targetClasses;

  public PropertiesSerdeProvider() {
    targetClasses = new HashMap<>();

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
  public Class<? extends Message> getTargetForTopic(String topic) {
    if (!targetClasses.containsKey(topic)) {
      throw new IllegalArgumentException("No type for topic " + topic);
    }

    return targetClasses.get(topic);
  }
}
