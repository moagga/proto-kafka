package com.moagga.serde.config;

import com.google.protobuf.Message;

public interface SerdeProvider {

  Class<? extends Message> getTargetForTopic(String topic);

}
