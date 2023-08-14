This project explains an approach to use Protocol Buffers as a binary message protocol with Kafka.
Using proto-buffers eliminates the need of schema registry. Schema registry for Avro format requires confluent license.

A generic ProtoBufDeserializer class is used for 2 different topics with different message objects.
This deserializer uses a property file to keep topic & associated message type.
The same configuration can be moved to some other external configuration as well such as
Database, ZooKeeper, etc.