package com.example.demo;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class KafkaAvroSerializerWithSchemaName extends AbstractKafkaAvroSerializer implements Serializer<Object> {

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaAvroSerializerWithSchemaName() {

  }

  public KafkaAvroSerializerWithSchemaName(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  public KafkaAvroSerializerWithSchemaName(SchemaRegistryClient client, Map<String, ?> props) {
    schemaRegistry = client;
    configure(serializerConfig(props));
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(new KafkaAvroSerializerConfig(configs));
  }

  @Override
  public byte[] serialize(String topic, Object record) {
    return serializeImpl(getSchema(record).getFullName(), record);
  }

  @Override
  public void close() {

  }
}