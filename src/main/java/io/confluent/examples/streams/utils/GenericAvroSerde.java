
package io.confluent.examples.streams.utils;

import java.util.Collections;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.example.demo.GenericAvroSerializerWithSchemaName;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class GenericAvroSerde implements Serde<GenericRecord> {

  private final Serde<GenericRecord> inner;

  /**
   * Constructor used by Kafka Streams.
   */
  public GenericAvroSerde() {
    inner = Serdes.serdeFrom(new GenericAvroSerializerWithSchemaName(), new GenericAvroDeserializer());
  }

  public GenericAvroSerde(SchemaRegistryClient client) {
    this(client, Collections.emptyMap());
  }

  public GenericAvroSerde(SchemaRegistryClient client, Map<String, ?> props) {
    inner = Serdes.serdeFrom(new GenericAvroSerializerWithSchemaName(client),
        new GenericAvroDeserializer(client, props));
  }

  @Override
  public Serializer<GenericRecord> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<GenericRecord> deserializer() {
    return inner.deserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.serializer().configure(configs, isKey);
    inner.deserializer().configure(configs, isKey);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

}