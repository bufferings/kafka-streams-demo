/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.demo;

import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class GenericAvroSerializerWithSchemaName implements Serializer<GenericRecord> {

  KafkaAvroSerializerWithSchemaName inner;

  /**
   * Constructor used by Kafka Streams.
   */
  public GenericAvroSerializerWithSchemaName() {
    inner = new KafkaAvroSerializerWithSchemaName();
  }

  public GenericAvroSerializerWithSchemaName(SchemaRegistryClient client) {
    inner = new KafkaAvroSerializerWithSchemaName(client);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, GenericRecord record) {
    return inner.serialize(topic, record);
  }

  @Override
  public void close() {
    inner.close();
  }
}