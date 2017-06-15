package com.example.demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import io.confluent.examples.streams.utils.GenericAvroSerde;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaClientConfig {

  private static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

  private final String schemaRegistryUrl;

  private final KafkaProperties properties;

  @Autowired
  public KafkaClientConfig(@Value("${schema.registry.url}") String schemaRegistryUrl, KafkaProperties properties) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.properties = properties;
  }

  @Bean
  public ProducerFactory<?, ?> kafkaProducerFactory() {
    Map<String, Object> producerProperties = properties.buildProducerProperties();
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializerWithSchemaName.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializerWithSchemaName.class);
    // Schema Registry
    producerProperties.put(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);
    return new DefaultKafkaProducerFactory<Object, Object>(producerProperties);
  }

  @Bean
  public ConsumerFactory<?, ?> kafkaConsumerFactory() {
    Map<String, Object> consumerProperties = properties.buildConsumerProperties();
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    // Schema Registry
    consumerProperties.put(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);
    return new DefaultKafkaConsumerFactory<Object, Object>(consumerProperties);
  }

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public StreamsConfig kStreamsConfigs() {
    Map<String, Object> streamProperties = new HashMap<>();
    streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
    // Spring BootのAutoConfigurationに入ってるやつ使えばいいかなと思って
    streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
    streamProperties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
    streamProperties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
    streamProperties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
    // 動作確認用に1秒ごとにコミット(コミット理解してないけど)
    streamProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
    // Schema Registry
    streamProperties.put(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);
    return new StreamsConfig(streamProperties);
  }

}
