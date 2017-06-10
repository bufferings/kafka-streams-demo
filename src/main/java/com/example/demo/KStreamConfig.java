package com.example.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import com.atilika.kuromoji.ipadic.Tokenizer;

@Configuration
@EnableKafkaStreams
public class KStreamConfig {

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public StreamsConfig kStreamsConfigs(KafkaProperties kafkaProperties) {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
    // Spring BootのAutoConfigurationに入ってるやつ使えばいいかなと思って
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
    // 動作確認用に1秒ごとにコミット(コミット理解してないけど)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
    return new StreamsConfig(props);
  }

  @Bean
  public KStream<String, String> kStream(KStreamBuilder kStreamBuilder) {
    KStream<String, String> stream = kStreamBuilder.stream("TweetTexts");
    // @formatter:off
    stream
      .flatMapValues(value -> tokenize(value))
      .groupBy((key, word) -> word)
      .count("TweetTextWords");
    // @formatter:on
    return stream;
  }

  private static final Tokenizer tokenizer = new Tokenizer();

  private List<String> tokenize(String value) {
    // @formatter:off
    return tokenizer.tokenize(value).stream()
      // 辞書にあって名詞で2文字以上のにしてみた
      .filter(token -> token.isKnown() && token.getPartOfSpeechLevel1().equals("名詞") && token.getBaseForm().length() >= 2)
      .map(token -> token.getBaseForm())
      .collect(Collectors.toList());
    // @formatter:on
  }
}
