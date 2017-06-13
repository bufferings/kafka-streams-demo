package com.example.demo;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.atilika.kuromoji.ipadic.Tokenizer;

@Component
public class TweetWordCounter {

  private static final Logger logger = LoggerFactory.getLogger(TweetWordCounter.class);

  private static final Tokenizer tokenizer = new Tokenizer();

  @Bean
  public KStream<String, GenericRecord> kStream(KStreamBuilder kStreamBuilder) {
    KStream<String, GenericRecord> stream = kStreamBuilder.stream("Tweets");
    // @formatter:off
    stream
      .map((key, tweet) -> {
        String text = ((Utf8)tweet.get("text")).toString();
        logger.info("Map to string. " + text);
        return new KeyValue<String, String>(key, ((Utf8)tweet.get("text")).toString());
      })
      .flatMapValues(value -> {
        List<String> tokens = tokenize(value);
        logger.info("Flatmap to tokens. " + tokens);
        return tokens;
      })
      .groupBy((key, word) -> word, Serdes.String(), Serdes.String())
      .count("TweetWordCounts");
    // @formatter:on
    return stream;
  }

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
