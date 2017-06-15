package com.example.demo;

import java.io.InputStream;
import java.time.ZoneId;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
public class TwitterToKafka implements CommandLineRunner {

  private static final Logger logger = LoggerFactory.getLogger(TwitterToKafka.class);

  @Value(value = "classpath:avro/tweet.avsc")
  private Resource tweetSchema;

  // 混ぜても使えるのかの確認
  @Value(value = "classpath:avro/different-schema.avsc")
  private Resource differentSchema;

  @Autowired
  private KafkaTemplate<String, GenericRecord> template;

  @Override
  public void run(String... args) throws Exception {
    Schema schema;
    try (InputStream is = tweetSchema.getInputStream()) {
      schema = new Schema.Parser().parse(is);
    }

    Schema schema2;
    try (InputStream is2 = differentSchema.getInputStream()) {
      schema2 = new Schema.Parser().parse(is2);
    }

    TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.addListener(new StatusAdapter() {
      @Override
      public void onStatus(Status status) {
        try {
          GenericRecord tweet;
          if (status.getId() % 2 == 0) {
            tweet = new GenericData.Record(schema);
            tweet.put("id", status.getId());
            tweet.put("text", status.getText());
          } else {
            tweet = new GenericData.Record(schema2);
            tweet.put("id2", status.getId());
            tweet.put("createdAt", status.getCreatedAt().toInstant().atZone(ZoneId.systemDefault()).toString());
            tweet.put("text2", status.getText());
          }

          template.send("Tweets", tweet);
        } catch (RuntimeException e) {
          logger.error("Fail to send.", e);
          throw e;
        }
      }
    });

    twitterStream.sample("ja");
  }

  @KafkaListener(id = "foo", topics = "Tweets")
  public void listen(GenericRecord record) throws Exception {
    logger.info("Tweet received. " + record);
  }

}
