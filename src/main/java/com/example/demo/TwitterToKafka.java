package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
public class TwitterToKafka implements CommandLineRunner {

  @Autowired
  private KafkaTemplate<String, String> template;

  @Override
  public void run(String... args) throws Exception {
    TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.addListener(new StatusAdapter() {
      @Override
      public void onStatus(Status status) {
        template.send("TweetTexts", status.getText());
      }
    });
    twitterStream.sample("ja");
  }

}
