package com.example.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KStreamBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TweetWordCountsViewer {

  @Autowired
  private KStreamBuilderFactoryBean kStreamBuilder;

  @GetMapping
  public List<KeyValue<String, Long>> query() {
    final ReadOnlyKeyValueStore<String, Long> store = kStreamBuilder.getKafkaStreams().store("TweetWordCounts",
        QueryableStoreTypes.keyValueStore());

    final List<KeyValue<String, Long>> results = new ArrayList<>();
    final KeyValueIterator<String, Long> range = store.all();
    while (range.hasNext()) {
      results.add(range.next());
    }

    // 強引に上位10件をとってみた
    Collections.sort(results, (r1, r2) -> {
      return r2.value.intValue() - r1.value.intValue();
    });
    return results.subList(0, Math.min(results.size(), 10));
  }

  @GetMapping("top5")
  public String top5() {
    final ReadOnlyKeyValueStore<String, GenericRecord> store = kStreamBuilder.getKafkaStreams().store("TweetWordTop5",
        QueryableStoreTypes.keyValueStore());
    return store.get("top5").toString();
  }

}
