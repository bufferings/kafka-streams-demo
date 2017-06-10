package com.example.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KStreamBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StateStoreViewer {

  @Autowired
  private KStreamBuilderFactoryBean kStreamBuilder;

  @GetMapping
  public List<KeyValue<String, Long>> query() {
    final ReadOnlyKeyValueStore<String, Long> store = kStreamBuilder.getKafkaStreams().store("TweetTextWords",
        QueryableStoreTypes.keyValueStore());

    final List<KeyValue<String, Long>> results = new ArrayList<>();
    final KeyValueIterator<String, Long> range = store.all();
    while (range.hasNext()) {
      results.add(range.next());
    }

    // 強引に上位100件をとってみた
    Collections.sort(results, (r1, r2) -> {
      return r2.value.intValue() - r1.value.intValue();
    });
    return results.subList(0, Math.min(results.size(), 100));
  }

}
