package com.shahriar.springkafka.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;

@RestController
@RequiredArgsConstructor
class RestService {

  private final StreamsBuilderFactoryBean factoryBean = new StreamsBuilderFactoryBean();

  @GetMapping("/count/{word}")
  public Long getCount(@PathVariable String word) {
    final KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

    final ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
    return counts.get(word);
  }

}
