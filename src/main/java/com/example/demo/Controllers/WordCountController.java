package com.example.demo.Controllers;

import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WordCountController {

    @Autowired
    private KafkaStreamsInteractiveQueryService interactiveQueryService;

    @GetMapping("/count/{word}")
    public Long getWordCount(@PathVariable String word) {
        ReadOnlyKeyValueStore<String, Long> counts =
            interactiveQueryService.retrieveQueryableStore(
                "counts", QueryableStoreTypes.keyValueStore()
            );
        return counts.get(word);
    }
}
