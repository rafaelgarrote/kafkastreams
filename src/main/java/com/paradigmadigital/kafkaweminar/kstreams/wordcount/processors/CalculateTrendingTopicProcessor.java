package com.paradigmadigital.kafkaweminar.kstreams.wordcount.processors;

import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.Key;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.TTKey;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.TTValue;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Optional;

public class CalculateTrendingTopicProcessor implements Processor <String, String> {

    private ProcessorContext context;
    private final Duration interval;
    private final String storeName;
    private KeyValueStore<String, Long> stateStore;

    public CalculateTrendingTopicProcessor(String storeName, Duration interval) {
        this.storeName = storeName;
        this.interval = interval;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.stateStore = (KeyValueStore<String, Long>) processorContext.getStateStore(storeName);
        this.context.schedule(interval, PunctuationType.STREAM_TIME, (timestamp) -> {
            KeyValueIterator<String, Long> it = stateStore.all();
            while (it.hasNext()) {
                KeyValue<String, Long> entry = it.next();
                System.out.println(entry.key + " " + entry.value);
                context.forward(new TTKey(entry.key), new TTValue(entry.value));
            }
            it.close();
            // commit the current processing progress
            context.commit();
        });
    }

    @Override
    public void process(String key, String hashtag) {
        Long newValue = Optional.ofNullable(stateStore.get(hashtag)).map(oldValue -> oldValue + 1).orElse(1L);
        stateStore.put(hashtag, newValue);
    }

    @Override
    public void close() {

    }
}
