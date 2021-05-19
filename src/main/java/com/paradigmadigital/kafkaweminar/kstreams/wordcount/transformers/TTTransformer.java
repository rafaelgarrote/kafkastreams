package com.paradigmadigital.kafkaweminar.kstreams.wordcount.transformers;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;

public class TTTransformer implements ValueTransformerWithKey<Windowed<String>, Long, Long > {

    private final String storeName;
    private KeyValueStore<String, Long> stateStore;

    public TTTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        stateStore = (KeyValueStore<String, Long>) processorContext.getStateStore(storeName);
    }

    @Override
    public Long transform(Windowed<String> key, Long value) {
        System.out.println("---New Value: " + key.key() + " value: " + value);
        Long newValue = Optional.ofNullable(stateStore.get(key.key())).map(oldValue -> oldValue + value).orElse(value);
        stateStore.put(key.key(), newValue);
        System.out.println("---Actual Value: " + key.key() + " value: " + newValue);
        return newValue;
    }


    @Override
    public void close() { }
}
