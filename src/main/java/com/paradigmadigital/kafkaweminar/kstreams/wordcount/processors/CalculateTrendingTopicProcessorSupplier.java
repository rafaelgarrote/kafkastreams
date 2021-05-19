package com.paradigmadigital.kafkaweminar.kstreams.wordcount.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.time.Duration;
import java.util.Set;

public class CalculateTrendingTopicProcessorSupplier implements ProcessorSupplier<String, String> {

    private final CalculateTrendingTopicProcessor processor;

    public CalculateTrendingTopicProcessorSupplier(String storeName, Duration interval) {
        this.processor = new CalculateTrendingTopicProcessor(storeName, interval);
    }

    @Override
    public Processor<String, String> get() {
        return processor;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return ProcessorSupplier.super.stores();
    }
}
