package com.paradigmadigital.kafkaweminar.kstreams.wordcount.processors;

import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.Key;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.Tweet;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

public class GetHashtagsByLangProcessorSupplier implements ProcessorSupplier<Key, Tweet> {

    private final GetHashtagsByLangProcessor processor;

    public GetHashtagsByLangProcessorSupplier() {
        this.processor = new GetHashtagsByLangProcessor();
    }

    @Override
    public Processor<Key, Tweet> get() {
        return this.processor;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return null;
    }
}
