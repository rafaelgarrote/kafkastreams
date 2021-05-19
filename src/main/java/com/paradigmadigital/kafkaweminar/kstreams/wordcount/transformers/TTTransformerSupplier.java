package com.paradigmadigital.kafkaweminar.kstreams.wordcount.transformers;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Windowed;

public class TTTransformerSupplier implements ValueTransformerWithKeySupplier<Windowed<String>, Long, Long > {

    private final TTTransformer transformer;

    public TTTransformerSupplier(String storeName) {
        transformer = new TTTransformer(storeName);
    }

    @Override
    public ValueTransformerWithKey<Windowed<String>, Long, Long> get() {
        return transformer;
    }
}