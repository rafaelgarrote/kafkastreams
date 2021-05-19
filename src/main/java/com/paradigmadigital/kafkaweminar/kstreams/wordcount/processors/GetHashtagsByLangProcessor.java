package com.paradigmadigital.kafkaweminar.kstreams.wordcount.processors;

import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.Key;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.Tweet;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.List;

public class GetHashtagsByLangProcessor implements Processor<Key, Tweet> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(Key key, Tweet tweet) {
        ((List<?>)tweet.getHashtags())
                .stream()
                .map(Object::toString)
                .forEach(hashtag -> context.forward(key.getLang(), hashtag.toLowerCase()));
        // commit the current processing progress
        context.commit();
    }

    @Override
    public void close() {

    }
}
