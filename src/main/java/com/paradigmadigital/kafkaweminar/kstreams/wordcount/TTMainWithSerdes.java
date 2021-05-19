package com.paradigmadigital.kafkaweminar.kstreams.wordcount;

import com.paradigmadigital.kafkaweminar.AppConfiguration;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.Key;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.TTKey;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.TTValue;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.Tweet;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.CustomAvroSerde;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TTMainWithSerdes extends AppConfiguration {

    static public void main(String[] args) {

        HashMap<String,String> serdeConf = new HashMap<>();
        serdeConf.put("schema.registry.url", "http://127.0.0.1:8085");

        CustomAvroSerde<Key> keySerde = new CustomAvroSerde<>(Key.class);
        keySerde.configure(serdeConf, true);

        CustomAvroSerde<Tweet> tweetSerde = new CustomAvroSerde<>(Tweet.class);
        tweetSerde.configure(serdeConf, false);

        CustomAvroSerde<TTKey> ttKeySerde = new CustomAvroSerde<>(TTKey.class);
        ttKeySerde.configure(serdeConf, true);

        CustomAvroSerde<TTValue> ttValueSerde = new CustomAvroSerde<>(TTValue.class);
        ttValueSerde.configure(serdeConf, false);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Source Node
        KStream<Key,Tweet> twitterKStream =
                streamsBuilder.stream(getTwitterTopic(), Consumed.with(keySerde, tweetSerde));

        // Processors nodes
        KTable<String, Long> processors = twitterKStream
                .map((key, value) -> KeyValue.pair(key.getLang(), value.getHashtags()))
                .filterNot(((key, value) -> value.isEmpty()))
                .flatMapValues(value -> ((List<?>)value).stream().map(Object::toString).collect(Collectors.toList()))
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String())) // Processor Node => aggregate by key
                .count();
        //.count(Materialized.as(Stores.inMemoryKeyValueStore("TT-counts")));

        processors.toStream().print(Printed.<String, Long>toSysOut().withLabel("KTable"));

        // Sink Node
        processors
                .toStream()
                .map((key, value) -> KeyValue.pair(new TTKey(key), new TTValue(value)))
                .to("trending-topic-4", Produced.with(ttKeySerde, ttValueSerde));

        // Topology
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trending-topic-serdes");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1 * 1000);

        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams =
                new KafkaStreams(topology, props);
        //kafkaStreams.cleanUp();
        kafkaStreams.start();

        // Gracefully shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

}
