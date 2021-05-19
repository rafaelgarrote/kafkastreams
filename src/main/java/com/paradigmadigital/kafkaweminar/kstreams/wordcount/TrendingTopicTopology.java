package com.paradigmadigital.kafkaweminar.kstreams.wordcount;

import com.paradigmadigital.kafkaweminar.AppConfiguration;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.Key;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.TTKey;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.TTValue;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.model.Tweet;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.processors.CalculateTrendingTopicProcessorSupplier;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.processors.GetHashtagsByLangProcessorSupplier;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.CustomAvroSerde;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.transformers.TTTransformerSupplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TrendingTopicTopology extends AppConfiguration {

    static public void main(String[] args) {

        String sinkTopicName = "trending-topic-5";
        Duration ttWindowTime = Duration.ofSeconds(60);

        // Topology
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trending-topic-topology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");

        // Sredes
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

        // State Store
        String storeName = "tt-accumulator";
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeName);
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores
                .keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Long());

        //Create the processors
        GetHashtagsByLangProcessorSupplier getHashtags = new GetHashtagsByLangProcessorSupplier();
        CalculateTrendingTopicProcessorSupplier trendingTopic =
                new CalculateTrendingTopicProcessorSupplier(storeName, ttWindowTime);

        // Topology
        Topology topology = new Topology();
        topology = topology
                .addSource("tweets", keySerde.deserializer(), tweetSerde.deserializer(), getTwitterTopic()) // Source Node
                .addProcessor("hashtags", getHashtags, "tweets")
                .addProcessor("trendingTopic", trendingTopic, "hashtags")
                .addStateStore(storeBuilder, "trendingTopic") // Attach state store to the stream
                .addSink("trending-topic-5", sinkTopicName, ttKeySerde.serializer(), ttValueSerde.serializer(), "trendingTopic"); // Sink Node

        KafkaStreams kafkaStreams =
                new KafkaStreams(topology, props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        // Gracefully shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

}
