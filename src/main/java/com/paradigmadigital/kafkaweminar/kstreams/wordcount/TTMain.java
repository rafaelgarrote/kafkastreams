package com.paradigmadigital.kafkaweminar.kstreams.wordcount;

import com.paradigmadigital.kafkaweminar.AppConfiguration;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

public class TTMain extends AppConfiguration {

    static public void main(String[] args) {

        GenericAvroSerde avroSerde = new GenericAvroSerde();
        HashMap<String, String> serdeConf = new HashMap<>();
        serdeConf.put("schema.registry.url", "http://127.0.0.1:8085");
        avroSerde.configure(serdeConf, true);
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Source Node
        KStream<GenericRecord, GenericRecord> twitterKStream =
                streamsBuilder.stream(getTwitterTopic(), Consumed.with(avroSerde, avroSerde));

        // Processors nodes
        KTable<String, Long> processors = twitterKStream
                .map((key, value) -> KeyValue.pair(key.get("lang").toString(), (List<String>) value.get("hashtags")))
                .filterNot(((key, value) -> value.isEmpty()))
                .flatMapValues(value -> ((List<?>)value).stream().map(Object::toString).collect(Collectors.toList()))
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String()))
                .count();
                //.count(Materialized.as(Stores.inMemoryKeyValueStore("TT-counts")));

        processors.toStream().print(Printed.<String, Long>toSysOut().withLabel("KTable"));

        // Sink Node
        processors.toStream().to("trending-topic-2", Produced.with(Serdes.String(), Serdes.Long()));

        // Topology
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trending-topic-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5 * 1000);

        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams =
                new KafkaStreams(topology, props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        // Gracefully shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        /*
          StoreBuilder countStoreBuilder =
          Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("Counts"),
            Serdes.String(),
            Serdes.Long())
          .withCachingEnabled()
         */

        /*
        private static <T> GenericRecord pojoToRecord(T model) throws IOException {
        Schema schema = ReflectData.get().getSchema(model.getClass());

        ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        datumWriter.write(model, encoder);
        encoder.flush();

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);

        return datumReader.read(null, decoder);
    }

    private static <T> T mapRecordToObject(GenericRecord record, T object) {
        Assert.notNull(record, "record must not be null");
        Assert.notNull(object, "object must not be null");
        final Schema schema = ReflectData.get().getSchema(object.getClass());

        Assert.isTrue(schema.getFields().equals(record.getSchema().getFields()), "Schema fields didn’t match");
        record.getSchema().getFields().forEach(d -> PropertyAccessorFactory.forDirectFieldAccess(object).setPropertyValue(d.name(), record.get(d.name()) == null ? record.get(d.name()) : record.get(d.name()).toString()));
        return object;
    }
         */
    }

}
