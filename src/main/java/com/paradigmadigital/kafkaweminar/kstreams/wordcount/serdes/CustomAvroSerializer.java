package com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomAvroSerializer<T extends AvroFormat<T>> implements Serializer<T> {

    private KafkaAvroSerializer inner;

    public CustomAvroSerializer() {
        this.inner = new KafkaAvroSerializer();
    }

    public CustomAvroSerializer(SchemaRegistryClient client) {
        this.inner = new KafkaAvroSerializer(client);
    }

    public CustomAvroSerializer(KafkaAvroSerializer inner) {
        this.inner = inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.inner.configure(configs, isKey);
    }


    @Override
    public byte[] serialize(String s, T t) {
        return inner.serialize(s, t.toAvro());
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return inner.serialize(topic, data.toAvro());
    }

    @Override
    public void close() {
        inner.close();
    }
}
