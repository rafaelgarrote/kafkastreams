package com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CustomAvroDeserializer<T extends AvroFormat<T>> implements Deserializer<T> {

    private KafkaAvroDeserializer inner;
    private Class<T> clazz;

    public CustomAvroDeserializer(Class<T> clazz) {
        this.clazz = clazz;
        this.inner = new KafkaAvroDeserializer();
    }

    public CustomAvroDeserializer(SchemaRegistryClient client) {
        this.inner = new KafkaAvroDeserializer(client);
    }

    public CustomAvroDeserializer(KafkaAvroDeserializer inner) {
        this.inner = inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            return this.clazz.newInstance().fromAvro((GenericRecord) inner.deserialize(s,bytes));
        } catch (InstantiationException | IllegalAccessException | DifferentNamespaceFomExpected e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        try {
            return this.clazz.newInstance().fromAvro((GenericRecord) inner.deserialize(topic,data));
        } catch (InstantiationException | IllegalAccessException | DifferentNamespaceFomExpected e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() {
        inner.close();
    }
}
