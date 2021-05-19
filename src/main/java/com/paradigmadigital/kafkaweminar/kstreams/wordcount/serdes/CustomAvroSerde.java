package com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomAvroSerde<T extends AvroFormat<T>> implements Serde<T> {

    private Serde<T> inner;

    public CustomAvroSerde(Class<T> clazz) {
        this.inner = Serdes.serdeFrom(new CustomAvroSerializer<T>(), new CustomAvroDeserializer<T>(clazz));
    }

    public CustomAvroSerde(SchemaRegistryClient client) {
        this.inner = Serdes.serdeFrom(new CustomAvroSerializer<T>(client), new CustomAvroDeserializer<T>(client));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }

    @Override
    public Serializer<T> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return inner.deserializer();
    }
}
