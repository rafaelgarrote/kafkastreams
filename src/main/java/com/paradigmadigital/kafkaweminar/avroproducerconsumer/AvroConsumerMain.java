package com.paradigmadigital.kafkaweminar.avroproducerconsumer;

import com.paradigmadigital.kafkaweminar.AppConfiguration;
import com.paradigmadigital.kafkaweminar.kafkaclient.Consumer;
import org.apache.avro.generic.GenericRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AvroConsumerMain extends AppConfiguration {

    static public void main(String[] args) {
        Properties kafkaProperties = getKafkaConsumerProperties();
        List<String> topics = new ArrayList<>();
        topics.add(getTwitterTopic());
        Duration d = Duration.ofMillis(getPollInterval());
        Consumer<GenericRecord, GenericRecord> consumer = new Consumer<>(kafkaProperties, topics);
        consumer.subscribeAndConsume(d, new AvroMessageProcessor<>());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                consumer.close();
            }
        });
    }
}
