package com.paradigmadigital.kafkaweminar.avroproducerconsumer;

import com.paradigmadigital.kafkaweminar.kafkaclient.MessageProcessor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class AvroMessageProcessor<String> implements MessageProcessor<GenericRecord,GenericRecord> {

    @Override
    public void process(ConsumerRecords<GenericRecord, GenericRecord> records) {
        for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
            System.out.println("Topic: " + record.topic());
            System.out.println("Partition: " + record.partition());
            System.out.println("Offset: " + record.offset());
            System.out.println("Key: " + record.key().toString());
            System.out.println("Value: " + record.value().toString());
        }
    }
}
