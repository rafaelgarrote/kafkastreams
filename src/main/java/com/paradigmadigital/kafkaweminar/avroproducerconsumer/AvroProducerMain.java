package com.paradigmadigital.kafkaweminar.avroproducerconsumer;

import com.paradigmadigital.kafkaweminar.AppConfiguration;
import com.paradigmadigital.kafkaweminar.kafkaclient.Producer;
import com.paradigmadigital.kafkaweminar.twitterclient.LangListener;
import com.paradigmadigital.kafkaweminar.twitterclient.StreamingClient;
import org.apache.avro.generic.GenericRecord;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

public class AvroProducerMain extends AppConfiguration {

    static public void main(String[] args) {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        Configuration twitterClientConf = configurationBuilder.setOAuthConsumerKey(getTwitterConsumerKey())
                .setOAuthConsumerSecret(getTwitterConsumerSecret())
                .setOAuthAccessToken(getTwitterAccessToken())
                .setOAuthAccessTokenSecret(getTwitterAccessTokenSecret())
                .setJSONStoreEnabled(true).build();

        Properties kafkaProperties = getKafkaProducerProperties();

        Producer<GenericRecord, GenericRecord> kafkaProducer = new Producer<>(kafkaProperties, getTwitterTopic());
        LangListener listener = new LangListener(kafkaProducer);
        StreamingClient twitterStream = StreamingClient.getInstance(twitterClientConf, listener);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                twitterStream.close();
                kafkaProducer.close();
            }
        });
    }
}
