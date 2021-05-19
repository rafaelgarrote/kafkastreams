package com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas;

import org.apache.avro.Schema;

public class KeySchema {

    private static final Schema.Parser schemaParser = new Schema.Parser();
    private static final String nameSpace = "tweet.key.avro";
    private static final Schema keySchema = schemaParser.parse(
            "{\"namespace\": \"" + nameSpace + "\", " +
                    "\"type\":\"record\"," +
                    "\"name\":\"key\"," +
                    "\"fields\":[" +
                        "{\"name\":\"lang\", \"type\":\"string\"}" +
                    "]}");

    private KeySchema() {}

    public static Schema schema = keySchema;

}
