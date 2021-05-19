package com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas;

import org.apache.avro.Schema;

public class TTKeySchema {

    private static final Schema.Parser schemaParser = new Schema.Parser();
    private static final String nameSpace = "tt.key.avro";
    private static final Schema keySchema = schemaParser.parse(
            "{\"namespace\": \"" + nameSpace + "\", " +
                    "\"type\":\"record\"," +
                    "\"name\":\"tt_key\"," +
                    "\"fields\":[" +
                        "{\"name\":\"hashtag\", \"type\":\"string\"}" +
                    "]}");

    private TTKeySchema() {}

    public static Schema schema = keySchema;

}
