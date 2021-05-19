package com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas;

import org.apache.avro.Schema;

public class TTValueSchema {

    private static final Schema.Parser schemaParser = new Schema.Parser();
    private static final String nameSpace = "tt.value.avro";
    private static final Schema valueSchema = schemaParser.parse(
            "{\"namespace\": \"" + nameSpace + "\", " +
                    "\"type\":\"record\"," +
                    "\"name\":\"tt_value\"," +
                    "\"fields\":[" +
                        "{\"name\":\"count\", \"type\":\"long\"}" +
                    "]}");

    private TTValueSchema() {}

    public static Schema schema = valueSchema;
}
