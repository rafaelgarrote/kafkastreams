package com.paradigmadigital.kafkaweminar.kstreams.wordcount.model;

import com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas.KeySchema;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.AvroFormat;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.DifferentNamespaceFomExpected;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import twitter4j.Status;

public class Key implements AvroFormat<Key> {

    private GenericRecord avroRecord;
    private String lang;

    public Key(Status status) {
        this.lang = status.getLang();
        this.avroRecord = new GenericData.Record(KeySchema.schema);
        this.avroRecord.put("lang", lang);
    }

    public Key() {}

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    @Override
    public GenericRecord toAvro() {
        return this.avroRecord;
    }

    @Override
    public Key fromAvro(GenericRecord record) throws DifferentNamespaceFomExpected {
        String name = record.getSchema().getNamespace();
        String nameSpace = KeySchema.schema.getNamespace();

        if (name.equals(nameSpace)) {
            this.avroRecord = record;
            this.lang = record.get("lang").toString();
        } else {
            throw new DifferentNamespaceFomExpected("GenericRecord form different namespace: " + name + " expected " + nameSpace);
        }
        return this;
    }
}
