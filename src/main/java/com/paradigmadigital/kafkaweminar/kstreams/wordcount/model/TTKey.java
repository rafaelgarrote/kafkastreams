package com.paradigmadigital.kafkaweminar.kstreams.wordcount.model;

import com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas.TTKeySchema;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.AvroFormat;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.DifferentNamespaceFomExpected;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class TTKey implements AvroFormat<TTKey> {

    private GenericRecord avroRecord;
    private String hashtag;

    public TTKey(String hashtag) {
        this.hashtag = hashtag;
        this.avroRecord = new GenericData.Record(TTKeySchema.schema);
        this.avroRecord.put("hashtag", hashtag);
    }

    public TTKey() {}

    @Override
    public GenericRecord toAvro() {
        return this.avroRecord;
    }

    @Override
    public TTKey fromAvro(GenericRecord record) throws DifferentNamespaceFomExpected {
        String name = record.getSchema().getNamespace();
        String nameSpace = TTKeySchema.schema.getNamespace();

        if (name.equals(nameSpace)) {
            this.avroRecord = record;
            this.hashtag = record.get("hashtag").toString();
        } else {
            throw new DifferentNamespaceFomExpected("GenericRecord form different namespace: " + name + " expected " + nameSpace);
        }
        return this;
    }

    public String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag;
    }
}
