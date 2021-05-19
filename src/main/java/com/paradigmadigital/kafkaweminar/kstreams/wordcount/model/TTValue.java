package com.paradigmadigital.kafkaweminar.kstreams.wordcount.model;

import com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas.TTValueSchema;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.AvroFormat;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.DifferentNamespaceFomExpected;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class TTValue implements AvroFormat<TTValue> {

    private GenericRecord avroRecord;
    private Long count;

    public TTValue(Long count) {
        this.count = count;
        this.avroRecord = new GenericData.Record(TTValueSchema.schema);
        this.avroRecord.put("count", count);
    }

    public TTValue() {}

    @Override
    public GenericRecord toAvro() {
        return this.avroRecord;
    }

    @Override
    public TTValue fromAvro(GenericRecord record) throws DifferentNamespaceFomExpected {
        String name = record.getSchema().getNamespace();
        String nameSpace = TTValueSchema.schema.getNamespace();

        if (name.equals(nameSpace)) {
            this.avroRecord = record;
            this.count = (Long) record.get("count");
        } else {
            throw new DifferentNamespaceFomExpected("GenericRecord form different namespace: " + name + " expected " + nameSpace);
        }
        return this;
    }
}
