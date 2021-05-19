package com.paradigmadigital.kafkaweminar.kstreams.wordcount.model;

import com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas.KeySchema;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas.TweetSchema;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.AvroFormat;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.DifferentNamespaceFomExpected;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import twitter4j.UserMentionEntity;

public class UserMention implements AvroFormat<UserMention> {

    private GenericRecord avroRecord;
    private Long id;
    private String name;
    private String screenName;

    public UserMention(){}

    public UserMention(UserMentionEntity mention) {
        GenericRecord record = new GenericData.Record(TweetSchema.mentionSchema);
        record.put("screenName", mention.getScreenName());
        record.put("name", mention.getName());
        record.put("id", mention.getId());
        this.avroRecord = record;
    }

    @Override
    public GenericRecord toAvro() {
        return this.avroRecord;
    }

    @Override
    public UserMention fromAvro(GenericRecord record) throws DifferentNamespaceFomExpected {
        String name = record.getSchema().getNamespace();
        String nameSpace = TweetSchema.mentionSchema.getNamespace();

        if (name.equals(nameSpace)) {
            this.avroRecord = record;
            this.id = (Long) record.get("id");
            this.name = record.get("name").toString();
            this.screenName = record.get("screenName").toString();
        } else {
            throw new DifferentNamespaceFomExpected("GenericRecord form different namespace: " + name + " expected " + nameSpace);
        }
        return this;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }
}
