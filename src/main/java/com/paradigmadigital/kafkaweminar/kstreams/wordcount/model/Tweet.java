package com.paradigmadigital.kafkaweminar.kstreams.wordcount.model;

import com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas.KeySchema;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas.TweetSchema;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.AvroFormat;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.DifferentNamespaceFomExpected;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Tweet implements AvroFormat<Tweet> {

    private GenericRecord avroRecord;
    private Long id;
    private String text;
    private String lang;
    private long createdAt;
    private long inReplyToStatusId;
    private long inReplyToUserId;
    private int retweetCount;
    private int favoriteCount;
    private List<String> hashtags;
    private boolean favorited;
    private boolean retweeted;
    private User user;
    private List<UserMention> mentions;

    public Tweet(){}

    public Tweet(Status status) {
        this.hashtags = Arrays.stream(status.getHashtagEntities())
                .map(HashtagEntity::getText)
                .collect(Collectors.toList());
        this.user = new User(status);
        this.mentions = Arrays.stream(status.getUserMentionEntities())
                .map(UserMention::new)
                .collect(Collectors.toList());
        GenericRecord user = this.user.toAvro();
        GenericRecord record = new GenericData.Record(TweetSchema.tweetSchema);
        record.put("id", status.getId());
        record.put("text", status.getText());
        record.put("lang", status.getLang());
        record.put("createdAt", status.getCreatedAt().getTime());
        record.put("inReplyToStatusId", status.getInReplyToStatusId());
        record.put("inReplyToUserId", status.getInReplyToUserId());
        record.put("inReplyToScreenName", status.getInReplyToScreenName());
        record.put("retweetCount", status.getRetweetCount());
        record.put("favoriteCount", status.getFavoriteCount());
        record.put("hashtags", this.hashtags);
        record.put("favorited", status.isFavorited());
        record.put("retweeted", status.isRetweeted());
        record.put("user", user);
        record.put("mentions", this.mentions.stream().map(UserMention::toAvro).collect(Collectors.toList()));
        this.avroRecord = record;
    }

    @Override
    public GenericRecord toAvro() {
        return this.avroRecord;
    }

    @Override
    public Tweet fromAvro(GenericRecord record) throws DifferentNamespaceFomExpected {
        String name = record.getSchema().getNamespace();
        String nameSpace = TweetSchema.tweetSchema.getNamespace();

        if (name.equals(nameSpace)) {
            this.avroRecord = record;
            this.id = (Long) record.get("id");
            this.text = record.get("text").toString();
            this.lang = record.get("lang").toString();
            this.createdAt = (Long) record.get("createdAt");
            this.inReplyToStatusId = (Long) record.get("inReplyToStatusId");
            this.inReplyToUserId = (Long) record.get("inReplyToUserId");
            this.retweetCount = (Integer) record.get("retweetCount");
            this.favoriteCount = (Integer) record.get("favoriteCount");
            this.hashtags = (List<String>) record.get("hashtags");
            this.favorited = (boolean) record.get("favorited");
            this.retweeted = (boolean) record.get("retweeted");
            this.user = new User().fromAvro((GenericRecord) record.get("user"));
            this.mentions = ((List<GenericRecord>) record.get("mentions")).stream().map(mention -> {
                try {
                    return new UserMention().fromAvro(mention);
                } catch (DifferentNamespaceFomExpected differentNamespaceFomExpected) {
                    differentNamespaceFomExpected.printStackTrace();
                    return null;
                }
            }).collect(Collectors.toList());
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

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getInReplyToStatusId() {
        return inReplyToStatusId;
    }

    public void setInReplyToStatusId(long inReplyToStatusId) {
        this.inReplyToStatusId = inReplyToStatusId;
    }

    public long getInReplyToUserId() {
        return inReplyToUserId;
    }

    public void setInReplyToUserId(long inReplyToUserId) {
        this.inReplyToUserId = inReplyToUserId;
    }

    public int getRetweetCount() {
        return retweetCount;
    }

    public void setRetweetCount(int retweetCount) {
        this.retweetCount = retweetCount;
    }

    public int getFavoriteCount() {
        return favoriteCount;
    }

    public void setFavoriteCount(int favoriteCount) {
        this.favoriteCount = favoriteCount;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public void setHashtags(List<String> hashtags) {
        this.hashtags = hashtags;
    }

    public boolean isFavorited() {
        return favorited;
    }

    public void setFavorited(boolean favorited) {
        this.favorited = favorited;
    }

    public boolean isRetweeted() {
        return retweeted;
    }

    public void setRetweeted(boolean retweeted) {
        this.retweeted = retweeted;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public List<UserMention> getMentions() {
        return mentions;
    }

    public void setMentions(List<UserMention> mentions) {
        this.mentions = mentions;
    }
}
