package com.paradigmadigital.kafkaweminar.kstreams.wordcount.model;

import com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas.TweetSchema;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.AvroFormat;
import com.paradigmadigital.kafkaweminar.kstreams.wordcount.serdes.DifferentNamespaceFomExpected;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import twitter4j.Status;

import java.util.Optional;

public class User implements AvroFormat<User> {

    private GenericRecord avroRecord;
    private Long id;
    private String name;
    private String screenName;
    private Optional<String> location;
    private boolean protected_;
    private boolean verified;
    private int followersCount;
    private int listedCount;
    private int favouritesCount;
    private int statusesCount;
    private long createdAt;
    private String profileImageUrl;

    public User() {}

    public User(Status status) {
        GenericRecord user = new GenericData.Record(TweetSchema.userSchema);
        user.put("id", status.getUser().getId());
        user.put("name", status.getUser().getName());
        user.put("screenName", status.getUser().getScreenName());
        user.put("location", status.getUser().getLocation());
        user.put("protected", status.getUser().isProtected());
        user.put("verified", status.getUser().isVerified());
        user.put("followersCount", status.getUser().getFollowersCount());
        user.put("listedCount", status.getUser().getListedCount());
        user.put("favouritesCount", status.getUser().getFavouritesCount());
        user.put("statusesCount", status.getUser().getStatusesCount());
        user.put("createdAt", status.getUser().getCreatedAt().getTime());
        user.put("profileImageUrl", status.getUser().getProfileImageURLHttps());
    }

    @Override
    public GenericRecord toAvro() {
        return this.avroRecord;
    }

    @Override
    public User fromAvro(GenericRecord record) throws DifferentNamespaceFomExpected {
        String name = record.getSchema().getNamespace();
        String nameSpace = TweetSchema.userSchema.getNamespace();

        if (name.equals(nameSpace)) {
            this.avroRecord = record;
            this.id = (Long) record.get("id");
            this.name = record.get("name").toString();
            this.screenName = record.get("screenName").toString();
            this.location = Optional.ofNullable(record.get("location")).map(Object::toString);
            this.protected_ = (Boolean) record.get("protected");
            this.verified = (Boolean) record.get("verified");
            this.followersCount = (Integer) record.get("followersCount");
            this.listedCount = (Integer) record.get("listedCount");
            this.favouritesCount = (Integer) record.get("favouritesCount");
            this.statusesCount = (Integer) record.get("statusesCount");
            this.createdAt = (Long) record.get("createdAt");
            this.profileImageUrl = record.get("profileImageUrl").toString();
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

    public Optional<String> getLocation() {
        return location;
    }

    public void setLocation(Optional<String> location) {
        this.location = location;
    }

    public boolean isProtected_() {
        return protected_;
    }

    public void setProtected_(boolean protected_) {
        this.protected_ = protected_;
    }

    public boolean isVerified() {
        return verified;
    }

    public void setVerified(boolean verified) {
        this.verified = verified;
    }

    public int getFollowersCount() {
        return followersCount;
    }

    public void setFollowersCount(int followersCount) {
        this.followersCount = followersCount;
    }

    public int getListedCount() {
        return listedCount;
    }

    public void setListedCount(int listedCount) {
        this.listedCount = listedCount;
    }

    public int getFavouritesCount() {
        return favouritesCount;
    }

    public void setFavouritesCount(int favouritesCount) {
        this.favouritesCount = favouritesCount;
    }

    public int getStatusesCount() {
        return statusesCount;
    }

    public void setStatusesCount(int statusesCount) {
        this.statusesCount = statusesCount;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public String getProfileImageUrl() {
        return profileImageUrl;
    }

    public void setProfileImageUrl(String profileImageUrl) {
        this.profileImageUrl = profileImageUrl;
    }
}
