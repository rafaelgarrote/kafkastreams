package com.paradigmadigital.kafkaweminar.twitterclient;

import com.paradigmadigital.kafkaweminar.kafkaclient.Message;
import com.paradigmadigital.kafkaweminar.kafkaclient.Producer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import twitter4j.*;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class LangListener implements StatusListener {

    private static final String tweetNameSpace = "tweet.value.avro";
    private static final String userNameSpace = "user.value.avro";
    private static final String userMentionNameSpace = "tweet.mention.avro";
    private final Producer<GenericRecord, GenericRecord> producer;
    private final Schema.Parser schemaParser = new Schema.Parser();
    private final Schema keySchema = schemaParser.parse(
            "{\"namespace\": \"tweet.key.avro\", " +
                    "\"type\":\"record\"," +
                    "\"name\":\"key\"," +
                    "\"fields\":[" +
                        "{\"name\":\"lang\", \"type\":\"string\"}" +
                    "]}");
    private final Schema valueSchema = schemaParser.parse("{\n" +
            "    \"namespace\": \"tweet.value.avro\",\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"tweet\",\n" +
            "    \"fields\": [\n" +
            "        {\"name\": \"id\", \"type\": \"long\"},\n" +
            "        {\"name\": \"text\", \"type\": \"string\"},\n" +
            "        {\"name\": \"lang\", \"type\":\"string\"},\n" +
            "        {\"name\": \"createdAt\", \"type\": \"long\", \"logicalType\": \"date\"},\n" +
            "        {\"name\": \"inReplyToStatusId\", \"type\":\"long\"},\n" +
            "        {\"name\": \"inReplyToUserId\", \"type\":\"long\"},\n" +
            "        {\"name\": \"inReplyToScreenName\", \"type\":[\"null\", \"string\"], \"default\" : null},\n" +
            "        {\"name\": \"isQuoteStatus\", \"type\":[\"null\", \"boolean\"], \"default\" : null},\n" +
            "        {\"name\": \"quoteCount\", \"type\":[\"null\", \"long\"], \"default\" : null},\n" +
            "        {\"name\": \"replyCount\", \"type\":[\"null\", \"long\"], \"default\" : null},\n" +
            "        {\"name\": \"retweetCount\", \"type\":\"int\"},\n" +
            "        {\"name\": \"favoriteCount\", \"type\":\"int\"},\n" +
            "        {\"name\":\"hashtags\",\n" +
            "            \"type\":{\n" +
            "                \"type\": \"array\",  \n" +
            "                \"items\" : \"string\", \"default\": []\n" +
            "            }\n" +
            "        },\n" +
            "        {\"name\": \"favorited\", \"type\":\"boolean\"},\n" +
            "        {\"name\": \"retweeted\", \"type\":\"boolean\"},\n" +
            "        {\"name\": \"user\", \"type\": {\n" +
            "            \"namespace\": \"" + userNameSpace + "\",\n" +
            "            \"type\" : \"record\",\n" +
            "            \"name\" : \"userData\",\n" +
            "            \"fields\" : [\n" +
            "                {\"name\": \"id\", \"type\": \"long\"},\n" +
            "                {\"name\": \"name\", \"type\": \"string\"},\n" +
            "                {\"name\": \"screenName\", \"type\": \"string\"},\n" +
            "                {\"name\": \"location\", \"type\":[\"null\", \"string\"], \"default\" : null},\n" +
            "                {\"name\": \"protected\", \"type\": \"boolean\"},\n" +
            "                {\"name\": \"verified\", \"type\": \"boolean\"},\n" +
            "                {\"name\": \"followersCount\", \"type\": \"int\"},\n" +
            "                {\"name\": \"listedCount\", \"type\": \"int\"},\n" +
            "                {\"name\": \"favouritesCount\", \"type\": \"int\"},\n" +
            "                {\"name\": \"statusesCount\", \"type\": \"int\"},\n" +
            "                {\"name\": \"createdAt\", \"type\": \"long\", \"logicalType\": \"date\"},    \n" +
            "                {\"name\": \"profileImageUrl\", \"type\": \"string\"}\n" +
            "            ]}\n" +
            "        },\n" +
            "        {\"name\":\"mentions\", \"type\":{\n" +
            "            \"type\": \"array\",  \n" +
            "            \"items\":{\n" +
            "                \"namespace\": \"" + userMentionNameSpace + "\",\n" +
            "                \"name\":\"mention\",\n" +
            "                \"type\":\"record\",\n" +
            "                \"fields\":[\n" +
            "                    {\"name\":\"screenName\", \"type\":\"string\"},\n" +
            "                    {\"name\":\"name\", \"type\":\"string\"},\n" +
            "                    {\"name\":\"id\", \"type\":\"long\"}\n" +
            "                ]}\n" +
            "            }\n" +
            "        }\n" +
            "    ]\n" +
            "}");

    public LangListener(Producer<GenericRecord, GenericRecord> producer) {
        this.producer = producer;
    }

    private GenericRecord mentionToRecord(UserMentionEntity mention) {
        GenericRecord record = new GenericData.Record(valueSchema.getField("mentions.mention").schema());
        record.put("screenName", mention.getScreenName());
        record.put("name", mention.getName());
        record.put("id", mention.getId());
        return record;
    }

    @Override
    public void onStatus(Status status) {

        System.out.println("--------------ddd");
        System.out.println(TwitterObjectFactory.getRawJSON(status));
        System.out.println("--------------");

        String lang = status.getLang();

        GenericRecord key = new GenericData.Record(keySchema);
        key.put("lang", lang);

        GenericRecord user = new GenericData.Record(valueSchema.getField("user").schema());
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

        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("id", status.getId());
        value.put("text", status.getText());
        value.put("lang", status.getLang());
        value.put("createdAt", status.getCreatedAt().getTime());
        value.put("inReplyToStatusId", status.getInReplyToStatusId());
        value.put("inReplyToUserId", status.getInReplyToUserId());
        value.put("inReplyToScreenName", status.getInReplyToScreenName());
        value.put("retweetCount", status.getRetweetCount());
        value.put("favoriteCount", status.getFavoriteCount());
        value.put("hashtags", Arrays.stream(status.getHashtagEntities()).map(HashtagEntity::getText).collect(Collectors.toList()));
        value.put("favorited", status.isFavorited());
        value.put("retweeted", status.isRetweeted());
        value.put("user", user);
        value.put("mentions", Arrays.stream(status.getUserMentionEntities()).map(this::mentionToRecord).collect(Collectors.toList()));

        producer.produceMessage(
                new Message<GenericRecord, GenericRecord>(Optional.of(key), value),
                Optional.of(new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        e.printStackTrace();
                    }
                }));
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {

    }

    @Override
    public void onStallWarning(StallWarning warning) {

    }

    @Override
    public void onException(Exception ex) {

    }
}
