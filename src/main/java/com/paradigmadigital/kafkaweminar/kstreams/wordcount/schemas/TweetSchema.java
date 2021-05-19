package com.paradigmadigital.kafkaweminar.kstreams.wordcount.schemas;

import org.apache.avro.Schema;

public class TweetSchema {

    private static final Schema.Parser schemaParser = new Schema.Parser();
    private static final String tweetNameSpace = "tweet.value.avro";
    private static final String userNameSpace = "user.value.avro";
    private static final String userMentionNameSpace = "tweet.mention.avro";
    private static final Schema schema = schemaParser.parse("{\n" +
            "    \"namespace\": \"" + tweetNameSpace + "\",\n" +
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

    private TweetSchema() {}

    public static Schema tweetSchema = schema;
    public static Schema userSchema = schema.getField("user").schema();
    public static Schema mentionSchema = schema.getField("mentions").schema();
}
