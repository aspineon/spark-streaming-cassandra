package com.nibado.example.sparkstream.tweet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.util.Map;

public class TweetJacksonSerializer implements Closeable, AutoCloseable, Serializer<Tweet>, Deserializer<Tweet> {
    private ThreadLocal<ObjectMapper> mappers = new ThreadLocal<ObjectMapper>() {
        protected ObjectMapper initialValue() {
            return new ObjectMapper();
        }
    };

    @Override
    public Tweet deserialize(String s, byte[] bytes) {
        try {
            return mappers.get().readValue(bytes, Tweet.class);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes",e);
        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Tweet tweet) {
        try {
            return mappers.get().writeValueAsBytes(tweet);
        }
        catch(JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {
        mappers.remove();
        mappers = null;
    }
}
