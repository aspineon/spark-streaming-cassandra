package com.nibado.example.sparkstream.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Properties;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String... argv) {
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        TwitterToKafka listener = new TwitterToKafka(properties, "tweets");
        listener.start();

        twitterStream.addListener(listener);
        twitterStream.sample("en");
    }
}
