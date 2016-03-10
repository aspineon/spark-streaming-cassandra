package com.nibado.example.sparkstream.producer;

import com.nibado.example.sparkstream.tweet.Tweet;
import com.nibado.example.sparkstream.tweet.TweetKryoSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.UserMentionEntity;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TwitterToKafka implements StatusListener {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafka.class);

    private Properties properties;
    private String topic;
    private Producer<String, Tweet> producer;

    public TwitterToKafka(Properties properties, String topic) {
        this.properties = properties;
        this.topic = topic;
    }

    public void start() {
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", TweetKryoSerializer.class.getName());

        producer = new KafkaProducer<>(properties);
    }

    public void stop() {
        producer.close();
    }

    @Override
    public void onStatus(Status status) {
        Tweet tweet = new Tweet(status.getUser().getScreenName(), status.getText(), status.getCreatedAt(), hashTags(status.getHashtagEntities()));
        producer.send(new ProducerRecord<>(topic, null, tweet));
        LOG.debug("Tweet: {}", tweet);
    }

    private static List<String> hashTags(HashtagEntity[] hashTags) {
        return Arrays.asList(hashTags).stream().map(HashtagEntity::getText).collect(Collectors.toList());
    }

    private static List<String> userMentions(UserMentionEntity[] mentions) {
        return Arrays.asList(mentions).stream().map(UserMentionEntity::getName).collect(Collectors.toList());
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
    }

    @Override
    public void onTrackLimitationNotice(int i) {
    }

    @Override
    public void onScrubGeo(long l, long l1) {
    }

    @Override
    public void onStallWarning(StallWarning stallWarning) {
    }

    @Override
    public void onException(Exception e) {
    }
}
