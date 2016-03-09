package com.nibado.example.sparkstream.tweet;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TweetKafkaSerializerTest {
    private static final String[] LOREM = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum".split(" ");
    private static final Random RANDOM = new Random(0);

    private TweetKafkaSerializer serializer;

    @Before
    public void setup() {
        serializer = new TweetKafkaSerializer();
    }

    @Test
    public void testSerialize() throws Exception {
        for(int i = 0;i < 1000;i++) {
            Tweet t = randomTweet();
            Tweet out = serializer.deserialize("", serializer.serialize("", t));

            assertThat(t).isNotSameAs(out);
            assertThat(out).isEqualTo(out);
            assertThat(out).isEqualToComparingFieldByField(out);
        }
    }

    private static Tweet randomTweet() {
        return new Tweet(randomWord(), randomString(50), new Date(), randomWords(10));
    }

    private static String randomWord() {
        double index = RANDOM.nextDouble() * RANDOM.nextDouble() * LOREM.length;
        return LOREM[(int)Math.round(index)];
    }

    private static List<String> randomWords(int max) {
        int size = RANDOM.nextInt(max);
        List<String> words = new ArrayList<>(size);

        for(int i = 0;i < size;i++) {
            words.add(randomWord());
        }

        return words;
    }

    private static String randomString(int max) {
        return randomWords(max).stream().reduce((a, b) -> a + " " + b).orElse("");
    }
}