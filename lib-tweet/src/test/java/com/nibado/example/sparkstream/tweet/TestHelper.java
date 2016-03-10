package com.nibado.example.sparkstream.tweet;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class TestHelper {
    private static final String[] LOREM = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum".split(" ");
    private static final Random RANDOM = new Random(0);
    private static List<Tweet> TEST_SET;

    public static List<Tweet> testSet() {
        if(TEST_SET == null) {
            TEST_SET = randomTweets(1000);
        }

        return TEST_SET;
    }

    public static List<Tweet> randomTweets(int amount) {
        List<Tweet> tweets = new ArrayList<>(amount);
        for(int i = 0;i < amount;i++) {
            tweets.add(randomTweet());
        }

        return tweets;
    }

    public static Tweet randomTweet() {
        return new Tweet(randomWord(), randomString(50), new Date(), randomWords(10), randomWords(5));
    }

    public static String randomWord() {
        double index = RANDOM.nextDouble() * RANDOM.nextDouble() * LOREM.length;
        return LOREM[(int)index];
    }

    public static List<String> randomWords(int max) {
        int size = RANDOM.nextInt(max);
        List<String> words = new ArrayList<>(size);

        for(int i = 0;i < size;i++) {
            words.add(randomWord());
        }

        return words;
    }

    public static String randomString(int max) {
        return randomWords(max).stream().reduce((a, b) -> a + " " + b).orElse("");
    }
}
