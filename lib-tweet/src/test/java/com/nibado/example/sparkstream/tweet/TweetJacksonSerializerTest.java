package com.nibado.example.sparkstream.tweet;

import org.junit.Before;
import org.junit.Test;

import static com.nibado.example.sparkstream.tweet.TestHelper.testSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class TweetJacksonSerializerTest {
    private TweetJacksonSerializer serializer;

    @Before
    public void setup() {
        serializer = new TweetJacksonSerializer();
    }

    @Test
    public void testSerialize() throws Exception {
        for(Tweet t : testSet()) {
            Tweet out = serializer.deserialize("", serializer.serialize("", t));
            System.out.println(t);
            assertThat(t).isNotSameAs(out);
            assertThat(out).isEqualTo(out);
            assertThat(out).isEqualToComparingFieldByField(out);
        }
    }
}