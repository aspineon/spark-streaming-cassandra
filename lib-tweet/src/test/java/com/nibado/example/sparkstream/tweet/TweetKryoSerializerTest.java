package com.nibado.example.sparkstream.tweet;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static com.nibado.example.sparkstream.tweet.TestHelper.testSet;
import static org.assertj.core.api.Assertions.assertThat;

public class TweetKryoSerializerTest {
    private TweetKryoSerializer serializer;

    @Before
    public void setup() {
        serializer = new TweetKryoSerializer();
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