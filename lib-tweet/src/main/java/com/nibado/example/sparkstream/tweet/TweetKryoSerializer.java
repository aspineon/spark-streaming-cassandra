package com.nibado.example.sparkstream.tweet;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TweetKryoSerializer extends Serializer<Tweet> {

    @Override
    public void write(Kryo kryo, Output output, Tweet tweet) {
        output.writeString(tweet.getUser());
        output.writeLong(tweet.getCreated().getTime());
        output.writeString(tweet.getText());
        output.writeVarInt(tweet.getHashtags().size(), true);
        for(String tag : tweet.getHashtags()) {
            output.writeString(tag);
        }
    }

    @Override
    public Tweet read(Kryo kryo, Input input, Class<Tweet> aClass) {
        String user = input.readString();
        Date created = new Date(input.readLong());
        String text = input.readString();
        int size = input.readVarInt(true);
        List<String> hashTags = new ArrayList<>(size);

        for(int i = 0;i < size;i++) {
            hashTags.add(input.readString());
        }

        return new Tweet(user, text, created, hashTags);
    }
}
