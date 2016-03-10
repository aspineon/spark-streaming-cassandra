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
        writeList(output, tweet.getHashtags());
        writeList(output, tweet.getMentions());
    }

    @Override
    public Tweet read(Kryo kryo, Input input, Class<Tweet> aClass) {
        String user = input.readString();
        Date created = new Date(input.readLong());
        String text = input.readString();

        return new Tweet(user, text, created, readList(input), readList(input));
    }

    private static List<String> readList(Input input) {
        int size = input.readVarInt(true);
        List<String> list = new ArrayList<>(size);

        for(int i = 0;i < size;i++) {
            list.add(input.readString());
        }

        return list;
    }

    private static void writeList(Output output, List<String> list) {
        output.writeVarInt(list.size(), true);
        for(String tag : list) {
            output.writeString(tag);
        }
    }
}
