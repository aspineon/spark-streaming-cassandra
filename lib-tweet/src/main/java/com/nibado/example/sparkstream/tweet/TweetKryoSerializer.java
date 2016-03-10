package com.nibado.example.sparkstream.tweet;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TweetKryoSerializer implements Closeable, AutoCloseable, Serializer<Tweet>, Deserializer<Tweet>  {
    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.addDefaultSerializer(Tweet.class, new TweetKryoKryoSerializer());
            return kryo;
        }
    };

    @Override
    public Tweet deserialize(String s, byte[] bytes) {
        try {
            return kryos.get().readObject(new ByteBufferInput(bytes), Tweet.class);
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
        ByteBufferOutput output = new ByteBufferOutput(256, 2000);
        kryos.get().writeObject(output, tweet);
        return output.toBytes();
    }

    @Override
    public void close() {
        kryos.remove();
        kryos = null;
    }

    private class TweetKryoKryoSerializer extends com.esotericsoftware.kryo.Serializer<Tweet> {

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

        private List<String> readList(Input input) {
            int size = input.readVarInt(true);
            List<String> list = new ArrayList<>(size);

            for(int i = 0;i < size;i++) {
                list.add(input.readString());
            }

            return list;
        }

        private void writeList(Output output, List<String> list) {
            output.writeVarInt(list.size(), true);
            for(String tag : list) {
                output.writeString(tag);
            }
        }
    }
}
