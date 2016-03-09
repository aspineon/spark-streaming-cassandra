package com.nibado.example.sparkstream.tweet;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.util.Map;

public class TweetKafkaSerializer implements Closeable, AutoCloseable, Serializer<Tweet>, Deserializer<Tweet>  {
    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.addDefaultSerializer(Tweet.class, new TweetKryoSerializer());
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
}
