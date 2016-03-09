package com.nibado.example.sparstrecass;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Language spread
 * Word counts
 * Geolocation spread
 */

public class SparkFacade {
    private final SparkConf conf;
    private final JavaStreamingContext jssc;

    public SparkFacade(String master, String appName) {
        conf =  new SparkConf().setMaster(master).setAppName(appName);
        jssc = new JavaStreamingContext(conf, Durations.milliseconds(250));
    }

    public void run() {
        Map<String, String> properties = new HashMap<>();
        Map<String, Integer> topics = new HashMap<>();
        topics.put("test-json", 1);
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("group.id", "test");
        properties.put("zookeeper.connection.timeout.ms", "5000");

        JavaPairReceiverInputDStream<String, String> directKafkaStream =
                KafkaUtils.createStream(jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        properties,
                        topics,
                        StorageLevel.MEMORY_ONLY());

        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(t -> {
                System.out.println(t._2());
            });
        });

        jssc.start();
        jssc.awaitTermination();
    }

    public static void main(String... argv) {
        SparkFacade facade = new SparkFacade("local[4]", "TwitterWordCound");

        facade.run();
    }
}
