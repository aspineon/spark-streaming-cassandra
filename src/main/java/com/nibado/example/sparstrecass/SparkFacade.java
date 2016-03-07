package com.nibado.example.sparstrecass;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import java.util.HashSet;
import java.util.Set;

/**
 * Language spread
 * Word counts
 * Geolocation spread
 */

public class SparkFacade {
    private final SparkConf conf;
    private final JavaStreamingContext jssc;
    private final Set<String> languageFilter = new HashSet<>();
    private String[] filterWords = {};

    public SparkFacade(String master, String appName) {
        conf =  new SparkConf().setMaster(master).setAppName(appName);
        jssc = new JavaStreamingContext(conf, Durations.milliseconds(250));
    }

    public void run() {
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, filterWords);

        twitterStream.filter(languageFilter(languageFilter)).map(s -> s.getText()).foreachRDD(rdd -> {
            rdd.foreach(s -> System.out.println(s));
        });

        jssc.start();
        jssc.awaitTermination();
    }

    private static Function<Status, Boolean> languageFilter(Set<String> languageFilter) {
        return s -> languageFilter.size() == 0 || languageFilter.contains(s.getLang());
    }

    public SparkFacade setFilterWords(String... filterWords) {
        this.filterWords = filterWords;

        return this;
    }

    public SparkFacade addFilterLanguages(String... filterLanguages) {
        for(String lang : filterLanguages) {
            languageFilter.add(lang);
        }

        return this;
    }

    public static void main(String... argv) {
        SparkFacade facade = new SparkFacade("local[4]", "TwitterWordCound");

        facade.addFilterLanguages("en");
        facade.run();

    }
}
