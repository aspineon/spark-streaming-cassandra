package com.nibado.example.sparkstream.tweet;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class Tweet {
    private final String user;
    private final String text;
    private final Date created;
    private final List<String> hashtags;
    private final List<String> mentions;

    public Tweet(
            @JsonProperty("user")String user,
            @JsonProperty("text")String text,
            @JsonProperty("created")Date created,
            @JsonProperty("hashtags")List<String> hashtags,
            @JsonProperty("mentions")List<String> mentions) {
        this.user = user;
        this.text = text;
        this.created = created;
        this.hashtags = unmodifiableList(hashtags);
        this.mentions = unmodifiableList(mentions);
    }

    public String getUser() {
        return user;
    }

    public String getText() {
        return text;
    }

    public Date getCreated() {
        return created;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public List<String> getMentions() {
        return mentions;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "user='" + user + '\'' +
                ", text='" + text + '\'' +
                ", created=" + created +
                ", hashtags=" + hashtags +
                ", mentions=" + mentions +
                '}';
    }
}
