package com.nibado.example.sparkstream.tweet;

import java.util.Date;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class Tweet {
    private final String user;
    private final String text;
    private final Date created;
    private final List<String> hashtags;

    public Tweet(String user, String text, Date created, List<String> hashtags) {
        this.user = user;
        this.text = text;
        this.created = created;
        this.hashtags = unmodifiableList(hashtags);
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

    @Override
    public String toString() {
        return "Tweet{" +
                "user='" + user + '\'' +
                ", text='" + text + '\'' +
                ", created=" + created +
                ", hashtags=" + hashtags +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tweet tweet = (Tweet) o;

        if (!user.equals(tweet.user)) return false;
        if (!text.equals(tweet.text)) return false;
        if (!created.equals(tweet.created)) return false;
        return equals(hashtags, tweet.hashtags);

    }

    @Override
    public int hashCode() {
        int result = user.hashCode();
        result = 31 * result + text.hashCode();
        result = 31 * result + created.hashCode();
        result = 31 * result + hashCode(hashtags);
        return result;
    }

    private static int hashCode(List<String> strings) {
        int result = 1;

        for(String s : strings) {
            result = 31 * result * s.hashCode();
        }

        return result;
    }

    private static boolean equals(List<String> list1, List<String> list2) {
        if(list1.size() != list2.size()) {
            return false;
        }
        for(int i = 0;i < list1.size();i++) {
            if(!list1.get(i).equals(list2.get(i))) {
                return false;
            }
        }

        return true;
    }
}
