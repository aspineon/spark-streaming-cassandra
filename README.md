# Spark Streaming Cassandra example

This is a quick example to show how to get twitter streaming data using Apache Spark Streaming and store different forms
of twitter status data as time series data in Cassandra.

## Running

Twitter4j needs a number of API keys that can be created [here](https://dev.twitter.com/apps). You need to specify these
keys on the command line when starting the example application:

-Dtwitter4j.debug=true  -Dtwitter4j.oauth.consumerKey=... -Dtwitter4j.oauth.consumerSecret=... -Dtwitter4j.oauth.accessToken=... -Dtwitter4j.oauth.accessTokenSecret=...

