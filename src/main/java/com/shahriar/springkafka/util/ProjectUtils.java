package com.shahriar.springkafka.util;

import java.util.Arrays;
import java.util.List;

import com.google.gson.JsonParser;

public class ProjectUtils {
	
	/**
	 * Some tweets are about Kafka the author. This method checks for some keywords
	 * to see if the tweet is about Kafka software.
	 * 
	 * @param tweetJson
	 * @return
	 */
    public static boolean extractKafkaSoftwareTweets(String tweetJson) {
		
		List<String> kafkaWords = Arrays.asList("Apache", "Stream", "stream", "event", "service", "microservice"
				, "consumer", "producer", "Elasticsearch", "ksqlDB"
				, "offset", "partition", "broker", "topic" 
        		, "data", "log", "devops", "Connect", "Sink"
        		, "config");
        
        try {
            String tweetText = JsonParser.parseString(tweetJson)
                    .getAsJsonObject()
                    .get("text").getAsString();
            return kafkaWords.stream().anyMatch(str -> tweetText.contains(str));            	
        }                               
        catch (NullPointerException e){
            return false;
        }        
    }

    public static String extractIdFromTweet(String tweetJson) {        
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
