package com.shahriar.springkafka.consumer;

import java.io.IOException;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.shahriar.springkafka.util.ProjectUtils;

@Service
public class KafkaConsumer {
	
	Logger log = org.slf4j.LoggerFactory.getLogger(KafkaConsumer.class);
	
	@Autowired
	RestClient elasticsearchClient;
		
	@KafkaListener(topics = "twitter_tweets", id = "kafka_tweets_group")	
    public void consume(String message) throws IOException {
		        
        try {
        	log.info("!!! Consuming message = " + message);
        	String messageId = ProjectUtils.extractIdFromTweet(message);
        	
            Request elasticSearchPutRequest = new Request("POST", "/kafkatweets/_doc/" + messageId);            
            elasticSearchPutRequest.setJsonEntity(message);                   
            elasticsearchClient.performRequest(elasticSearchPutRequest);
            
        } catch (NullPointerException e){ 
            log.error("skipping bad data: " + message + " " + e.getMessage());
        }
        finally {
        	elasticsearchClient.close();
        }
    }
}
