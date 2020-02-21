package com.shahriar.springkafka.producer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.shahriar.springkafka.util.ProjectUtils;
import com.shahriar.springkafka.util.TwitterClient;
import com.twitter.hbc.core.Client;

@Service
public class KafkaProducer {
	
	Logger log = org.slf4j.LoggerFactory.getLogger(KafkaProducer.class);
	
	@Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
	
	private static final String TOPIC = "twitter_tweets";


	@EventListener(ApplicationReadyEvent.class)
	public void startKafkaProducer() {				
				
		/** 
		 * Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream 
		 * */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);        

        TwitterClient twitterClient = new TwitterClient();
        Client client = twitterClient.createTwitterClient(msgQueue);
        client.connect();
        
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            	log.error(e.getMessage());
                client.stop();
            }
            if (msg != null){            	
            	boolean kafkaTweet = ProjectUtils.extractKafkaSoftwareTweets(msg);
            	
            	if(kafkaTweet) {
            		log.info("TRUE!!! " + msg);
            	    kafkaTemplate.send(TOPIC, msg);
            	}
            }
        }		
	}
}
